"""法律证据保管与流转后台。"""
from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import os
import sqlite3
import threading
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB = BASE_DIR / "custody.db"
MEMBER_ROLES = {"custodian", "analyst", "auditor"}
# 借调提前提醒天数：提醒日 = 保留期限 - REMIND_DAYS
LOAN_REMIND_DAYS = 30
# 借调单状态
LOAN_STATUSES = ("pending", "out", "closed", "void")
RECEIPT_STATUSES = ("none", "pending", "confirmed", "failed")
# 借调相关保管事件类型
LOAN_EVENT_TYPES = ("LOAN_OUT", "LOAN_RETURN", "LOAN_VOID")


class BusinessError(Exception):
    def __init__(self, message, status=400, code="bad_request", extra=None):
        super().__init__(message)
        self.message, self.status, self.code = message, status, code
        # 附加冲突信息（如冲突借调单编号），会并入错误响应
        self.extra = extra or {}


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class CustodyStore:
    def __init__(self, db_path=DEFAULT_DB):
        self.db_path = str(db_path)
        self._lock = threading.Lock()
        # 回执传输模拟开关：为 True 时回执传输失败，用于演示/测试重试。
        # 可通过环境变量 CUSTODY_RECEIPT_FAIL=1 开启。
        self.receipt_fail = os.environ.get("CUSTODY_RECEIPT_FAIL", "0") == "1"

    def connect(self):
        conn = sqlite3.connect(self.db_path, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    def init_schema(self):
        with self._lock, self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users(
                    id TEXT PRIMARY KEY, name TEXT NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1))
                );
                CREATE TABLE IF NOT EXISTS cases(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, case_number TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL, created_by TEXT NOT NULL REFERENCES users(id), created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS case_members(
                    case_id INTEGER NOT NULL REFERENCES cases(id), user_id TEXT NOT NULL REFERENCES users(id),
                    role TEXT NOT NULL CHECK(role IN ('custodian','analyst','auditor')),
                    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
                    granted_by TEXT NOT NULL REFERENCES users(id), granted_at TEXT NOT NULL,
                    PRIMARY KEY(case_id,user_id)
                );
                CREATE TABLE IF NOT EXISTS evidence(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    case_id INTEGER NOT NULL REFERENCES cases(id), label TEXT NOT NULL,
                    filename TEXT NOT NULL, sha256 TEXT NOT NULL, size INTEGER NOT NULL,
                    content BLOB NOT NULL, status TEXT NOT NULL DEFAULT 'custody'
                        CHECK(status IN ('custody','opened','released','derivative')),
                    current_custodian TEXT NOT NULL, legal_hold INTEGER NOT NULL DEFAULT 0 CHECK(legal_hold IN (0,1)),
                    retention_until TEXT NOT NULL, created_by TEXT NOT NULL REFERENCES users(id),
                    created_at TEXT NOT NULL, UNIQUE(case_id,label)
                );
                CREATE TABLE IF NOT EXISTS loans(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    loan_no TEXT NOT NULL UNIQUE,
                    case_id INTEGER NOT NULL REFERENCES cases(id),
                    evidence_id INTEGER NOT NULL REFERENCES evidence(id),
                    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','out','closed','void')),
                    initiator_id TEXT NOT NULL REFERENCES users(id),
                    approver_id TEXT REFERENCES users(id),
                    borrower TEXT NOT NULL, purpose TEXT NOT NULL,
                    out_at TEXT, due_at TEXT, remind_at TEXT,
                    void_reason TEXT,
                    receipt_status TEXT NOT NULL DEFAULT 'none' CHECK(receipt_status IN ('none','pending','confirmed','failed')),
                    receipt_attempts INTEGER NOT NULL DEFAULT 0,
                    receipt_last_error TEXT,
                    receipt_transmitted_at TEXT,
                    receipt_confirmed_at TEXT,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                -- 同一证据同一时刻只允许一条有效（待放行/借出中）借调单
                CREATE UNIQUE INDEX IF NOT EXISTS idx_loans_one_active
                    ON loans(evidence_id) WHERE status IN ('pending','out');
                CREATE TABLE IF NOT EXISTS loan_receipts(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    receipt_no TEXT NOT NULL UNIQUE,
                    loan_id INTEGER NOT NULL UNIQUE REFERENCES loans(id),
                    evidence_id INTEGER NOT NULL REFERENCES evidence(id),
                    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','confirmed','failed')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    transmitted_at TEXT,
                    confirmed_at TEXT,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS custody_events(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    evidence_id INTEGER NOT NULL REFERENCES evidence(id), sequence INTEGER NOT NULL,
                    event_type TEXT NOT NULL CHECK(event_type IN ('INGEST','TRANSFER','OPEN','ANALYZE','RELEASE','HOLD_SET','HOLD_CLEARED','LOAN_OUT','LOAN_RETURN','LOAN_VOID')),
                    actor_id TEXT NOT NULL REFERENCES users(id), from_person TEXT,
                    to_person TEXT, location TEXT NOT NULL DEFAULT '', note TEXT NOT NULL DEFAULT '',
                    previous_hash TEXT NOT NULL, event_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL, loan_id INTEGER REFERENCES loans(id),
                    UNIQUE(evidence_id,sequence)
                );
                CREATE TABLE IF NOT EXISTS derivatives(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    parent_evidence_id INTEGER NOT NULL REFERENCES evidence(id),
                    child_evidence_id INTEGER NOT NULL UNIQUE REFERENCES evidence(id),
                    method TEXT NOT NULL, actor_id TEXT NOT NULL REFERENCES users(id),
                    created_at TEXT NOT NULL, UNIQUE(parent_evidence_id,child_evidence_id)
                );
                CREATE TABLE IF NOT EXISTS audit_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    case_id INTEGER NOT NULL REFERENCES cases(id), actor_id TEXT NOT NULL REFERENCES users(id),
                    action TEXT NOT NULL, detail TEXT NOT NULL, created_at TEXT NOT NULL
                );
                """
            )
            self._migrate_custody_events(conn)
            self._backfill_legacy_loans(conn)

    def _migrate_custody_events(self, conn):
        """旧库 custody_events 缺少 loan_id 且事件类型较旧，重建表以纳入借调事件。"""
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='custody_events'"
        ).fetchone()
        if not row:
            return
        if "loan_id" in (row[0] or "") and "LOAN_OUT" in (row[0] or ""):
            return  # 已是新结构
        conn.executescript(
            """
            CREATE TABLE custody_events_new(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER NOT NULL REFERENCES evidence(id), sequence INTEGER NOT NULL,
                event_type TEXT NOT NULL CHECK(event_type IN ('INGEST','TRANSFER','OPEN','ANALYZE','RELEASE','HOLD_SET','HOLD_CLEARED','LOAN_OUT','LOAN_RETURN','LOAN_VOID')),
                actor_id TEXT NOT NULL REFERENCES users(id), from_person TEXT,
                to_person TEXT, location TEXT NOT NULL DEFAULT '', note TEXT NOT NULL DEFAULT '',
                previous_hash TEXT NOT NULL, event_hash TEXT NOT NULL,
                created_at TEXT NOT NULL, loan_id INTEGER REFERENCES loans(id),
                UNIQUE(evidence_id,sequence)
            );
            INSERT INTO custody_events_new(id,evidence_id,sequence,event_type,actor_id,from_person,to_person,location,note,previous_hash,event_hash,created_at)
                SELECT id,evidence_id,sequence,event_type,actor_id,from_person,to_person,location,note,previous_hash,event_hash,created_at FROM custody_events;
            DROP TABLE custody_events;
            ALTER TABLE custody_events_new RENAME TO custody_events;
            """
        )

    def _backfill_legacy_loans(self, conn):
        """旧库移交记录（TRANSFER）补借调归属：为每条无归属的历史移交补一条已结束借调单。"""
        legacy = conn.execute(
            """SELECT e.id AS event_id, e.evidence_id, e.actor_id, e.from_person, e.to_person,
                      e.location, e.note, e.created_at
               FROM custody_events e
               WHERE e.event_type='TRANSFER' AND e.loan_id IS NULL
               ORDER BY e.id"""
        ).fetchall()
        for ev in legacy:
            evidence = conn.execute("SELECT case_id, retention_until FROM evidence WHERE id=?", (ev["evidence_id"],)).fetchone()
            if not evidence:
                continue
            ts = ev["created_at"]
            loan_no = f"LOAN-LEGACY-{ev['event_id']}"
            cur = conn.execute(
                """INSERT INTO loans(loan_no,case_id,evidence_id,status,initiator_id,approver_id,borrower,purpose,
                                     out_at,due_at,remind_at,receipt_status,receipt_transmitted_at,receipt_confirmed_at,
                                     created_at,updated_at)
                   VALUES(?,?,?,'closed',?,?,?,?,?,?,?,'confirmed',?,?,?,?)""",
                (loan_no, evidence["case_id"], ev["evidence_id"], ev["actor_id"], ev["actor_id"],
                 ev["to_person"] or "", f"旧库移交记录补录：{ev['note'] or '历史移交'}",
                 ts, evidence["retention_until"], evidence["retention_until"], ts, ts, ts, ts),
            )
            conn.execute("UPDATE custody_events SET loan_id=? WHERE id=?", (cur.lastrowid, ev["event_id"]))

    def seed(self):
        self.init_schema()
        with self.connect() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO users(id,name) VALUES(?,?)",
                [
                    ("custodian1", "证据保管员甲"), ("custodian2", "证据保管员乙"),
                    ("analyst1", "电子数据分析员"), ("auditor1", "案件审计员"), ("outsider", "外部人员"),
                ],
            )

    def _user(self, conn, user_id):
        if not user_id:
            raise BusinessError("缺少 X-User-Id", 401, "authentication_required")
        user = conn.execute("SELECT * FROM users WHERE id=? AND active=1", (user_id,)).fetchone()
        if not user:
            raise BusinessError("用户不存在或已停用", 401, "unknown_user")
        return user

    def _case(self, conn, case_id):
        row = conn.execute("SELECT * FROM cases WHERE id=?", (case_id,)).fetchone()
        if not row:
            raise BusinessError("案件不存在", 404, "not_found")
        return row

    def _member(self, conn, case_id, user_id, roles=None):
        user = self._user(conn, user_id)
        self._case(conn, case_id)
        row = conn.execute(
            "SELECT * FROM case_members WHERE case_id=? AND user_id=? AND active=1", (case_id, user_id)
        ).fetchone()
        if not row:
            raise BusinessError("不是案件有效成员", 403, "forbidden")
        if roles and row["role"] not in roles:
            raise BusinessError("当前案件角色无权执行此操作", 403, "forbidden")
        return user, row

    def _audit(self, conn, case_id, actor_id, action, detail):
        conn.execute(
            "INSERT INTO audit_log(case_id,actor_id,action,detail,created_at) VALUES(?,?,?,?,?)",
            (case_id, actor_id, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), now()),
        )

    def create_case(self, user_id, case_number, title):
        if not case_number.strip() or len(title.strip()) < 2:
            raise BusinessError("案件编号和标题不能为空", 422, "invalid_case")
        with self.connect() as conn:
            self._user(conn, user_id)
            try:
                cur = conn.execute(
                    "INSERT INTO cases(case_number,title,created_by,created_at) VALUES(?,?,?,?)",
                    (case_number.strip(), title.strip(), user_id, now()),
                )
            except sqlite3.IntegrityError:
                raise BusinessError("案件编号已存在", 409, "case_exists")
            case_id = cur.lastrowid
            conn.execute(
                "INSERT INTO case_members(case_id,user_id,role,granted_by,granted_at) VALUES(?,?,?,?,?)",
                (case_id, user_id, "custodian", user_id, now()),
            )
            self._audit(conn, case_id, user_id, "case.create", {"case_number": case_number.strip()})
            return {"id": case_id, "case_number": case_number.strip(), "title": title.strip()}

    def add_member(self, user_id, case_id, member_id, role):
        if role not in MEMBER_ROLES:
            raise BusinessError("案件角色必须是 custodian、analyst 或 auditor", 422, "invalid_role")
        with self.connect() as conn:
            case = self._case(conn, case_id)
            if case["created_by"] != user_id:
                raise BusinessError("只有案件创建人可以授权成员", 403, "forbidden")
            self._user(conn, member_id)
            conn.execute(
                """INSERT INTO case_members(case_id,user_id,role,active,granted_by,granted_at) VALUES(?,?,?,1,?,?)
                   ON CONFLICT(case_id,user_id) DO UPDATE SET role=excluded.role,active=1,granted_by=excluded.granted_by,granted_at=excluded.granted_at""",
                (case_id, member_id, role, user_id, now()),
            )
            self._audit(conn, case_id, user_id, "member.grant", {"member_id": member_id, "role": role})
            return {"case_id": case_id, "member_id": member_id, "role": role}

    @staticmethod
    def _event_hash(event):
        canonical = json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(canonical).hexdigest()

    def _append_event(self, conn, evidence_id, event_type, actor, from_person="", to_person="", location="", note="", loan_id=None):
        previous = conn.execute(
            "SELECT event_hash,sequence FROM custody_events WHERE evidence_id=? ORDER BY sequence DESC LIMIT 1", (evidence_id,)
        ).fetchone()
        sequence = (previous["sequence"] + 1) if previous else 1
        previous_hash = previous["event_hash"] if previous else "GENESIS"
        payload = {
            "evidence_id": evidence_id, "sequence": sequence, "event_type": event_type,
            "actor_id": actor, "from_person": from_person or None, "to_person": to_person or None,
            "location": location, "note": note, "previous_hash": previous_hash, "created_at": now(),
        }
        digest = self._event_hash(payload)
        cur = conn.execute(
            """INSERT INTO custody_events(evidence_id,sequence,event_type,actor_id,from_person,to_person,location,note,previous_hash,event_hash,created_at,loan_id)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (evidence_id, sequence, event_type, actor, from_person or None, to_person or None, location, note, previous_hash, digest, payload["created_at"], loan_id),
        )
        return cur.lastrowid, digest

    def ingest_evidence(self, user_id, case_id, label, filename, content_b64, retention_until, custodian=None):
        label, filename = label.strip(), filename.strip()
        if not label or not filename:
            raise BusinessError("证据标签和文件名不能为空", 422, "invalid_evidence")
        try:
            content = base64.b64decode(content_b64, validate=True)
            deadline = date.fromisoformat(retention_until)
        except (binascii.Error, ValueError, TypeError):
            raise BusinessError("证据内容 Base64 或保留期限格式错误", 422, "invalid_evidence")
        if deadline < date.today():
            raise BusinessError("保留期限不能早于今天", 422, "invalid_retention")
        digest = hashlib.sha256(content).hexdigest()
        custodian = (custodian or user_id).strip()
        with self.connect() as conn:
            _, member = self._member(conn, case_id, user_id, {"custodian"})
            if not custodian:
                raise BusinessError("保管人不能为空", 422, "invalid_custodian")
            try:
                conn.execute("BEGIN IMMEDIATE")
                cur = conn.execute(
                    """INSERT INTO evidence(case_id,label,filename,sha256,size,content,current_custodian,retention_until,created_by,created_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (case_id, label, filename, digest, len(content), content, custodian, retention_until, user_id, now()),
                )
                evidence_id = cur.lastrowid
                self._append_event(conn, evidence_id, "INGEST", user_id, to_person=custodian, note=f"入册 SHA-256 {digest}")
                self._audit(conn, case_id, user_id, "evidence.ingest", {"evidence_id": evidence_id, "sha256": digest, "label": label})
                return {"id": evidence_id, "label": label, "sha256": digest, "size": len(content), "status": "custody", "current_custodian": custodian}
            except sqlite3.IntegrityError:
                conn.rollback()
                raise BusinessError("该案件中的证据标签已存在", 409, "label_exists")
            except Exception:
                conn.rollback()
                raise

    def _evidence(self, conn, evidence_id):
        row = conn.execute("SELECT * FROM evidence WHERE id=?", (evidence_id,)).fetchone()
        if not row:
            raise BusinessError("证据不存在", 404, "not_found")
        return row

    def get_evidence(self, user_id, evidence_id, include_content=False):
        with self.connect() as conn:
            row = self._evidence(conn, evidence_id)
            self._member(conn, row["case_id"], user_id)
            result = {k: row[k] for k in row.keys() if k != "content"}
            result["legal_hold"] = bool(row["legal_hold"])
            result["integrity_valid"] = hashlib.sha256(row["content"]).hexdigest() == row["sha256"]
            result["events"] = [dict(x) for x in conn.execute("SELECT * FROM custody_events WHERE evidence_id=? ORDER BY sequence", (evidence_id,)).fetchall()]
            result["derived_children"] = [dict(x) for x in conn.execute("SELECT * FROM derivatives WHERE parent_evidence_id=? ORDER BY id", (evidence_id,)).fetchall()]
            result["loans"] = self._loans_for_evidence(conn, evidence_id)
            if include_content:
                result["content_b64"] = base64.b64encode(row["content"]).decode()
            return result

    def transfer(self, user_id, evidence_id, to_person, location, note=""):
        if not to_person.strip() or not location.strip():
            raise BusinessError("接收人和保管位置不能为空", 422, "invalid_transfer")
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = self._evidence(conn, evidence_id)
                _, member = self._member(conn, row["case_id"], user_id, {"custodian"})
                if row["status"] == "released":
                    raise BusinessError("已释放证据不能再移交", 409, "evidence_released")
                self._append_event(conn, evidence_id, "TRANSFER", user_id, from_person=row["current_custodian"], to_person=to_person.strip(), location=location.strip(), note=note.strip())
                conn.execute("UPDATE evidence SET current_custodian=? WHERE id=?", (to_person.strip(), evidence_id))
                self._audit(conn, row["case_id"], user_id, "custody.transfer", {"evidence_id": evidence_id, "to": to_person.strip(), "location": location.strip()})
                return {"id": evidence_id, "current_custodian": to_person.strip(), "location": location.strip()}
            except Exception:
                conn.rollback()
                raise

    def open_evidence(self, user_id, evidence_id, location, note=""):
        if not location.strip():
            raise BusinessError("开箱地点不能为空", 422, "location_required")
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = self._evidence(conn, evidence_id)
                _, member = self._member(conn, row["case_id"], user_id, {"custodian"})
                if row["status"] != "custody":
                    raise BusinessError("只有处于封存保管状态的证据可以开箱", 409, "invalid_status")
                self._append_event(conn, evidence_id, "OPEN", user_id, from_person=row["current_custodian"], location=location.strip(), note=note.strip())
                conn.execute("UPDATE evidence SET status='opened' WHERE id=?", (evidence_id,))
                self._audit(conn, row["case_id"], user_id, "evidence.open", {"evidence_id": evidence_id, "location": location.strip()})
                return {"id": evidence_id, "status": "opened", "location": location.strip()}
            except Exception:
                conn.rollback()
                raise

    def derive(self, user_id, evidence_id, method, label, filename, content_b64):
        if len(method.strip()) < 3 or not label.strip() or not filename.strip():
            raise BusinessError("分析方法、子证据标签和文件名不能为空", 422, "invalid_derivative")
        try:
            content = base64.b64decode(content_b64, validate=True)
        except (binascii.Error, ValueError, TypeError):
            raise BusinessError("content_b64 不是合法 Base64", 422, "invalid_base64")
        digest = hashlib.sha256(content).hexdigest()
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                parent = self._evidence(conn, evidence_id)
                _, member = self._member(conn, parent["case_id"], user_id, {"analyst"})
                if parent["status"] != "opened":
                    raise BusinessError("原始证据必须先开箱才能分析", 409, "evidence_not_opened")
                cur = conn.execute(
                    """INSERT INTO evidence(case_id,label,filename,sha256,size,content,status,current_custodian,legal_hold,retention_until,created_by,created_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (parent["case_id"], label.strip(), filename.strip(), digest, len(content), content, "derivative", user_id, 0, parent["retention_until"], user_id, now()),
                )
                child_id = cur.lastrowid
                conn.execute(
                    "INSERT INTO derivatives(parent_evidence_id,child_evidence_id,method,actor_id,created_at) VALUES(?,?,?,?,?)",
                    (evidence_id, child_id, method.strip(), user_id, now()),
                )
                self._append_event(conn, evidence_id, "ANALYZE", user_id, from_person=parent["current_custodian"], note=f"生成衍生证据 #{child_id}: {method.strip()}")
                self._append_event(conn, child_id, "INGEST", user_id, from_person=parent["current_custodian"], to_person=user_id, note=f"由证据 #{evidence_id} 派生，SHA-256 {digest}")
                self._audit(conn, parent["case_id"], user_id, "evidence.derive", {"parent_id": evidence_id, "child_id": child_id, "method": method.strip(), "sha256": digest})
                return {"id": child_id, "parent_id": evidence_id, "label": label.strip(), "sha256": digest, "status": "derivative"}
            except sqlite3.IntegrityError:
                conn.rollback()
                raise BusinessError("衍生证据标签已存在", 409, "label_exists")
            except Exception:
                conn.rollback()
                raise

    def set_hold(self, user_id, evidence_id, hold, reason):
        if len(reason.strip()) < 5:
            raise BusinessError("法律保留原因至少 5 字", 422, "reason_required")
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = self._evidence(conn, evidence_id)
                case = self._case(conn, row["case_id"])
                if user_id != case["created_by"]:
                    self._member(conn, row["case_id"], user_id, {"auditor"})
                conn.execute("UPDATE evidence SET legal_hold=? WHERE id=?", (int(bool(hold)), evidence_id))
                event = "HOLD_SET" if hold else "HOLD_CLEARED"
                self._append_event(conn, evidence_id, event, user_id, note=reason.strip())
                if hold:
                    # 法律保留：待放行单自动失效，借出中的单按保留期限重算提醒
                    voided = self._void_pending_loans(conn, evidence_id, "法律保留：" + reason.strip(), user_id)
                    recalculated = self._recalc_loan_reminders(conn, evidence_id)
                else:
                    voided, recalculated = [], []
                self._audit(conn, row["case_id"], user_id, "evidence.hold", {"evidence_id": evidence_id, "hold": bool(hold), "reason": reason.strip(), "voided_loans": voided, "recalculated_loans": recalculated})
                return {"id": evidence_id, "legal_hold": bool(hold), "voided_loans": voided, "recalculated_loans": recalculated}
            except Exception:
                conn.rollback()
                raise

    def release(self, user_id, evidence_id, recipient, note=""):
        if not recipient.strip():
            raise BusinessError("接收方不能为空", 422, "recipient_required")
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = self._evidence(conn, evidence_id)
                _, member = self._member(conn, row["case_id"], user_id, {"custodian"})
                if row["legal_hold"]:
                    raise BusinessError("存在法律保留，禁止释放证据", 409, "legal_hold_active")
                if row["status"] == "released":
                    raise BusinessError("证据已经释放", 409, "already_released")
                self._append_event(conn, evidence_id, "RELEASE", user_id, from_person=row["current_custodian"], to_person=recipient.strip(), note=note.strip())
                conn.execute("UPDATE evidence SET status='released' WHERE id=?", (evidence_id,))
                # 证据释放：待放行单自动失效，借出中的单按保留期限重算提醒
                voided = self._void_pending_loans(conn, evidence_id, "证据释放：" + recipient.strip(), user_id)
                recalculated = self._recalc_loan_reminders(conn, evidence_id)
                self._audit(conn, row["case_id"], user_id, "evidence.release", {"evidence_id": evidence_id, "recipient": recipient.strip(), "voided_loans": voided, "recalculated_loans": recalculated})
                return {"id": evidence_id, "status": "released", "recipient": recipient.strip(), "voided_loans": voided, "recalculated_loans": recalculated}
            except Exception:
                conn.rollback()
                raise

    # ---------------- 借调单（保管链） ----------------

    def _loan_row(self, conn, loan_id):
        loan = conn.execute("SELECT * FROM loans WHERE id=?", (loan_id,)).fetchone()
        if not loan:
            raise BusinessError("借调单不存在", 404, "loan_not_found")
        return loan

    def _next_loan_no(self, conn, case_id):
        seq = conn.execute("SELECT COUNT(*) AS c FROM loans WHERE case_id=?", (case_id,)).fetchone()["c"] + 1
        return f"LOAN-{case_id}-{seq:04d}"

    def _next_receipt_no(self, conn):
        seq = conn.execute("SELECT COUNT(*) AS c FROM loan_receipts").fetchone()["c"] + 1
        return f"RCP-{seq:06d}"

    def _recalc_loan_reminders(self, conn, evidence_id):
        """借出中的借调单按证据最新保留期限重算应还/提醒日期。"""
        evidence = conn.execute("SELECT retention_until FROM evidence WHERE id=?", (evidence_id,)).fetchone()
        if not evidence:
            return []
        retention = date.fromisoformat(evidence["retention_until"])
        due = retention.isoformat()
        remind = (retention - timedelta(days=LOAN_REMIND_DAYS)).isoformat()
        rows = conn.execute(
            "SELECT id, loan_no FROM loans WHERE evidence_id=? AND status='out'", (evidence_id,)
        ).fetchall()
        for r in rows:
            conn.execute(
                "UPDATE loans SET due_at=?, remind_at=?, updated_at=? WHERE id=?",
                (due, remind, now(), r["id"]),
            )
        return [r["loan_no"] for r in rows]

    def _void_pending_loans(self, conn, evidence_id, reason, actor_id):
        """待放行借调单自动失效，并追加 LOAN_VOID 保管事件。返回失效借调单编号。"""
        pending = conn.execute(
            "SELECT id, loan_no, case_id FROM loans WHERE evidence_id=? AND status='pending'", (evidence_id,)
        ).fetchall()
        for loan in pending:
            conn.execute(
                "UPDATE loans SET status='void', void_reason=?, updated_at=? WHERE id=?",
                (reason, now(), loan["id"]),
            )
            self._append_event(conn, evidence_id, "LOAN_VOID", actor_id, note=f"借调单 {loan['loan_no']} 自动失效：{reason}", loan_id=loan["id"])
            self._audit(conn, loan["case_id"], actor_id, "loan.void", {"loan_id": loan["id"], "loan_no": loan["loan_no"], "reason": reason})
        return [l["loan_no"] for l in pending]

    def _assert_can_manage_loan(self, conn, case, user_id, allow_creator=True):
        """借调单管理权限：创建人或保管员。"""
        if allow_creator and case["created_by"] == user_id:
            return
        self._member(conn, case["id"], user_id, {"custodian"})

    def initiate_loan(self, user_id, evidence_id, borrower, purpose):
        if not borrower.strip() or len(purpose.strip()) < 3:
            raise BusinessError("借调接收人和事由不能为空（事由至少 3 字）", 422, "invalid_loan")
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = self._evidence(conn, evidence_id)
                case = self._case(conn, row["case_id"])
                self._member(conn, row["case_id"], user_id, {"custodian"})
                if row["status"] == "released":
                    raise BusinessError("证据已释放，不能发起借调", 409, "evidence_released")
                # 同一证据只允许一条有效借调单
                active = conn.execute(
                    "SELECT id, loan_no, status FROM loans WHERE evidence_id=? AND status IN ('pending','out') ORDER BY id LIMIT 1",
                    (evidence_id,),
                ).fetchone()
                if active:
                    raise BusinessError(
                        f"该证据已有有效借调单 {active['loan_no']}（{active['status']}），不能重复发起",
                        409, "loan_conflict",
                        extra={"conflict_loan_id": active["id"], "conflict_loan_no": active["loan_no"], "conflict_status": active["status"]},
                    )
                loan_no = self._next_loan_no(conn, row["case_id"])
                ts = now()
                cur = conn.execute(
                    """INSERT INTO loans(loan_no,case_id,evidence_id,status,initiator_id,borrower,purpose,
                                         receipt_status,created_at,updated_at)
                       VALUES(?,?,?,'pending',?,?,?,'none',?,?)""",
                    (loan_no, row["case_id"], evidence_id, user_id, borrower.strip(), purpose.strip(), ts, ts),
                )
                loan_id = cur.lastrowid
                self._audit(conn, row["case_id"], user_id, "loan.initiate",
                            {"loan_id": loan_id, "loan_no": loan_no, "evidence_id": evidence_id, "borrower": borrower.strip()})
                return self._loan_to_dict(conn, conn.execute("SELECT * FROM loans WHERE id=?", (loan_id,)).fetchone())
            except Exception:
                conn.rollback()
                raise

    def approve_loan(self, user_id, loan_id):
        """创建人放行：待放行 -> 借出中（出库），并生成回执。"""
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                loan = self._loan_row(conn, loan_id)
                row = self._evidence(conn, loan["evidence_id"])
                case = self._case(conn, row["case_id"])
                if case["created_by"] != user_id:
                    raise BusinessError("只有案件创建人可以放行借调单", 403, "forbidden")
                if loan["status"] == "out":
                    # 两人同时放行同一证据：后到者收到冲突编号
                    raise BusinessError(
                        f"借调单 {loan['loan_no']} 已放行出库，不能重复放行",
                        409, "loan_already_approved",
                        extra={"conflict_loan_id": loan["id"], "conflict_loan_no": loan["loan_no"]},
                    )
                if loan["status"] in ("closed", "void"):
                    raise BusinessError(f"借调单已{('结束' if loan['status']=='closed' else '失效')}，不能放行", 409, "loan_not_pending")
                if loan["status"] != "pending":
                    raise BusinessError("借调单当前状态不可放行", 409, "loan_not_pending")
                # 出库：借出中的单按保留期限重算日程
                retention = date.fromisoformat(row["retention_until"])
                ts = now()
                conn.execute(
                    """UPDATE loans SET status='out', approver_id=?, out_at=?, due_at=?, remind_at=?,
                                       receipt_status='pending', updated_at=? WHERE id=?""",
                    (user_id, ts, retention.isoformat(), (retention - timedelta(days=LOAN_REMIND_DAYS)).isoformat(), ts, loan_id),
                )
                self._append_event(conn, loan["evidence_id"], "LOAN_OUT", user_id,
                                   from_person=row["current_custodian"], to_person=loan["borrower"],
                                   note=f"借调单 {loan['loan_no']} 放行出库，借调给 {loan['borrower']}", loan_id=loan_id)
                # 生成回执并尝试传输
                receipt = self._create_receipt(conn, loan_id, loan["evidence_id"])
                self._transmit_receipt(conn, receipt)
                self._audit(conn, row["case_id"], user_id, "loan.approve",
                            {"loan_id": loan_id, "loan_no": loan["loan_no"], "evidence_id": loan["evidence_id"],
                             "receipt_no": receipt["receipt_no"]})
                return self._loan_to_dict(conn, conn.execute("SELECT * FROM loans WHERE id=?", (loan_id,)).fetchone())
            except Exception:
                conn.rollback()
                raise

    def _create_receipt(self, conn, loan_id, evidence_id):
        existing = conn.execute("SELECT * FROM loan_receipts WHERE loan_id=?", (loan_id,)).fetchone()
        if existing:
            return existing
        receipt_no = self._next_receipt_no(conn)
        ts = now()
        cur = conn.execute(
            """INSERT INTO loan_receipts(receipt_no,loan_id,evidence_id,status,attempts,created_at,updated_at)
               VALUES(?,?,?,'pending',0,?,?)""",
            (receipt_no, loan_id, evidence_id, ts, ts),
        )
        return conn.execute("SELECT * FROM loan_receipts WHERE id=?", (cur.lastrowid,)).fetchone()

    def _transmit_receipt(self, conn, receipt):
        """模拟回执传输：成功 -> pending（待确认），失败 -> failed。"""
        ts = now()
        attempts = receipt["attempts"] + 1
        if self.receipt_fail:
            conn.execute(
                "UPDATE loan_receipts SET status='failed', attempts=?, last_error=?, transmitted_at=?, updated_at=? WHERE id=?",
                (attempts, "回执传输失败（模拟）", ts, ts, receipt["id"]),
            )
        else:
            conn.execute(
                "UPDATE loan_receipts SET status='pending', attempts=?, last_error=NULL, transmitted_at=?, updated_at=? WHERE id=?",
                (attempts, ts, ts, receipt["id"]),
            )
        return conn.execute("SELECT * FROM loan_receipts WHERE id=?", (receipt["id"],)).fetchone()

    def confirm_receipt(self, user_id, loan_id):
        """回执确认：借出中 -> 已结束（归还），回执已确认。"""
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                loan = self._loan_row(conn, loan_id)
                row = self._evidence(conn, loan["evidence_id"])
                case = self._case(conn, row["case_id"])
                self._assert_can_manage_loan(conn, case, user_id)
                receipt = conn.execute("SELECT * FROM loan_receipts WHERE loan_id=?", (loan_id,)).fetchone()
                if loan["status"] == "closed":
                    # 幂等：已确认结束的借调单不重复出库/事件
                    return self._loan_to_dict(conn, loan)
                if loan["status"] != "out":
                    raise BusinessError("只有借出中的借调单可以确认回执", 409, "loan_not_out")
                if not receipt:
                    raise BusinessError("回执不存在", 404, "receipt_not_found")
                if receipt["status"] == "failed":
                    raise BusinessError("回执传输失败，请先重试传输", 409, "receipt_failed")
                if receipt["status"] != "confirmed":
                    ts = now()
                    conn.execute(
                        "UPDATE loan_receipts SET status='confirmed', confirmed_at=?, updated_at=? WHERE id=?",
                        (ts, ts, receipt["id"]),
                    )
                    conn.execute(
                        "UPDATE loans SET status='closed', receipt_status='confirmed', receipt_confirmed_at=?, updated_at=? WHERE id=?",
                        (ts, ts, loan_id),
                    )
                    self._append_event(conn, loan["evidence_id"], "LOAN_RETURN", user_id,
                                       from_person=loan["borrower"], to_person=row["current_custodian"],
                                       note=f"借调单 {loan['loan_no']} 回执确认，证据归还入库", loan_id=loan_id)
                    self._audit(conn, row["case_id"], user_id, "loan.confirm_receipt",
                                {"loan_id": loan_id, "loan_no": loan["loan_no"], "receipt_no": receipt["receipt_no"]})
                return self._loan_to_dict(conn, conn.execute("SELECT * FROM loans WHERE id=?", (loan_id,)).fetchone())
            except Exception:
                conn.rollback()
                raise

    def retry_receipts(self, user_id, case_id, loan_id=None):
        """重试传输未确认回执；已确认的不重复出库。"""
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                case = self._case(conn, case_id)
                self._assert_can_manage_loan(conn, case, user_id)
                sql = """SELECT r.* FROM loan_receipts r
                         JOIN loans l ON l.id=r.loan_id
                         WHERE l.case_id=?"""
                params = [case_id]
                if loan_id is not None:
                    sql += " AND r.loan_id=?"
                    params.append(loan_id)
                sql += " ORDER BY r.id"
                receipts = conn.execute(sql, params).fetchall()
                retried, skipped = [], []
                for r in receipts:
                    if r["status"] == "confirmed":
                        skipped.append(r["receipt_no"])  # 已确认，不重复出库
                        continue
                    updated = self._transmit_receipt(conn, r)
                    retried.append({"receipt_no": r["receipt_no"], "loan_id": r["loan_id"], "status": updated["status"], "attempts": updated["attempts"]})
                self._audit(conn, case_id, user_id, "loan.retry_receipts",
                            {"retried": [x["receipt_no"] for x in retried], "skipped_confirmed": skipped})
                return {"case_id": case_id, "retried": retried, "skipped_confirmed": skipped,
                        "retried_count": len(retried), "skipped_count": len(skipped)}
            except Exception:
                conn.rollback()
                raise

    def update_retention(self, user_id, evidence_id, retention_until):
        try:
            deadline = date.fromisoformat(retention_until)
        except (ValueError, TypeError):
            raise BusinessError("保留期限格式错误", 422, "invalid_retention")
        if deadline < date.today():
            raise BusinessError("保留期限不能早于今天", 422, "invalid_retention")
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = self._evidence(conn, evidence_id)
                case = self._case(conn, row["case_id"])
                self._assert_can_manage_loan(conn, case, user_id)
                conn.execute("UPDATE evidence SET retention_until=? WHERE id=?", (retention_until, evidence_id))
                recalculated = self._recalc_loan_reminders(conn, evidence_id)
                self._audit(conn, row["case_id"], user_id, "evidence.update_retention",
                            {"evidence_id": evidence_id, "retention_until": retention_until, "recalculated_loans": recalculated})
                return {"evidence_id": evidence_id, "retention_until": retention_until, "recalculated_loans": recalculated}
            except Exception:
                conn.rollback()
                raise

    def get_loan(self, user_id, loan_id):
        with self.connect() as conn:
            loan = self._loan_row(conn, loan_id)
            self._member(conn, loan["case_id"], user_id)
            return self._loan_to_dict(conn, loan)

    def _loan_to_dict(self, conn, loan):
        receipt = conn.execute("SELECT * FROM loan_receipts WHERE loan_id=?", (loan["id"],)).fetchone()
        out = {k: loan[k] for k in loan.keys()}
        out["receipt"] = dict(receipt) if receipt else None
        return out

    def _loans_for_evidence(self, conn, evidence_id):
        rows = conn.execute("SELECT * FROM loans WHERE evidence_id=? ORDER BY id", (evidence_id,)).fetchall()
        return [self._loan_to_dict(conn, r) for r in rows]

    def report(self, user_id, case_id):
        with self.connect() as conn:
            self._member(conn, case_id, user_id)
            case = self._case(conn, case_id)
            items, all_valid = [], True
            for row in conn.execute("SELECT * FROM evidence WHERE case_id=? ORDER BY id", (case_id,)).fetchall():
                hash_valid = hashlib.sha256(row["content"]).hexdigest() == row["sha256"]
                events = conn.execute("SELECT * FROM custody_events WHERE evidence_id=? ORDER BY sequence", (row["id"],)).fetchall()
                expected_prev, chain_valid = "GENESIS", True
                for e in events:
                    payload = {
                        "evidence_id": e["evidence_id"], "sequence": e["sequence"], "event_type": e["event_type"],
                        "actor_id": e["actor_id"], "from_person": e["from_person"], "to_person": e["to_person"],
                        "location": e["location"], "note": e["note"], "previous_hash": e["previous_hash"], "created_at": e["created_at"],
                    }
                    if e["previous_hash"] != expected_prev or self._event_hash(payload) != e["event_hash"]:
                        chain_valid = False
                    expected_prev = e["event_hash"]
                all_valid = all_valid and hash_valid and chain_valid
                items.append({
                    "id": row["id"], "label": row["label"], "filename": row["filename"], "sha256": row["sha256"],
                    "size": row["size"], "status": row["status"], "current_custodian": row["current_custodian"],
                    "legal_hold": bool(row["legal_hold"]), "retention_until": row["retention_until"],
                    "hash_valid": hash_valid, "chain_valid": chain_valid,
                    "events": [dict(e) for e in events],
                    "derivatives": [dict(x) for x in conn.execute("SELECT * FROM derivatives WHERE parent_evidence_id=? ORDER BY id", (row["id"],)).fetchall()],
                    "loans": self._loans_for_evidence(conn, row["id"]),
                })
            audit = conn.execute("SELECT * FROM audit_log WHERE case_id=? ORDER BY id", (case_id,)).fetchall()
            return {
                "case": dict(case), "generated_at": now(), "overall_integrity_valid": all_valid,
                "evidence_count": len(items), "evidence": items,
                "audit": [dict(a) | {"detail": json.loads(a["detail"])} for a in audit],
            }


class Handler(BaseHTTPRequestHandler):
    server_version = "EvidenceCustody/1.0"
    def _store(self): return self.server.store  # type: ignore[attr-defined]
    def _body(self):
        length = int(self.headers.get("Content-Length", "0"))
        try: data = json.loads(self.rfile.read(length) or b"{}")
        except (json.JSONDecodeError, UnicodeDecodeError): raise BusinessError("请求体必须是合法 JSON", 400, "invalid_json")
        if not isinstance(data, dict): raise BusinessError("JSON 顶层必须是对象", 422, "invalid_json")
        return data
    def _send(self, status, payload):
        body=json.dumps(payload,ensure_ascii=False).encode(); self.send_response(status)
        self.send_header("Content-Type","application/json; charset=utf-8"); self.send_header("Content-Length",str(len(body)))
        self.end_headers(); self.wfile.write(body)
    def _dispatch(self, method):
        path=urlparse(self.path).path.rstrip("/") or "/"; parts=[p for p in path.split("/") if p]
        user=self.headers.get("X-User-Id",""); store=self._store()
        if method=="GET" and path=="/":
            body=(BASE_DIR/"web"/"index.html").read_bytes(); self.send_response(200)
            self.send_header("Content-Type","text/html; charset=utf-8"); self.send_header("Content-Length",str(len(body)))
            self.end_headers(); self.wfile.write(body); return
        if method=="GET" and path=="/health": return self._send(200,{"ok":True})
        if parts==["api","cases"] and method=="POST":
            d=self._body(); return self._send(201,store.create_case(user,d.get("case_number",""),d.get("title","")))
        if len(parts)==4 and parts[:2]==["api","cases"] and parts[3]=="members" and method=="POST":
            d=self._body(); return self._send(201,store.add_member(user,int(parts[2]),d.get("user_id",""),d.get("role","")))
        if len(parts)==4 and parts[:2]==["api","cases"] and parts[3]=="evidence" and method=="POST":
            d=self._body(); return self._send(201,store.ingest_evidence(user,int(parts[2]),d.get("label",""),d.get("filename",""),d.get("content_b64",""),d.get("retention_until",""),d.get("custodian")))
        if len(parts)==4 and parts[:2]==["api","cases"] and parts[3]=="report" and method=="GET":
            return self._send(200,store.report(user,int(parts[2])))
        if len(parts)==5 and parts[:2]==["api","cases"] and parts[3]=="loans" and parts[4]=="retry" and method=="POST":
            return self._send(200,store.retry_receipts(user,int(parts[2])))
        if len(parts)==4 and parts[:2]==["api","evidence"] and parts[3]=="loans" and method=="POST":
            d=self._body(); return self._send(201,store.initiate_loan(user,int(parts[2]),d.get("borrower",""),d.get("purpose","")))
        if len(parts)==4 and parts[:2]==["api","evidence"] and parts[3]=="retention" and method=="POST":
            d=self._body(); return self._send(200,store.update_retention(user,int(parts[2]),d.get("retention_until","")))
        if len(parts)>=3 and parts[:2]==["api","loans"]:
            loan_id=int(parts[2])
            if len(parts)==3 and method=="GET": return self._send(200,store.get_loan(user,loan_id))
            if len(parts)==4 and method=="POST":
                if parts[3]=="approve": return self._send(200,store.approve_loan(user,loan_id))
                if parts[3]=="confirm": return self._send(200,store.confirm_receipt(user,loan_id))
                if parts[3]=="retry":
                    loan=store.get_loan(user,loan_id)
                    return self._send(200,store.retry_receipts(user,loan["case_id"],loan_id))
        if len(parts)>=3 and parts[:2]==["api","evidence"]:
            evidence_id=int(parts[2])
            if len(parts)==3 and method=="GET": return self._send(200,store.get_evidence(user,evidence_id,bool(urlparse(self.path).query)))
            if len(parts)==4 and method=="POST":
                d=self._body()
                if parts[3]=="transfer": return self._send(200,store.transfer(user,evidence_id,d.get("to_person",""),d.get("location",""),d.get("note","")))
                if parts[3]=="open": return self._send(200,store.open_evidence(user,evidence_id,d.get("location",""),d.get("note","")))
                if parts[3]=="derive": return self._send(201,store.derive(user,evidence_id,d.get("method",""),d.get("label",""),d.get("filename",""),d.get("content_b64","")))
                if parts[3]=="release": return self._send(200,store.release(user,evidence_id,d.get("recipient",""),d.get("note","")))
                if parts[3]=="hold": return self._send(200,store.set_hold(user,evidence_id,bool(d.get("hold")),d.get("reason","")))
        raise BusinessError("接口不存在",404,"not_found")
    def _handle(self, method):
        try: self._dispatch(method)
        except BusinessError as exc:
            payload={"error":{"code":exc.code,"message":exc.message}}
            if exc.extra: payload["error"].update(exc.extra)
            self._send(exc.status,payload)
        except (ValueError,TypeError): self._send(400,{"error":{"code":"invalid_path","message":"路径参数格式错误"}})
        except Exception as exc: self._send(500,{"error":{"code":"internal_error","message":str(exc)}})
    def do_GET(self): self._handle("GET")
    def do_POST(self): self._handle("POST")
    def do_DELETE(self): self._send(405,{"error":{"code":"immutable_audit","message":"证据和保管记录不提供删除接口"}})
    def log_message(self, fmt, *args): print(f"{self.address_string()} - {fmt % args}")


class CustodyServer(ThreadingHTTPServer):
    daemon_threads=True
    def __init__(self,address,store): self.store=store; super().__init__(address,Handler)


def main():
    parser=argparse.ArgumentParser(description="法律证据保管与流转后台")
    parser.add_argument("--db",default=str(DEFAULT_DB)); parser.add_argument("--port",type=int,default=8105)
    parser.add_argument("--init",action="store_true"); parser.add_argument("--seed",action="store_true")
    args=parser.parse_args(); store=CustodyStore(args.db); store.init_schema()
    if args.seed: store.seed()
    if args.init or args.seed: print(f"数据库已初始化: {args.db}"); return
    server=CustodyServer(("127.0.0.1",args.port),store); print(f"证据保管系统运行于 http://127.0.0.1:{args.port}")
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()

if __name__=="__main__": main()
