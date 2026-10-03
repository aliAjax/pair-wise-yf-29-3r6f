"""法律证据保管与流转后台。"""
from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import sqlite3
import threading
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB = BASE_DIR / "custody.db"
MEMBER_ROLES = {"custodian", "analyst", "auditor"}
LOAN_REMINDER_LEAD_DAYS = 7  # 借调归还提醒早于保管期限/约定归还日的天数
ACTIVE_LOAN_STATUSES = ("pending", "out")


class BusinessError(Exception):
    def __init__(self, message, status=400, code="bad_request", extra=None):
        super().__init__(message)
        self.message, self.status, self.code, self.extra = message, status, code, extra or {}


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class CustodyStore:
    def __init__(self, db_path=DEFAULT_DB):
        self.db_path = str(db_path)
        self._lock = threading.Lock()

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
                CREATE TABLE IF NOT EXISTS custody_events(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    evidence_id INTEGER NOT NULL REFERENCES evidence(id), sequence INTEGER NOT NULL,
                    event_type TEXT NOT NULL CHECK(event_type IN (
                        'INGEST','TRANSFER','OPEN','ANALYZE','RELEASE','HOLD_SET','HOLD_CLEARED',
                        'LOAN_REQUEST','LOAN_OUT','LOAN_RETURN','LOAN_VOID')),
                    actor_id TEXT NOT NULL REFERENCES users(id), from_person TEXT,
                    to_person TEXT, location TEXT NOT NULL DEFAULT '', note TEXT NOT NULL DEFAULT '',
                    previous_hash TEXT NOT NULL, event_hash TEXT NOT NULL,
                    loan_id INTEGER, created_at TEXT NOT NULL, UNIQUE(evidence_id,sequence)
                );
                CREATE TABLE IF NOT EXISTS derivatives(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    parent_evidence_id INTEGER NOT NULL REFERENCES evidence(id),
                    child_evidence_id INTEGER NOT NULL UNIQUE REFERENCES evidence(id),
                    method TEXT NOT NULL, actor_id TEXT NOT NULL REFERENCES users(id),
                    created_at TEXT NOT NULL, UNIQUE(parent_evidence_id,child_evidence_id)
                );
                CREATE TABLE IF NOT EXISTS loans(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    evidence_id INTEGER NOT NULL REFERENCES evidence(id),
                    case_id INTEGER NOT NULL REFERENCES cases(id),
                    status TEXT NOT NULL CHECK(status IN ('pending','out','returned','void')),
                    requested_by TEXT NOT NULL REFERENCES users(id),
                    borrower TEXT NOT NULL, purpose TEXT NOT NULL,
                    destination TEXT NOT NULL, due_date TEXT NOT NULL,
                    released_by TEXT, released_at TEXT,
                    voided_by TEXT, voided_at TEXT, void_reason TEXT NOT NULL DEFAULT '',
                    reminder_at TEXT, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS loan_receipts(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    loan_id INTEGER NOT NULL REFERENCES loans(id),
                    receipt_number TEXT NOT NULL UNIQUE,
                    confirmed INTEGER NOT NULL DEFAULT 0 CHECK(confirmed IN (0,1)),
                    returned_by TEXT, note TEXT NOT NULL DEFAULT '',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL, confirmed_at TEXT
                );
                CREATE TABLE IF NOT EXISTS audit_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    case_id INTEGER NOT NULL REFERENCES cases(id), actor_id TEXT NOT NULL REFERENCES users(id),
                    action TEXT NOT NULL, detail TEXT NOT NULL, created_at TEXT NOT NULL
                );
                """
            )
            # 同一证据同时只允许一张有效（待放行/借出中）借调单
            conn.executescript(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_active_loan
                    ON loans(evidence_id) WHERE status IN ('pending','out');
                CREATE INDEX IF NOT EXISTS idx_loans_case ON loans(case_id);
                CREATE INDEX IF NOT EXISTS idx_receipts_unconfirmed
                    ON loan_receipts(loan_id) WHERE confirmed=0;
                """
            )
            self._migrate_custody_events(conn)

    def _migrate_custody_events(self, conn):
        """旧库 custody_events 缺少 loan_id 列且事件类型 CHECK 不覆盖借调事件，按表重建迁移。

        SQLite 不支持修改既有列约束，采用 12 步事务式重建；行 id 保持不变，
        事件哈希链完全不受影响（loan_id 在补录归属前均为 NULL，不进哈希负载）。
        """
        cols = [r[1] for r in conn.execute("PRAGMA table_info(custody_events)").fetchall()]
        if "loan_id" in cols:
            return
        conn.executescript(
            """
            ALTER TABLE custody_events RENAME TO custody_events_old;
            CREATE TABLE custody_events(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER NOT NULL REFERENCES evidence(id), sequence INTEGER NOT NULL,
                event_type TEXT NOT NULL CHECK(event_type IN (
                    'INGEST','TRANSFER','OPEN','ANALYZE','RELEASE','HOLD_SET','HOLD_CLEARED',
                    'LOAN_REQUEST','LOAN_OUT','LOAN_RETURN','LOAN_VOID')),
                actor_id TEXT NOT NULL REFERENCES users(id), from_person TEXT,
                to_person TEXT, location TEXT NOT NULL DEFAULT '', note TEXT NOT NULL DEFAULT '',
                previous_hash TEXT NOT NULL, event_hash TEXT NOT NULL,
                loan_id INTEGER, created_at TEXT NOT NULL, UNIQUE(evidence_id,sequence)
            );
            INSERT INTO custody_events
                (id,evidence_id,sequence,event_type,actor_id,from_person,to_person,location,note,
                 previous_hash,event_hash,loan_id,created_at)
            SELECT id,evidence_id,sequence,event_type,actor_id,from_person,to_person,location,note,
                   previous_hash,event_hash,NULL,created_at FROM custody_events_old;
            DROP TABLE custody_events_old;
            """
        )

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
        # loan_id 只对借调类事件入哈希；给历史 TRANSFER 补录归属时不重算（也无法重算）历史哈希
        if loan_id is not None and event_type in ("LOAN_REQUEST", "LOAN_OUT", "LOAN_RETURN", "LOAN_VOID"):
            payload["loan_id"] = loan_id
        digest = self._event_hash(payload)
        cur = conn.execute(
            """INSERT INTO custody_events(evidence_id,sequence,event_type,actor_id,from_person,to_person,location,note,previous_hash,event_hash,loan_id,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (evidence_id, sequence, event_type, actor, from_person or None, to_person or None, location, note, previous_hash, digest, loan_id, payload["created_at"]),
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
            result["loans"] = [
                self._serialize_loan(conn, x)
                for x in conn.execute("SELECT * FROM loans WHERE evidence_id=? ORDER BY id", (evidence_id,)).fetchall()
            ]
            result["derived_children"] = [dict(x) for x in conn.execute("SELECT * FROM derivatives WHERE parent_evidence_id=? ORDER BY id", (evidence_id,)).fetchall()]
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
                active_loan = self._active_loan(conn, evidence_id)
                if active_loan and active_loan["status"] == "out":
                    raise BusinessError(
                        f"证据已按借调单 #{active_loan['id']} 出库，归还确认前不能移交",
                        409, "evidence_on_loan", {"loan_id": active_loan["id"]},
                    )
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
                active_loan = self._active_loan(conn, evidence_id)
                if active_loan and active_loan["status"] == "out":
                    raise BusinessError(
                        f"证据已按借调单 #{active_loan['id']} 出库，归还确认前不能开箱",
                        409, "evidence_on_loan", {"loan_id": active_loan["id"]},
                    )
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

    def set_hold(self, user_id, evidence_id, hold, reason, retention_until=None):
        if len(reason.strip()) < 5:
            raise BusinessError("法律保留原因至少 5 字", 422, "reason_required")
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = self._evidence(conn, evidence_id)
                case = self._case(conn, row["case_id"])
                if user_id != case["created_by"]:
                    self._member(conn, row["case_id"], user_id, {"auditor"})
                new_retention = row["retention_until"]
                if retention_until is not None:
                    if not hold:
                        raise BusinessError("只有设置法律保留时才能延长保管期限", 422, "retention_requires_hold")
                    try:
                        parsed = date.fromisoformat(retention_until)
                    except ValueError:
                        raise BusinessError("新保管期限格式错误，应为 YYYY-MM-DD", 422, "invalid_retention")
                    if parsed < date.fromisoformat(row["retention_until"]):
                        raise BusinessError("法律保留只能延长、不能缩短保管期限", 422, "retention_shorten")
                    new_retention = retention_until
                conn.execute(
                    "UPDATE evidence SET legal_hold=?,retention_until=? WHERE id=?",
                    (int(bool(hold)), new_retention, evidence_id),
                )
                refreshed = self._evidence(conn, evidence_id)
                event = "HOLD_SET" if hold else "HOLD_CLEARED"
                note = reason.strip()
                if new_retention != row["retention_until"]:
                    note += f"；保管期限延长至 {new_retention}"
                self._append_event(conn, evidence_id, event, user_id, note=note)
                voided_count, recalculated = 0, []
                if hold:
                    # 法律保留后：待放行单自动失效；借出中的单按新保留期限重算提醒
                    voided_count = self._void_pending_loans(conn, refreshed, user_id, "证据进入法律保留，借调单自动失效")
                    recalculated = self._recalculate_reminders(conn, refreshed)
                self._audit(conn, row["case_id"], user_id, "evidence.hold",
                            {"evidence_id": evidence_id, "hold": bool(hold), "reason": reason.strip(),
                             "retention_until": new_retention, "pending_voided": voided_count,
                             "reminders_recalculated": len(recalculated)})
                return {"id": evidence_id, "legal_hold": bool(hold), "retention_until": new_retention,
                        "pending_loans_voided": voided_count, "loans_recalculated": recalculated}
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
                # 证据释放后不再属于本保管库：待放行单失效，借出中的单同样终止
                self._void_pending_loans(conn, row, user_id, "证据已释放，借调单自动失效")
                self._close_out_loans_on_release(conn, row, user_id, recipient.strip())
                self._append_event(conn, evidence_id, "RELEASE", user_id, from_person=row["current_custodian"], to_person=recipient.strip(), note=note.strip())
                conn.execute("UPDATE evidence SET status='released' WHERE id=?", (evidence_id,))
                self._audit(conn, row["case_id"], user_id, "evidence.release", {"evidence_id": evidence_id, "recipient": recipient.strip()})
                return {"id": evidence_id, "status": "released", "recipient": recipient.strip()}
            except Exception:
                conn.rollback()
                raise

    @staticmethod
    def _reminder_at(due_date, retention_until):
        """借调提醒日 = min(约定归还日, 保管期限) 提前 LOAN_REMINDER_LEAD_DAYS 天。"""
        limit = min(date.fromisoformat(due_date), date.fromisoformat(retention_until))
        return (limit - timedelta(days=LOAN_REMINDER_LEAD_DAYS)).isoformat()

    def _active_loan(self, conn, evidence_id, lock=False):
        sql = "SELECT * FROM loans WHERE evidence_id=? AND status IN ('pending','out') ORDER BY id LIMIT 1"
        return conn.execute(sql, (evidence_id,)).fetchone()

    def _serialize_loan(self, conn, loan):
        data = {k: loan[k] for k in loan.keys()}
        data["receipts"] = [
            {**dict(r), "confirmed": bool(r["confirmed"])}
            for r in conn.execute("SELECT * FROM loan_receipts WHERE loan_id=? ORDER BY id", (loan["id"],)).fetchall()
        ]
        return data

    def request_loan(self, user_id, evidence_id, borrower, purpose, destination, due_date):
        borrower, purpose, destination = borrower.strip(), purpose.strip(), destination.strip()
        if not borrower or len(purpose) < 3 or not destination:
            raise BusinessError("借用人、借用目的（至少 3 字）和去向不能为空", 422, "invalid_loan")
        try:
            due = date.fromisoformat(due_date)
        except (ValueError, TypeError):
            raise BusinessError("约定归还日期格式错误，应为 YYYY-MM-DD", 422, "invalid_due_date")
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = self._evidence(conn, evidence_id)
                self._member(conn, row["case_id"], user_id, {"custodian"})
                if row["status"] == "released":
                    raise BusinessError("已释放证据不能发起借调", 409, "evidence_released")
                if due < date.today():
                    raise BusinessError("约定归还日期不能早于今天", 422, "invalid_due_date")
                active = self._active_loan(conn, evidence_id)
                if active:
                    raise BusinessError(
                        f"证据已有有效借调单 #{active['id']}（{active['status']}），不能重复发起",
                        409, "loan_conflict", {"conflict_loan_id": active["id"], "conflict_status": active["status"]},
                    )
                warning = ""
                if due > date.fromisoformat(row["retention_until"]):
                    warning = "约定归还日晚于保管期限，提醒将按保管期限提前计算"
                cur = conn.execute(
                    """INSERT INTO loans(evidence_id,case_id,status,requested_by,borrower,purpose,destination,due_date,created_at)
                       VALUES(?,?,'pending',?,?,?,?,?,?)""",
                    (evidence_id, row["case_id"], user_id, borrower, purpose, destination, due_date, now()),
                )
                loan_id = cur.lastrowid
                self._append_event(
                    conn, evidence_id, "LOAN_REQUEST", user_id, to_person=borrower,
                    location=destination, note=f"借调单 #{loan_id}: {purpose}（约定 {due_date} 归还）", loan_id=loan_id,
                )
                self._audit(conn, row["case_id"], user_id, "loan.request", {"loan_id": loan_id, "evidence_id": evidence_id, "borrower": borrower})
                result = {"id": loan_id, "evidence_id": evidence_id, "status": "pending", "borrower": borrower,
                          "due_date": due_date, "destination": destination}
                if warning:
                    result["warning"] = warning
                return result
            except Exception:
                conn.rollback()
                raise

    def approve_loan(self, user_id, loan_id):
        """案件创建人放行：状态 pending→out，证据出库。并发放行只成功一条。"""
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                loan = conn.execute("SELECT * FROM loans WHERE id=?", (loan_id,)).fetchone()
                if not loan:
                    raise BusinessError("借调单不存在", 404, "not_found")
                row = self._evidence(conn, loan["evidence_id"])
                case = self._case(conn, loan["case_id"])
                if user_id != case["created_by"]:
                    raise BusinessError("只有案件创建人可以放行借调单", 403, "forbidden")
                if loan["status"] != "pending":
                    raise BusinessError(
                        f"借调单 #{loan_id} 当前状态为 {loan['status']}，不能放行",
                        409, "loan_conflict", {"conflict_loan_id": loan_id, "conflict_status": loan["status"]},
                    )
                if row["status"] == "released":
                    raise BusinessError("证据已释放，不能放行出库", 409, "evidence_released")
                if row["legal_hold"]:
                    raise BusinessError("证据处于法律保留中，不能放行出库", 409, "legal_hold_active")
                cur = conn.execute(
                    "UPDATE loans SET status='out',released_by=?,released_at=? WHERE id=? AND status='pending'",
                    (user_id, now(), loan_id),
                )
                if cur.rowcount != 1:
                    active = self._active_loan(conn, loan["evidence_id"])
                    raise BusinessError(
                        "该证据的借调单已被其他人处理",
                        409, "loan_conflict",
                        {"conflict_loan_id": active["id"] if active else loan_id,
                         "conflict_status": active["status"] if active else "out"},
                    )
                conn.execute(
                    "UPDATE loans SET reminder_at=? WHERE id=?",
                    (self._reminder_at(loan["due_date"], row["retention_until"]), loan_id),
                )
                # 出库只改保管位置，保管人不变 —— 借用人不是保管人，回执确认后仍归原保管员
                self._append_event(
                    conn, row["id"], "LOAN_OUT", user_id,
                    from_person=row["current_custodian"], to_person=loan["borrower"],
                    location=loan["destination"],
                    note=f"借调单 #{loan_id} 放行出库，约定 {loan['due_date']} 归还", loan_id=loan_id,
                )
                self._audit(conn, loan["case_id"], user_id, "loan.approve", {"loan_id": loan_id, "evidence_id": row["id"]})
                return {"id": loan_id, "evidence_id": row["id"], "status": "out",
                        "borrower": loan["borrower"], "released_by": user_id}
            except Exception:
                conn.rollback()
                raise

    def _void_pending_loans(self, conn, evidence_row, actor_id, reason):
        """法律保留或证据释放后，待放行单自动失效。"""
        pending = conn.execute(
            "SELECT * FROM loans WHERE evidence_id=? AND status='pending' ORDER BY id", (evidence_row["id"],)
        ).fetchall()
        for loan in pending:
            conn.execute(
                "UPDATE loans SET status='void',voided_by=?,voided_at=?,void_reason=? WHERE id=?",
                (actor_id, now(), reason, loan["id"]),
            )
            self._append_event(
                conn, evidence_row["id"], "LOAN_VOID", actor_id,
                to_person=loan["borrower"], note=f"借调单 #{loan['id']} 自动失效：{reason}", loan_id=loan["id"],
            )
            self._audit(conn, evidence_row["case_id"], actor_id, "loan.void",
                        {"loan_id": loan["id"], "evidence_id": evidence_row["id"], "reason": reason})
        return len(pending)

    def _close_out_loans_on_release(self, conn, evidence_row, actor_id, recipient):
        """证据直接释放时，借出中的单随证据终止（保管链登记 LOAN_VOID + RELEASE，回执仍可补登记）。"""
        outs = conn.execute(
            "SELECT * FROM loans WHERE evidence_id=? AND status='out' ORDER BY id", (evidence_row["id"],)
        ).fetchall()
        for loan in outs:
            reason = f"证据释放给 {recipient}，借调终止"
            conn.execute(
                "UPDATE loans SET status='void',voided_by=?,voided_at=?,void_reason=? WHERE id=?",
                (actor_id, now(), reason, loan["id"]),
            )
            self._append_event(
                conn, evidence_row["id"], "LOAN_VOID", actor_id,
                from_person=loan["borrower"], note=f"借调单 #{loan['id']} 终止：{reason}", loan_id=loan["id"],
            )
            self._audit(conn, evidence_row["case_id"], actor_id, "loan.void_on_release",
                        {"loan_id": loan["id"], "evidence_id": evidence_row["id"], "recipient": recipient})
        return len(outs)

    def _recalculate_reminders(self, conn, evidence_row):
        """保管期限变化后，借出中的单按新期限重算提醒。"""
        outs = conn.execute(
            "SELECT * FROM loans WHERE evidence_id=? AND status='out' ORDER BY id", (evidence_row["id"],)
        ).fetchall()
        updated = []
        for loan in outs:
            reminder = self._reminder_at(loan["due_date"], evidence_row["retention_until"])
            conn.execute("UPDATE loans SET reminder_at=? WHERE id=?", (reminder, loan["id"]))
            updated.append({"loan_id": loan["id"], "reminder_at": reminder})
        return updated

    def transmit_receipt(self, user_id, loan_id, receipt_number, note="", fail=False):
        """回执传输：落一条未确认记录。fail=True 模拟传输失败（记录保留，供重试）。"""
        receipt_number = receipt_number.strip()
        if not receipt_number:
            raise BusinessError("回执编号不能为空", 422, "receipt_number_required")
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                loan = conn.execute("SELECT * FROM loans WHERE id=?", (loan_id,)).fetchone()
                if not loan:
                    raise BusinessError("借调单不存在", 404, "not_found")
                row = self._evidence(conn, loan["evidence_id"])
                self._member(conn, row["case_id"], user_id, {"custodian"})
                if loan["status"] not in ("out", "returned"):
                    raise BusinessError(
                        f"借调单 #{loan_id} 状态为 {loan['status']}，无回执可传输", 409, "loan_not_out"
                    )
                try:
                    cur = conn.execute(
                        """INSERT INTO loan_receipts(loan_id,receipt_number,note,created_at)
                           VALUES(?,?,?,?)""",
                        (loan_id, receipt_number, note.strip(), now()),
                    )
                except sqlite3.IntegrityError:
                    existing = conn.execute(
                        "SELECT * FROM loan_receipts WHERE receipt_number=?", (receipt_number,)
                    ).fetchone()
                    raise BusinessError(
                        f"回执编号 {receipt_number} 已存在（借调单 #{existing['loan_id']}）",
                        409, "receipt_exists", {"loan_id": existing["loan_id"], "receipt_id": existing["id"]},
                    )
                receipt_id = cur.lastrowid
                if fail:
                    # 传输失败也要先留下未确认记录，便于后续按编号重试
                    conn.execute(
                        "UPDATE loan_receipts SET attempts=attempts+1,last_error=? WHERE id=?",
                        ("模拟传输失败", receipt_id),
                    )
                    self._audit(conn, row["case_id"], user_id, "receipt.transmit_failed",
                                {"loan_id": loan_id, "receipt_number": receipt_number})
                    conn.commit()
                    raise BusinessError(
                        f"回执 {receipt_number} 传输失败，记录已保留，可重试",
                        502, "receipt_transmit_failed",
                        {"loan_id": loan_id, "receipt_id": receipt_id, "receipt_number": receipt_number},
                    )
                result = self._confirm_receipt(conn, loan, row, receipt_id, receipt_number, user_id, note.strip())
                self._audit(conn, row["case_id"], user_id, "receipt.transmit",
                            {"loan_id": loan_id, "receipt_number": receipt_number, "receipt_id": receipt_id})
                return result
            except BusinessError:
                # 失败记录已 commit，rollback 对已结束事务无害
                conn.rollback()
                raise
            except Exception:
                conn.rollback()
                raise

    def _confirm_receipt(self, conn, loan, evidence_row, receipt_id, receipt_number, user_id, note):
        """确认回执：首次确认触发归还入库（LOAN_RETURN）；已确认则幂等返回，不重复出库。"""
        receipt = conn.execute("SELECT * FROM loan_receipts WHERE id=?", (receipt_id,)).fetchone()
        already_confirmed = bool(receipt["confirmed"])
        if not already_confirmed:
            conn.execute(
                "UPDATE loan_receipts SET confirmed=1,confirmed_at=?,attempts=attempts+1,last_error='' WHERE id=?",
                (now(), receipt_id),
            )
        if loan["status"] == "out":
            # 证据回到原保管员手中：出库时保管员未变，这里只登记归还事件
            conn.execute("UPDATE loans SET status='returned' WHERE id=?", (loan["id"],))
            self._append_event(
                conn, evidence_row["id"], "LOAN_RETURN", user_id,
                from_person=loan["borrower"], to_person=evidence_row["current_custodian"],
                location=evidence_row["current_custodian"],
                note=f"借调单 #{loan['id']} 回执 {receipt_number} 确认，归还入库" + (f"：{note}" if note else ""),
                loan_id=loan["id"],
            )
            self._audit(conn, evidence_row["case_id"], user_id, "loan.return",
                        {"loan_id": loan["id"], "evidence_id": evidence_row["id"], "receipt_number": receipt_number})
        return {"id": receipt_id, "loan_id": loan["id"], "receipt_number": receipt_number,
                "confirmed": True, "loan_status": "returned" if loan["status"] == "out" else loan["status"],
                "duplicate": already_confirmed}

    def retry_receipt(self, user_id, receipt_id=None, receipt_number=None):
        """重试传输失败后仍未确认的回执；已确认的幂等返回，绝不重复归还出库。"""
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                if receipt_id is not None:
                    receipt = conn.execute("SELECT * FROM loan_receipts WHERE id=?", (receipt_id,)).fetchone()
                else:
                    receipt = conn.execute(
                        "SELECT * FROM loan_receipts WHERE receipt_number=?", (receipt_number.strip(),)
                    ).fetchone()
                if not receipt:
                    raise BusinessError("回执记录不存在", 404, "not_found")
                loan = conn.execute("SELECT * FROM loans WHERE id=?", (receipt["loan_id"],)).fetchone()
                row = self._evidence(conn, loan["evidence_id"])
                self._member(conn, row["case_id"], user_id, {"custodian"})
                if receipt["confirmed"]:
                    return {"id": receipt["id"], "loan_id": loan["id"], "confirmed": True,
                            "duplicate": True, "loan_status": loan["status"]}
                result = self._confirm_receipt(conn, loan, row, receipt["id"], receipt["receipt_number"], user_id, receipt["note"])
                self._audit(conn, row["case_id"], user_id, "receipt.retry",
                            {"loan_id": loan["id"], "receipt_number": receipt["receipt_number"], "receipt_id": receipt["id"]})
                return result
            except Exception:
                conn.rollback()
                raise

    def backfill_transfer_loan(self, user_id, event_id, borrower=None, purpose=""):
        """旧库移交记录补借调归属：给历史 TRANSFER 事件挂一张已归还的借调单和已确认回执。"""
        purpose = purpose.strip() or "历史移交记录补录借调归属"
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                event = conn.execute("SELECT * FROM custody_events WHERE id=?", (event_id,)).fetchone()
                if not event:
                    raise BusinessError("保管事件不存在", 404, "not_found")
                row = self._evidence(conn, event["evidence_id"])
                case = self._case(conn, row["case_id"])
                if user_id != case["created_by"]:
                    raise BusinessError("只有案件创建人可以补录借调归属", 403, "forbidden")
                if event["event_type"] != "TRANSFER":
                    raise BusinessError("只能给移交（TRANSFER）事件补录借调归属", 422, "invalid_event_type")
                if event["loan_id"] is not None:
                    raise BusinessError(
                        f"该移交记录已归属借调单 #{event['loan_id']}",
                        409, "loan_backfilled", {"loan_id": event["loan_id"]},
                    )
                borrower = (borrower or event["to_person"] or "").strip()
                if not borrower:
                    raise BusinessError("无法从移交记录推断借用人，请显式提供", 422, "borrower_required")
                cur = conn.execute(
                    """INSERT INTO loans(evidence_id,case_id,status,requested_by,borrower,purpose,destination,due_date,
                                        released_by,released_at,reminder_at,created_at)
                       VALUES(?,?,'returned',?,?,?,?,?,?,?,?,?)""",
                    (row["id"], row["case_id"], event["actor_id"], borrower, purpose,
                     event["location"] or "历史移交去向", row["retention_until"],
                     event["actor_id"], event["created_at"], None, event["created_at"]),
                )
                loan_id = cur.lastrowid
                receipt_number = f"BF-{event_id}-{loan_id}"
                conn.execute(
                    """INSERT INTO loan_receipts(loan_id,receipt_number,confirmed,returned_by,note,attempts,created_at,confirmed_at)
                       VALUES(?,?,'1',?,?,0,?,?)""",
                    (loan_id, receipt_number, event["actor_id"], "历史移交补录回执，默认已确认", event["created_at"], event["created_at"]),
                )
                conn.execute("UPDATE custody_events SET loan_id=? WHERE id=?", (loan_id, event_id))
                self._audit(conn, row["case_id"], user_id, "loan.backfill",
                            {"loan_id": loan_id, "evidence_id": row["id"], "event_id": event_id, "borrower": borrower})
                return {"loan_id": loan_id, "event_id": event_id, "receipt_number": receipt_number,
                        "status": "returned", "borrower": borrower}
            except Exception:
                conn.rollback()
                raise

    def list_loans(self, user_id, case_id):
        with self.connect() as conn:
            self._member(conn, case_id, user_id)
            loans = conn.execute("SELECT * FROM loans WHERE case_id=? ORDER BY id", (case_id,)).fetchall()
            return {"case_id": case_id, "count": len(loans), "loans": [self._serialize_loan(conn, x) for x in loans]}

    def get_loan(self, user_id, loan_id):
        with self.connect() as conn:
            loan = conn.execute("SELECT * FROM loans WHERE id=?", (loan_id,)).fetchone()
            if not loan:
                raise BusinessError("借调单不存在", 404, "not_found")
            self._member(conn, loan["case_id"], user_id)
            return self._serialize_loan(conn, loan)

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
                    if e["loan_id"] is not None and e["event_type"] in ("LOAN_REQUEST", "LOAN_OUT", "LOAN_RETURN", "LOAN_VOID"):
                        payload["loan_id"] = e["loan_id"]
                    if e["previous_hash"] != expected_prev or self._event_hash(payload) != e["event_hash"]:
                        chain_valid = False
                    expected_prev = e["event_hash"]
                all_valid = all_valid and hash_valid and chain_valid
                loans = conn.execute(
                    "SELECT * FROM loans WHERE evidence_id=? ORDER BY id", (row["id"],)
                ).fetchall()
                loan_items = []
                for loan in loans:
                    receipts = conn.execute(
                        "SELECT * FROM loan_receipts WHERE loan_id=? ORDER BY id", (loan["id"],)
                    ).fetchall()
                    loan_items.append({
                        **{k: loan[k] for k in loan.keys()},
                        "receipts": [{**dict(r), "confirmed": bool(r["confirmed"])} for r in receipts],
                    })
                items.append({
                    "id": row["id"], "label": row["label"], "filename": row["filename"], "sha256": row["sha256"],
                    "size": row["size"], "status": row["status"], "current_custodian": row["current_custodian"],
                    "legal_hold": bool(row["legal_hold"]), "retention_until": row["retention_until"],
                    "hash_valid": hash_valid, "chain_valid": chain_valid,
                    "events": [dict(e) for e in events],
                    "loans": loan_items,
                    "derivatives": [dict(x) for x in conn.execute("SELECT * FROM derivatives WHERE parent_evidence_id=? ORDER BY id", (row["id"],)).fetchall()],
                })
            audit = conn.execute("SELECT * FROM audit_log WHERE case_id=? ORDER BY id", (case_id,)).fetchall()
            all_loans = conn.execute("SELECT * FROM loans WHERE case_id=? ORDER BY id", (case_id,)).fetchall()
            return {
                "case": dict(case), "generated_at": now(), "overall_integrity_valid": all_valid,
                "evidence_count": len(items), "evidence": items,
                "loan_count": len(all_loans), "loans": [self._serialize_loan(conn, x) for x in all_loans],
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
        if len(parts)==4 and parts[:2]==["api","cases"] and parts[3]=="loans" and method=="GET":
            return self._send(200,store.list_loans(user,int(parts[2])))
        if parts==["api","loans"] and method=="POST":
            d=self._body()
            return self._send(201,store.request_loan(user,int(d.get("evidence_id")),d.get("borrower",""),d.get("purpose",""),d.get("destination",""),d.get("due_date","")))
        if len(parts)==3 and parts[:2]==["api","loans"] and method=="GET":
            return self._send(200,store.get_loan(user,int(parts[2])))
        if len(parts)==4 and parts[:2]==["api","loans"] and method=="POST":
            loan_id=int(parts[2]); d=self._body()
            if parts[3]=="approve": return self._send(200,store.approve_loan(user,loan_id))
            if parts[3]=="receipt":
                return self._send(200,store.transmit_receipt(user,loan_id,d.get("receipt_number",""),d.get("note",""),bool(d.get("fail",False))))
        if parts==["api","receipts","retry"] and method=="POST":
            d=self._body()
            rid=d.get("receipt_id"); num=d.get("receipt_number")
            return self._send(200,store.retry_receipt(user,int(rid) if rid is not None else None,num if rid is None else None))
        if len(parts)==4 and parts[:2]==["api","custody-events"] and parts[3]=="backfill-loan" and method=="POST":
            d=self._body()
            return self._send(200,store.backfill_transfer_loan(user,int(parts[2]),d.get("borrower"),d.get("purpose","")))
        if len(parts)>=3 and parts[:2]==["api","evidence"]:
            evidence_id=int(parts[2])
            if len(parts)==3 and method=="GET": return self._send(200,store.get_evidence(user,evidence_id,bool(urlparse(self.path).query)))
            if len(parts)==4 and method=="POST":
                d=self._body()
                if parts[3]=="transfer": return self._send(200,store.transfer(user,evidence_id,d.get("to_person",""),d.get("location",""),d.get("note","")))
                if parts[3]=="open": return self._send(200,store.open_evidence(user,evidence_id,d.get("location",""),d.get("note","")))
                if parts[3]=="derive": return self._send(201,store.derive(user,evidence_id,d.get("method",""),d.get("label",""),d.get("filename",""),d.get("content_b64","")))
                if parts[3]=="release": return self._send(200,store.release(user,evidence_id,d.get("recipient",""),d.get("note","")))
                if parts[3]=="hold":
                    return self._send(200,store.set_hold(user,evidence_id,bool(d.get("hold")),d.get("reason",""),d.get("retention_until")))
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
