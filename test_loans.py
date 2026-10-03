import base64
import tempfile
import threading
import unittest
from datetime import date, timedelta
from pathlib import Path

from app import LOAN_REMINDER_LEAD_DAYS, BusinessError, CustodyStore


class LoanTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = CustodyStore(Path(self.tmp.name) / "test.db")
        self.store.seed()
        self.case = self.store.create_case("custodian1", "CASE-2026-100", "借调流程案件")
        # custodian1 是创建人（放行权），custodian2 是保管员（可发起），analyst/auditor 旁观
        self.store.add_member("custodian1", self.case["id"], "custodian2", "custodian")
        self.store.add_member("custodian1", self.case["id"], "auditor1", "auditor")
        self.store.add_member("custodian1", self.case["id"], "analyst1", "analyst")
        self.retention = (date.today() + timedelta(days=365)).isoformat()
        self.due = (date.today() + timedelta(days=30)).isoformat()
        self.ev = self.store.ingest_evidence(
            "custodian1", self.case["id"], "LOAN-E1", "sealed.zip",
            base64.b64encode(b"sealed evidence").decode(), self.retention, "custodian1",
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _request(self, evidence_id=None, user="custodian2", borrower="鉴定中心王工", due=None):
        return self.store.request_loan(
            user, evidence_id or self.ev["id"], borrower, "司法鉴定需要比对原件",
            "市公安局鉴定中心 302", due or self.due,
        )

    def test_full_flow_request_approve_out_receipt_return(self):
        loan = self._request()
        self.assertEqual(loan["status"], "pending")
        # 非创建人不能放行
        with self.assertRaises(BusinessError) as ctx:
            self.store.approve_loan("custodian2", loan["id"])
        self.assertEqual(ctx.exception.status, 403)
        out = self.store.approve_loan("custodian1", loan["id"])
        self.assertEqual(out["status"], "out")
        # 出库不改保管人，借用人不是保管人
        detail = self.store.get_evidence("custodian1", self.ev["id"])
        self.assertEqual(detail["current_custodian"], "custodian1")
        # 借出中不能移交或开箱
        with self.assertRaises(BusinessError) as ctx:
            self.store.transfer("custodian1", self.ev["id"], "custodian2", "其他库房")
        self.assertEqual(ctx.exception.code, "evidence_on_loan")
        with self.assertRaises(BusinessError) as ctx:
            self.store.open_evidence("custodian1", self.ev["id"], "鉴定室")
        self.assertEqual(ctx.exception.code, "evidence_on_loan")
        # 回执确认后归还结束
        receipt = self.store.transmit_receipt("custodian2", loan["id"], "RCP-1", "原件完好归还")
        self.assertTrue(receipt["confirmed"])
        self.assertEqual(receipt["loan_status"], "returned")
        self.assertFalse(receipt["duplicate"])
        detail = self.store.get_evidence("custodian1", self.ev["id"])
        types = [e["event_type"] for e in detail["events"]]
        self.assertEqual(types, ["INGEST", "LOAN_REQUEST", "LOAN_OUT", "LOAN_RETURN"])
        # 保管链完整
        report = self.store.report("auditor1", self.case["id"])
        item = report["evidence"][0]
        self.assertTrue(item["chain_valid"])
        self.assertEqual(report["loan_count"], 1)
        self.assertEqual(item["loans"][0]["status"], "returned")
        self.assertEqual(item["loans"][0]["receipts"][0]["receipt_number"], "RCP-1")

    def test_concurrent_requests_only_one_active_loan(self):
        # 两个保管员同时发起借调，只有一张 pending，后到者拿冲突编号
        barrier = threading.Barrier(2)
        results, errors = [], []

        def go(borrower):
            barrier.wait()
            try:
                results.append(self._request(borrower=borrower))
            except BusinessError as exc:
                errors.append(exc)

        t1 = threading.Thread(target=go, args=("鉴定中心甲",))
        t2 = threading.Thread(target=go, args=("鉴定中心乙",))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(len(results), 1)
        self.assertEqual(len(errors), 1)
        loser = errors[0]
        self.assertEqual(loser.code, "loan_conflict")
        self.assertEqual(loser.extra["conflict_loan_id"], results[0]["id"])
        self.assertEqual(loser.extra["conflict_status"], "pending")

    def test_concurrent_approval_second_gets_conflict_number(self):
        loan = self._request()
        # 对同一张待放行单并发放行（两个请求同号竞争）
        outcomes = []

        def go():
            try:
                outcomes.append(("ok", self.store.approve_loan("custodian1", loan["id"])))
            except BusinessError as exc:
                outcomes.append(("err", exc))

        threads = [threading.Thread(target=go) for _ in range(2)]
        for t in threads: t.start()
        for t in threads: t.join()
        oks = [o for o in outcomes if o[0] == "ok"]
        errs = [o for o in outcomes if o[0] == "err"]
        self.assertEqual(len(oks), 1)
        self.assertEqual(len(errs), 1)
        self.assertEqual(errs[0][1].code, "loan_conflict")
        self.assertEqual(errs[0][1].extra["conflict_loan_id"], loan["id"])

    def test_second_loan_request_while_pending_conflicts(self):
        first = self._request(borrower="甲机构")
        with self.assertRaises(BusinessError) as ctx:
            self._request(borrower="乙机构")
        self.assertEqual(ctx.exception.code, "loan_conflict")
        self.assertEqual(ctx.exception.extra["conflict_loan_id"], first["id"])
        # 归还结束后可以再借
        self.store.approve_loan("custodian1", first["id"])
        self.store.transmit_receipt("custodian2", first["id"], "R-1")
        second = self._request(borrower="乙机构")
        self.assertEqual(second["status"], "pending")

    def test_hold_voids_pending_and_recalculates_out_reminders(self):
        # 待放行单：设置法律保留后自动失效
        pending = self._request(borrower="甲")
        new_retention = (date.today() + timedelta(days=720)).isoformat()
        res = self.store.set_hold("auditor1", self.ev["id"], True, "法院要求继续保全", new_retention)
        self.assertEqual(res["pending_loans_voided"], 1)
        held_loan = self.store.get_loan("custodian1", pending["id"])
        self.assertEqual(held_loan["status"], "void")
        self.assertIn("法律保留", held_loan["void_reason"])
        # 失效后可以重新发起；创建人放行会因保留被拒
        again = self._request(borrower="乙")
        with self.assertRaises(BusinessError) as ctx:
            self.store.approve_loan("custodian1", again["id"])
        self.assertEqual(ctx.exception.code, "legal_hold_active")

        # 另一张证据：放行后处于借出中，保留延长期限 -> 提醒按新期限重算。
        # due_date 取很靠后的日期（1000 天），此时短边是原保管期限 365 天；
        # 延长到 700 天后短边变成新期限，提醒随之推后。
        ev2 = self.store.ingest_evidence(
            "custodian1", self.case["id"], "LOAN-E2", "disk.img",
            base64.b64encode(b"disk image").decode(), self.retention, "custodian1",
        )
        self.store.set_hold("auditor1", self.ev["id"], False, "保全事由解除撤回保留")
        far_due = (date.today() + timedelta(days=1000)).isoformat()
        loan2 = self.store.request_loan(
            "custodian2", ev2["id"], "检测机构", "硬盘数据恢复检验", "检测机构实验室", far_due)
        self.store.approve_loan("custodian1", loan2["id"])
        before = self.store.get_loan("custodian1", loan2["id"])["reminder_at"]
        self.assertEqual(before, (date.fromisoformat(self.retention) - timedelta(days=LOAN_REMINDER_LEAD_DAYS)).isoformat())
        extended = (date.today() + timedelta(days=700)).isoformat()
        rec = self.store.set_hold("auditor1", ev2["id"], True, "上诉需要延长保管期限", extended)
        self.assertEqual(rec["loans_recalculated"],
                         [{"loan_id": loan2["id"],
                           "reminder_at": (date.fromisoformat(extended) - timedelta(days=LOAN_REMINDER_LEAD_DAYS)).isoformat()}])
        after = self.store.get_loan("custodian1", loan2["id"])["reminder_at"]
        self.assertEqual(after, (date.fromisoformat(extended) - timedelta(days=LOAN_REMINDER_LEAD_DAYS)).isoformat())
        self.assertGreater(after, before)

        # 短借期场景：due_date 早于任何保管期限，提醒始终按 due_date
        near_due = (date.today() + timedelta(days=10)).isoformat()
        ev3 = self.store.ingest_evidence(
            "custodian1", self.case["id"], "LOAN-E3", "phone.bin",
            base64.b64encode(b"phone dump").decode(), self.retention, "custodian1",
        )
        loan3 = self.store.request_loan("custodian2", ev3["id"], "机构丙", "手机取证检验", "实验室", near_due)
        self.store.approve_loan("custodian1", loan3["id"])
        long_retention = (date.today() + timedelta(days=400)).isoformat()
        r3 = self.store.set_hold("auditor1", ev3["id"], True, "重大案件长期保全", long_retention)
        self.assertEqual(r3["loans_recalculated"][0]["reminder_at"],
                         (date.fromisoformat(near_due) - timedelta(days=LOAN_REMINDER_LEAD_DAYS)).isoformat())

    def test_release_voids_pending_loan(self):
        pending = self._request()
        # 无保留即可释放
        self.store.release("custodian1", self.ev["id"], "检察机关", "按调取令释放")
        loan = self.store.get_loan("custodian1", pending["id"])
        self.assertEqual(loan["status"], "void")
        detail = self.store.get_evidence("custodian1", self.ev["id"])
        self.assertIn("LOAN_VOID", [e["event_type"] for e in detail["events"]])

    def test_receipt_failure_then_retry_confirmed_idempotent(self):
        loan = self._request()
        self.store.approve_loan("custodian1", loan["id"])
        # 第一次传输失败：留下未确认记录，抛 502
        with self.assertRaises(BusinessError) as ctx:
            self.store.transmit_receipt("custodian2", loan["id"], "RCP-FAIL", "网络抖动", fail=True)
        self.assertEqual(ctx.exception.status, 502)
        self.assertEqual(ctx.exception.code, "receipt_transmit_failed")
        rid = ctx.exception.extra["receipt_id"]
        stored = self.store.get_loan("custodian1", loan["id"])
        self.assertFalse(stored["receipts"][0]["confirmed"])
        self.assertEqual(stored["receipts"][0]["attempts"], 1)
        # 借调仍是出库状态，没有 LOAN_RETURN
        detail = self.store.get_evidence("custodian1", self.ev["id"])
        self.assertNotIn("LOAN_RETURN", [e["event_type"] for e in detail["events"]])
        # 重试成功 -> 归还确认
        retried = self.store.retry_receipt("custodian2", receipt_id=rid)
        self.assertTrue(retried["confirmed"])
        self.assertEqual(retried["loan_status"], "returned")
        # 再重试已确认记录：幂等，不重复出库
        again = self.store.retry_receipt("custodian2", receipt_number="RCP-FAIL")
        self.assertTrue(again["duplicate"])
        detail = self.store.get_evidence("custodian1", self.ev["id"])
        self.assertEqual([e["event_type"] for e in detail["events"]].count("LOAN_RETURN"), 1)

    def test_report_shows_loans_chain_and_receipts(self):
        loan = self._request()
        self.store.approve_loan("custodian1", loan["id"])
        self.store.transmit_receipt("custodian2", loan["id"], "RR-9")
        report = self.store.report("auditor1", self.case["id"])
        self.assertTrue(report["overall_integrity_valid"])
        item = report["evidence"][0]
        # 借调单、保管链事件、回执三类数据齐全
        self.assertEqual([e["event_type"] for e in item["events"]],
                         ["INGEST", "LOAN_REQUEST", "LOAN_OUT", "LOAN_RETURN"])
        self.assertEqual(item["loans"][0]["id"], loan["id"])
        self.assertEqual(item["loans"][0]["receipts"][0]["receipt_number"], "RR-9")
        self.assertTrue(item["loans"][0]["receipts"][0]["confirmed"])
        # 事件上挂借调归属
        for e in item["events"][1:]:
            self.assertEqual(e["loan_id"], loan["id"])
        self.assertIsNone(item["events"][0]["loan_id"])

    def test_backfill_old_transfer_attribution(self):
        # 模拟旧库移交：直接调 transfer，事件无 loan_id
        out = self.store.transfer("custodian1", self.ev["id"], "外地公安张警官", "异地证物暂存柜", "办案调取")
        detail = self.store.get_evidence("custodian1", self.ev["id"])
        transfer_event = next(e for e in detail["events"] if e["event_type"] == "TRANSFER")
        self.assertIsNone(transfer_event["loan_id"])
        # 非创建人不能补录
        with self.assertRaises(BusinessError) as ctx:
            self.store.backfill_transfer_loan("custodian2", transfer_event["id"], purpose="补录旧案借调")
        self.assertEqual(ctx.exception.status, 403)
        res = self.store.backfill_transfer_loan("custodian1", transfer_event["id"], purpose="补录旧案借调")
        self.assertEqual(res["status"], "returned")
        # 事件已挂归属，借调单带已确认回执
        detail = self.store.get_evidence("custodian1", self.ev["id"])
        transfer_event = next(e for e in detail["events"] if e["event_type"] == "TRANSFER")
        self.assertEqual(transfer_event["loan_id"], res["loan_id"])
        loan = self.store.get_loan("custodian1", res["loan_id"])
        self.assertEqual(loan["status"], "returned")
        self.assertTrue(loan["receipts"][0]["confirmed"])
        # 补录不改变哈希链（历史事件哈希负载不含 loan_id），报告仍完整
        report = self.store.report("auditor1", self.case["id"])
        self.assertTrue(report["evidence"][0]["chain_valid"])
        # 重复补录被拒绝
        with self.assertRaises(BusinessError) as ctx:
            self.store.backfill_transfer_loan("custodian1", transfer_event["id"])
        self.assertEqual(ctx.exception.code, "loan_backfilled")

    def test_release_terminates_out_loan(self):
        loan = self._request()
        self.store.approve_loan("custodian1", loan["id"])
        # 直接释放出库中的证据（无保留）：借调终止且链上有 LOAN_VOID + RELEASE
        self.store.release("custodian1", self.ev["id"], "法院", "判决后释放")
        detail = self.store.get_evidence("custodian1", self.ev["id"])
        types = [e["event_type"] for e in detail["events"]]
        self.assertIn("LOAN_VOID", types)
        self.assertEqual(types[-1], "RELEASE")
        self.assertEqual(self.store.get_loan("custodian1", loan["id"])["status"], "void")

    def test_permissions_who_can_request(self):
        # 只有保管员能发起借调
        with self.assertRaises(BusinessError) as ctx:
            self.store.request_loan("analyst1", self.ev["id"], "某人", "分析需要使用原件", "实验室", self.due)
        self.assertEqual(ctx.exception.status, 403)
        with self.assertRaises(BusinessError) as ctx:
            self.store.request_loan("outsider", self.ev["id"], "某人", "外部想调取原件", "外部", self.due)
        self.assertEqual(ctx.exception.status, 403)

    def test_duplicate_receipt_number_rejected(self):
        loan = self._request()
        self.store.approve_loan("custodian1", loan["id"])
        self.store.transmit_receipt("custodian2", loan["id"], "UNIQ-1")
        with self.assertRaises(BusinessError) as ctx:
            self.store.transmit_receipt("custodian2", loan["id"], "UNIQ-1")
        self.assertEqual(ctx.exception.code, "receipt_exists")


class MigrationTests(unittest.TestCase):
    """旧库（借调功能上线前的 custody.db）迁移后哈希链必须仍然可校验。"""

    def test_legacy_schema_migrates_and_chain_validates(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        import sqlite3
        db_path = Path(tmp.name) / "legacy.db"
        # 用旧版结构建库并写入一条 INGEST + TRANSFER
        with sqlite3.connect(db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE users(id TEXT PRIMARY KEY,name TEXT NOT NULL,active INTEGER NOT NULL DEFAULT 1);
                CREATE TABLE cases(id INTEGER PRIMARY KEY AUTOINCREMENT,case_number TEXT UNIQUE,title TEXT,created_by TEXT,created_at TEXT);
                CREATE TABLE case_members(case_id INTEGER,user_id TEXT,role TEXT,active INTEGER DEFAULT 1,granted_by TEXT,granted_at TEXT,PRIMARY KEY(case_id,user_id));
                CREATE TABLE evidence(id INTEGER PRIMARY KEY AUTOINCREMENT,case_id INTEGER,label TEXT,filename TEXT,sha256 TEXT,size INTEGER,content BLOB,status TEXT DEFAULT 'custody',current_custodian TEXT,legal_hold INTEGER DEFAULT 0,retention_until TEXT,created_by TEXT,created_at TEXT,UNIQUE(case_id,label));
                CREATE TABLE custody_events(id INTEGER PRIMARY KEY AUTOINCREMENT,evidence_id INTEGER,sequence INTEGER,event_type TEXT CHECK(event_type IN ('INGEST','TRANSFER','OPEN','ANALYZE','RELEASE','HOLD_SET','HOLD_CLEARED')),actor_id TEXT,from_person TEXT,to_person TEXT,location TEXT DEFAULT '',note TEXT DEFAULT '',previous_hash TEXT,event_hash TEXT,created_at TEXT,UNIQUE(evidence_id,sequence));
                CREATE TABLE derivatives(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_evidence_id INTEGER,child_evidence_id INTEGER UNIQUE,method TEXT,actor_id TEXT,created_at TEXT);
                CREATE TABLE audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT,case_id INTEGER,actor_id TEXT,action TEXT,detail TEXT,created_at TEXT);
                INSERT INTO users VALUES('custodian1','甲',1);
                INSERT INTO cases VALUES(1,'OLD-1','旧案','custodian1','2026-01-01T00:00:00+00:00');
                INSERT INTO case_members VALUES(1,'custodian1','custodian',1,'custodian1','2026-01-01T00:00:00+00:00');
                """
            )
        store = CustodyStore(db_path)
        store.init_schema()
        # 迁移后再走正常入册+借调全流程
        store.seed()
        retention = (date.today() + timedelta(days=100)).isoformat()
        item = store.ingest_evidence(
            "custodian1", 1, "OLD-E1", "a.bin",
            base64.b64encode(b"abc").decode(), retention, "custodian1",
        )
        store.transfer("custodian1", item["id"], "乙警官", "旧档案室")
        report = store.report("custodian1", 1)
        self.assertTrue(report["overall_integrity_valid"])
        # 新事件类型（借调）在迁移后的 CHECK 约束下可用
        due = (date.today() + timedelta(days=5)).isoformat()
        loan = store.request_loan("custodian1", item["id"], "丙", "旧案复查调阅原件", "复查室", due)
        store.approve_loan("custodian1", loan["id"])
        store.transmit_receipt("custodian1", loan["id"], "LEGACY-R1")
        report = store.report("custodian1", 1)
        self.assertTrue(report["overall_integrity_valid"])
        self.assertEqual(report["loan_count"], 1)


if __name__ == "__main__":
    unittest.main()
