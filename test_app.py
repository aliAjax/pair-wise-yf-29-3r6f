import base64
import tempfile
import threading
import unittest
from datetime import date, timedelta
from pathlib import Path

from app import BusinessError, CustodyStore


class CustodyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = CustodyStore(Path(self.tmp.name) / "test.db")
        self.store.seed()
        self.case = self.store.create_case("custodian1", "CASE-2026-001", "跨境资金调查")
        self.store.add_member("custodian1", self.case["id"], "custodian2", "custodian")
        self.store.add_member("custodian1", self.case["id"], "analyst1", "analyst")
        self.store.add_member("custodian1", self.case["id"], "auditor1", "auditor")
        self.retention = (date.today() + timedelta(days=3650)).isoformat()

    def tearDown(self):
        self.tmp.cleanup()

    def test_full_custody_analysis_release_and_integrity_report(self):
        raw = b"bank statement original bytes"
        item = self.store.ingest_evidence(
            "custodian1", self.case["id"], "E-001", "statement.csv",
            base64.b64encode(raw).decode(), self.retention, "custodian1",
        )
        opened = self.store.open_evidence("custodian1", item["id"], "A 区证物室", "两名人员在场开箱")
        self.assertEqual(opened["status"], "opened")
        child = self.store.derive(
            "analyst1", item["id"], "CSV 提取交易记录", "E-001-D1", "transactions.json",
            base64.b64encode(b'[{"amount": 100}]').decode(),
        )
        self.store.transfer("custodian2", item["id"], "custodian2", "法院证物库", "封存后移交")
        self.store.release("custodian2", item["id"], "检察机关", "按调取令释放原件")
        report = self.store.report("auditor1", self.case["id"])
        self.assertTrue(report["overall_integrity_valid"])
        self.assertEqual(report["evidence_count"], 2)
        original = next(x for x in report["evidence"] if x["id"] == item["id"])
        self.assertEqual(original["status"], "released")
        self.assertTrue(original["chain_valid"])
        self.assertEqual(child["parent_id"], item["id"])

    def test_permissions_and_legal_hold_block_release(self):
        item = self.store.ingest_evidence(
            "custodian1", self.case["id"], "E-002", "raw.bin",
            base64.b64encode(b"evidence").decode(), self.retention,
        )
        with self.assertRaises(BusinessError) as ctx:
            self.store.get_evidence("outsider", item["id"])
        self.assertEqual(ctx.exception.status, 403)
        with self.assertRaises(BusinessError) as ctx:
            self.store.release("analyst1", item["id"], "外部机构")
        self.assertEqual(ctx.exception.status, 403)
        self.store.set_hold("auditor1", item["id"], True, "诉讼保全要求")
        with self.assertRaises(BusinessError) as ctx:
            self.store.release("custodian1", item["id"], "外部机构")
        self.assertEqual(ctx.exception.code, "legal_hold_active")


class LoanTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = CustodyStore(Path(self.tmp.name) / "loan.db")
        self.store.seed()
        self.case = self.store.create_case("custodian1", "CASE-LOAN-001", "借调流程测试")
        self.store.add_member("custodian1", self.case["id"], "custodian2", "custodian")
        self.store.add_member("custodian1", self.case["id"], "auditor1", "auditor")
        self.retention = (date.today() + timedelta(days=3650)).isoformat()

    def tearDown(self):
        self.tmp.cleanup()

    def _evidence(self, label="E-001", content=b"evidence bytes"):
        return self.store.ingest_evidence(
            "custodian1", self.case["id"], label, f"{label}.bin",
            base64.b64encode(content).decode(), self.retention, "custodian1",
        )

    def test_full_loan_lifecycle_initiate_approve_confirm(self):
        item = self._evidence()
        loan = self.store.initiate_loan("custodian1", item["id"], "检察机关", "配合调查取证")
        self.assertEqual(loan["status"], "pending")
        self.assertIsNone(loan["receipt"])
        # 创建人放行 -> 出库，回执待确认
        approved = self.store.approve_loan("custodian1", loan["id"])
        self.assertEqual(approved["status"], "out")
        self.assertEqual(approved["approver_id"], "custodian1")
        self.assertEqual(approved["receipt"]["status"], "pending")
        self.assertIsNotNone(approved["out_at"])
        # 回执确认 -> 结束
        closed = self.store.confirm_receipt("custodian1", loan["id"])
        self.assertEqual(closed["status"], "closed")
        self.assertEqual(closed["receipt"]["status"], "confirmed")
        # 幂等：重复确认不重复出库/事件
        again = self.store.confirm_receipt("custodian1", loan["id"])
        self.assertEqual(again["status"], "closed")
        events = self.store.get_evidence("auditor1", item["id"])["events"]
        self.assertEqual([e["event_type"] for e in events].count("LOAN_OUT"), 1)
        self.assertEqual([e["event_type"] for e in events].count("LOAN_RETURN"), 1)

    def test_only_one_active_loan_and_late_approval_gets_conflict_number(self):
        item = self._evidence()
        loan = self.store.initiate_loan("custodian1", item["id"], "检察机关", "调查取证")
        self.store.approve_loan("custodian1", loan["id"])
        # 两人同时放行同一证据：后到者收到冲突编号
        with self.assertRaises(BusinessError) as ctx:
            self.store.approve_loan("custodian1", loan["id"])
        self.assertEqual(ctx.exception.status, 409)
        self.assertEqual(ctx.exception.code, "loan_already_approved")
        self.assertEqual(ctx.exception.extra["conflict_loan_no"], loan["loan_no"])
        # 只留一条有效借出单
        with self.store.connect() as conn:
            active = conn.execute(
                "SELECT COUNT(*) c FROM loans WHERE evidence_id=? AND status='out'", (item["id"],)
            ).fetchone()["c"]
        self.assertEqual(active, 1)

    def test_concurrent_approval_race(self):
        item = self._evidence()
        loan = self.store.initiate_loan("custodian1", item["id"], "法院", "司法鉴定")
        results = []
        barrier = threading.Barrier(2)

        def approve():
            barrier.wait()
            try:
                results.append(("ok", self.store.approve_loan("custodian1", loan["id"])["status"]))
            except BusinessError as exc:
                results.append(("err", exc.code, exc.extra.get("conflict_loan_no")))

        t1 = threading.Thread(target=approve)
        t2 = threading.Thread(target=approve)
        t1.start(); t2.start(); t1.join(); t2.join()
        oks = [r for r in results if r[0] == "ok"]
        errs = [r for r in results if r[0] == "err"]
        self.assertEqual(len(oks), 1)
        self.assertEqual(len(errs), 1)
        self.assertEqual(errs[0][1], "loan_already_approved")
        self.assertEqual(errs[0][2], loan["loan_no"])

    def test_duplicate_initiate_blocked_when_active(self):
        item = self._evidence()
        self.store.initiate_loan("custodian1", item["id"], "检察机关", "调查取证")
        with self.assertRaises(BusinessError) as ctx:
            self.store.initiate_loan("custodian2", item["id"], "法院", "司法鉴定")
        self.assertEqual(ctx.exception.code, "loan_conflict")
        self.assertIn("conflict_loan_no", ctx.exception.extra)
        # 结束后可重新发起
        self.store.approve_loan("custodian1", 1)
        self.store.confirm_receipt("custodian1", 1)
        second = self.store.initiate_loan("custodian2", item["id"], "律师", "律师阅卷")
        self.assertEqual(second["status"], "pending")

    def test_legal_hold_voids_pending_loans(self):
        item = self._evidence()
        loan = self.store.initiate_loan("custodian1", item["id"], "检察机关", "调查取证")
        result = self.store.set_hold("auditor1", item["id"], True, "诉讼保全要求")
        self.assertIn(loan["loan_no"], result["voided_loans"])
        got = self.store.get_loan("custodian1", loan["id"])
        self.assertEqual(got["status"], "void")
        self.assertIn("法律保留", got["void_reason"])

    def test_release_voids_pending_loans(self):
        item = self._evidence()
        loan = self.store.initiate_loan("custodian1", item["id"], "公安", "立案侦查")
        result = self.store.release("custodian1", item["id"], "公安", "调取证据")
        self.assertIn(loan["loan_no"], result["voided_loans"])
        got = self.store.get_loan("custodian1", loan["id"])
        self.assertEqual(got["status"], "void")
        self.assertIn("证据释放", got["void_reason"])

    def test_retention_change_recalculates_out_loan_reminders(self):
        item = self._evidence()
        loan = self.store.initiate_loan("custodian1", item["id"], "审计", "专项核查")
        self.store.approve_loan("custodian1", loan["id"])
        new_retention = (date.today() + timedelta(days=300)).isoformat()
        result = self.store.update_retention("custodian1", item["id"], new_retention)
        self.assertIn(loan["loan_no"], result["recalculated_loans"])
        got = self.store.get_loan("custodian1", loan["id"])
        expected_remind = (date.fromisoformat(new_retention) - timedelta(days=30)).isoformat()
        self.assertEqual(got["due_at"], new_retention)
        self.assertEqual(got["remind_at"], expected_remind)

    def test_receipt_retry_skips_confirmed_and_retries_failed(self):
        item = self._evidence()
        loan = self.store.initiate_loan("custodian1", item["id"], "检察", "调查取证")
        # 传输失败
        self.store.receipt_fail = True
        approved = self.store.approve_loan("custodian1", loan["id"])
        self.assertEqual(approved["receipt"]["status"], "failed")
        with self.assertRaises(BusinessError) as ctx:
            self.store.confirm_receipt("custodian1", loan["id"])
        self.assertEqual(ctx.exception.code, "receipt_failed")
        # 失败时重试仍失败
        r = self.store.retry_receipts("custodian1", self.case["id"], loan["id"])
        self.assertEqual(r["retried"][0]["status"], "failed")
        # 恢复后重试 -> 待确认
        self.store.receipt_fail = False
        r = self.store.retry_receipts("custodian1", self.case["id"], loan["id"])
        self.assertEqual(r["retried"][0]["status"], "pending")
        # 确认后再重试：已确认的不重复出库
        self.store.confirm_receipt("custodian1", loan["id"])
        r = self.store.retry_receipts("custodian1", self.case["id"], loan["id"])
        self.assertEqual(r["retried_count"], 0)
        self.assertEqual(r["skipped_count"], 1)
        self.assertEqual(r["skipped_confirmed"], [approved["receipt"]["receipt_no"]])

    def test_report_shows_loans_chain_and_receipts(self):
        item = self._evidence()
        loan = self.store.initiate_loan("custodian1", item["id"], "检察机关", "调查取证")
        self.store.approve_loan("custodian1", loan["id"])
        self.store.confirm_receipt("custodian1", loan["id"])
        report = self.store.report("auditor1", self.case["id"])
        self.assertTrue(report["overall_integrity_valid"])
        ev = next(x for x in report["evidence"] if x["id"] == item["id"])
        self.assertEqual(len(ev["loans"]), 1)
        self.assertEqual(ev["loans"][0]["status"], "closed")
        self.assertEqual(ev["loans"][0]["receipt"]["status"], "confirmed")
        loan_events = [e for e in ev["events"] if e["event_type"].startswith("LOAN")]
        self.assertTrue(loan_events)
        self.assertTrue(all(e["loan_id"] == loan["id"] for e in loan_events))

    def test_legacy_transfer_backfilled_with_loan_attribution(self):
        item = self._evidence()
        self.store.transfer("custodian2", item["id"], "custodian2", "旧库证物室", "历史移交")
        # 重新初始化触发旧库移交记录补录
        store2 = CustodyStore(Path(self.tmp.name) / "loan.db")
        store2.init_schema()
        report = store2.report("auditor1", self.case["id"])
        ev = next(x for x in report["evidence"] if x["id"] == item["id"])
        legacy = [l for l in ev["loans"] if l["loan_no"].startswith("LOAN-LEGACY-")]
        self.assertEqual(len(legacy), 1)
        self.assertEqual(legacy[0]["status"], "closed")
        self.assertTrue(report["overall_integrity_valid"])

    def test_permissions_outsider_and_non_creator(self):
        item = self._evidence()
        with self.assertRaises(BusinessError) as ctx:
            self.store.initiate_loan("outsider", item["id"], "检察", "调查取证")
        self.assertEqual(ctx.exception.status, 403)
        loan = self.store.initiate_loan("custodian1", item["id"], "检察", "调查取证")
        # 非创建人不能放行
        with self.assertRaises(BusinessError) as ctx:
            self.store.approve_loan("custodian2", loan["id"])
        self.assertEqual(ctx.exception.status, 403)
        self.store.approve_loan("custodian1", loan["id"])


if __name__ == "__main__":
    unittest.main()
