# 法律证据保管与流转后台

仅使用 Python 3.11+ 标准库实现的证据保管项目。支持真实 SHA-256 入册、封存/开箱/移交、分析衍生关系、案件成员权限、法律保留、保留期限、不可变保管事件链、**证据借调单全流程（发起→放行→出库→回执确认）**和 JSON 报告导出。

## 运行

```bash
python3 app.py --init --seed
python3 app.py
```

访问 <http://127.0.0.1:8105>，默认数据库 `custody.db`。测试：

```bash
python3 -m unittest -v
```

演示身份：`custodian1`、`custodian2`、`analyst1`、`auditor1`、`outsider`。请求使用 `X-User-Id`。

## 借调流程（接入保管链）

借调单状态机：`pending`（待放行）→ `out`（已出库）→ `returned`（回执确认结束）；任意未结束单可变为 `void`（失效）。

1. **保管员发起** `POST /api/loans`（custodian 角色）：登记借用人、目的、去向、约定归还日，保管链追加 `LOAN_REQUEST`。
2. **创建人放行** `POST /api/loans/{id}/approve`：仅案件创建人；放行后证据出库，追加 `LOAN_OUT`。出库期间保管人不变（借用人不是保管人），禁止移交、开箱。
3. **回执确认结束** `POST /api/loans/{id}/receipt`：保管员传输回执，确认后追加 `LOAN_RETURN`，借调单变为 `returned`。

并发与冲突规则：

- 同一证据同时只允许一张有效借调单（数据库部分唯一索引 + 事务锁保证）。两人同时发起，只有一条成功，后到者收到 `409 loan_conflict` 和 `conflict_loan_id`。
- 两人同时放行同一张单，条件更新只生效一条，后到者同样收到冲突编号。
- 法律保留（`hold`，可同时延长保管期限）后，待放行单自动 `void` 并追加 `LOAN_VOID`；借出中的单不收回，但按新保管期限 `min(约定归还日, 保管期限) - 7天` 重算 `reminder_at`。
- 证据释放（`release`）后，待放行单自动失效；借出中的单随证据释放终止。
- 回执传输失败（请求体 `"fail": true` 模拟）时记录以**未确认**落库，接口返回 `502 receipt_transmit_failed`；用 `POST /api/receipts/retry`（按 `receipt_id` 或 `receipt_number`）重试。确认按回执编号幂等：已确认记录重试返回 `duplicate: true`，**不会重复归还出库**，`LOAN_RETURN` 全链只有一条。
- `POST /api/custody-events/{eventId}/backfill-loan`：案件创建人给旧库历史 `TRANSFER` 事件补借调归属，生成一张已归还借调单和已确认补录回执（`BF-{事件}-{借调单}`）。补录只加归属外键，历史事件哈希不重算、保管链仍可校验。
- `GET /api/cases/{id}/loans`、`GET /api/loans/{id}`：查询借调单及回执。

## 主要接口

- `POST /api/cases`：创建案件，创建人自动成为保管员。
- `POST /api/cases/{id}/members`：授予 custodian、analyst 或 auditor 角色。
- `POST /api/cases/{id}/evidence`：以 Base64 入册证据，服务端计算 SHA-256 和大小。
- `GET /api/evidence/{id}`：查看元数据、完整保管事件链、完整性结果、借调单/回执和衍生关系。
- `POST /api/evidence/{id}/open`：保管员开箱。
- `POST /api/evidence/{id}/transfer`：移交保管人并记录位置（借出中禁止）。
- `POST /api/evidence/{id}/derive`：分析员从已开箱证据创建衍生证据。
- `POST /api/evidence/{id}/hold`：审计员或案件创建人设置/解除法律保留，可传 `retention_until` 延长期限。
- `POST /api/evidence/{id}/release`：存在法律保留时拒绝释放；释放会终止关联借调。
- `GET /api/cases/{id}/report`：校验所有证据哈希和每条事件链，报告内含每张证据的**借调单、保管链事件（含借调归属）、回执**及借调汇总。
- 所有 `DELETE` 请求返回 405；证据和保管记录不提供删除接口。

保管事件通过前一条事件哈希串联；借调类事件（`LOAN_REQUEST/LOAN_OUT/LOAN_RETURN/LOAN_VOID`）的 `loan_id` 参与哈希，历史 `TRANSFER` 补录归属不影响既有哈希。旧版数据库首次启动时自动迁移。报告会重新计算文件哈希和事件链。项目适合流程与完整性原型，不涵盖现实中的签名证书、WORM 存储、证据文件加密或司法辖区合规认证。
