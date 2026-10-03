# 法律证据保管与流转后台

仅使用 Python 3.11+ 标准库实现的证据保管项目。支持真实 SHA-256 入册、封存/开箱/移交、分析衍生关系、案件成员权限、法律保留、保留期限、不可变保管事件链和 JSON 报告导出。

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

## 主要接口

- `POST /api/cases`：创建案件，创建人自动成为保管员。
- `POST /api/cases/{id}/members`：授予 custodian、analyst 或 auditor 角色。
- `POST /api/cases/{id}/evidence`：以 Base64 入册证据，服务端计算 SHA-256 和大小。
- `GET /api/evidence/{id}`：查看元数据、完整保管事件链、完整性结果和衍生关系。
- `POST /api/evidence/{id}/open`：保管员开箱。
- `POST /api/evidence/{id}/transfer`：移交保管人并记录位置。
- `POST /api/evidence/{id}/derive`：分析员从已开箱证据创建衍生证据。
- `POST /api/evidence/{id}/hold`：审计员或案件创建人设置/解除法律保留。
- `POST /api/evidence/{id}/release`：存在法律保留时拒绝释放。
- `POST /api/evidence/{id}/loans`：保管员发起借调单（待放行）。
- `POST /api/loans/{id}/approve`：创建人放行后出库；同一证据只留一条有效借调单，重复放行返回 `loan_already_approved` 冲突编号。
- `POST /api/loans/{id}/confirm`：回执确认后借调结束（已确认不重复出库）。
- `POST /api/loans/{id}/retry`：重试传输未确认回执，已确认的不重复出库。
- `POST /api/cases/{id}/loans/retry`：整案重试未确认回执。
- `POST /api/evidence/{id}/retention`：更新保留期限，借出中借调单按新期限重算提醒日（提醒日 = 保留期限 - 30 天）。
- `GET /api/loans/{id}`：查看借调单与回执。
- `GET /api/cases/{id}/report`：校验所有证据哈希和每条事件链，导出完整报告（含借调单、保管链和回执）。
- 所有 `DELETE` 请求返回 405；证据和保管记录不提供删除接口。

借调流程：保管员发起 → 创建人放行出库（生成回执）→ 回执确认结束。两人同时放行同一证据时，后到者收到冲突编号；法律保留或证据释放后待放行单自动失效，借出中的单按新保留期限重算提醒；回执传输失败可重试，已确认的不重复出库。旧库移交记录（TRANSFER）在初始化时自动补录借调归属。

保管事件通过前一条事件哈希串联；报告会重新计算文件哈希和事件链。项目适合流程与完整性原型，不涵盖现实中的签名证书、WORM 存储、证据文件加密或司法辖区合规认证。
