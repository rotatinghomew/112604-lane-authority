# 双车道凭车架号进区与闸机不确定交接

进区岗、监管名单与双车道设备的进区交接。

当前资料覆盖现场和业务对端使用的交换记录。数据均为合成，`samples/` 保存原始样本和字段约定，《交换资料约定》说明同一对象的编号、时间及来源。

`entry_service/` 是进区服务：名单、车道观测、纸单复核、许可申请/撤回、设备回查和越线交接围绕同一份 SQLite 状态运行。

## 运行环境

使用 Python 3.11.2，只用标准库。由项目根目录运行：

```sh
python3 -m unittest discover -s tests -v
```

## 进区服务

```sh
# 本地对端回放（可选，占用 127.0.0.1:8087）
python3 peer_replay.py

# 启动服务；--import-samples 把合成资料导入状态库（可重复执行）
python3 -m entry_service --db entry.sqlite3 --device-url http://127.0.0.1:8087 \
    --port 8099 --import-samples samples
```

服务持续运行，后台每 `--tick` 秒回查仍未确认的许可、处理过期许可并补处理
先前搁置的设备回执。`--now` 可把时钟固定，便于按样本回放。

### 主要接口

| 请求 | 用途 |
| --- | --- |
| `POST /stations` | 摄像机 / 闸机归属（按设备引用） |
| `POST /manifests` | 名单记录；请求体为列表时按整批新修订处理 |
| `POST /holds` | 监管暂停或有效名单更正 |
| `POST /observations` | 摄像机读数，按 `lane/pass/sequence` 去重 |
| `POST /paper_checks` | 纸单登记 |
| `GET /lanes/<lane>/passes/<pass>` | 该次观察的唯一结论与处置 |
| `POST /lanes/<lane>/passes/<pass>/permits` | 申请开闸（可带 `{"vin": ...}`） |
| `POST /lanes/<lane>/passes/<pass>/review` | 值班员凭纸单复核 |
| `GET /waiting` | 占用哪条车道、为何在等、是否已交场内 |
| `GET /permits/<id>` | 单车许可：权限依据、实际发生、车辆去向 |
| `POST /permits/<id>/cancel` | 撤回；设备证明不可用后才算成功 |
| `POST /permits/<id>/reconcile` | 向同一车道设备回查，不重发 |
| `POST /permits/<id>/switch` | 尚未开闸的车改走另一车道 |
| `POST /events` | 设备事件回执（可乱序、可重复） |
| `POST /tick` | 手动推进一轮后台任务 |

### 关键口径

* **唯一匹配才自动申请。** 同一 `lane/pass` 的重复读数取候选交集；只剩一个
  候选且该时刻最新名单修订放行、无监管暂停时才写入许可。
* **一车一许可。** 同一 VIN 同时最多一条未交公许可，由 SQLite 局部唯一索引
  保证，两个入口并发时只有一个成功；地感批次 `lane/pass` 越线后不能换车。
* **设备不确定不重发。** 开闸/取消没收到回话时许可停在 `pending`，只能回查
  同一车道或等设备事件；回查里拿到 `crossed` 也算设备证明。
* **许可与名单绑定。** 新修订或监管暂停会停用未使用的许可；设备已记越线时
  仍保留这次进区并交场内扣留，不把车显示成没进来。
* **越线只交接一次。** 事件先入库再处理，重复 `event_id`、同许可或同批次的
  重复越线不会重复交接；超期、车道/批次不符的越线留现场核查。
* **来源可追溯。** 观测、纸单、设备、值班员各自带 `actor/source` 写入审计，
  `GET /permits/<id>` 分开解释当时权限、实际发生和车辆去向。

## 本地对端

运行 `python3 peer_replay.py` 可在 `127.0.0.1:8087` 重放 `samples/peer.json` 中的 HTTP 录制。`--recording` 可读取另一份同格式录制，`--port` 可更改端口。每条录制依次写请求方法、路径、JSON 请求体和响应序列。没有请求体时写 null。每种请求独立推进响应，最后一项可重复读取，状态码 0 代表连接断开且未收到应答。重启重放进程会重新开始读取录制，不代表现实中的远端状态被重置。

录制按完整请求体匹配（含 `permit` 与 `expires_at`），而进区服务每次申请都会生成
新的许可号。因此 `samples/peer.json` 是设备报文的格式样本，由
`tests/test_peer.py` 校验；对接时请提供能接受服务生成的许可号的设备实现，或把
录制改写为实际请求。服务里任何“写入后无回话”的路径都会停在 `pending`，并在
`GET /waiting` 里要求回查，不会向另一条车道重发。
