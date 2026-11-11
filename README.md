# 多站点披露材料采集的公平调度

这个 Python 项目实现可持久恢复的多站点采集调度器：每个 origin 有独立的并发上限、
最短请求间隔与限流退避，按指定 User-Agent 解释 robots.txt，跨域重定向重新接受
目标站点策略约束，并把每一次允许、延迟、拒绝的原因写入决策记录。代码位于 `app`，
合成素材位于 `fixtures`，测试位于 `tests`。示例不代表任何真实单位的业务记录，
全程使用 `.invalid` 域名、虚拟时钟与响应重放，不访问真实网络、不做真实等待。

## 架构与模块

| 模块 | 职责 |
| --- | --- |
| `app/clock.py` | 时钟协议与两个实现：`RealClock`（系统时钟）、`FakeClock`（确定性虚拟时钟，可序列化进检查点，含并行延迟建模）。HTTP-date 解析也委托给时钟。 |
| `app/robots.py` | robots.txt 解释器：命名 UA 组优先于 `*`，Allow/Disallow 最长匹配、同长度 Allow 胜出，支持 `*` 与 `$`，解析 Crawl-delay。 |
| `app/transport.py` | 传输层协议与实现：`ReplayTransport`（包装现有 `app.wire.Replay`，带线程安全锁）与 `HttpTransport`（不跟随跳转、不重试）。传输层不含任何策略。 |
| `app/checkpoint.py` | 原子写检查点（临时文件 + `os.replace`）与基于 `fcntl.flock` 的跨进程排他锁。 |
| `app/journal.py` | JSON Lines 决策日志，只追加；进程重启后序号延续。 |
| `app/scheduler.py` | 调度核心：每 origin 独立线程池与闸门、429/503 Retry-After、重定向入队、同轮去重、claim/recover 与公平轮转。 |
| `app/runner.py` | 命令行入口：`run` / `resume` / `show`。 |
| `app/demo.py` | 三站点重放演示（公平性、取消、跨进程崩溃恢复）。 |

## 策略语义（对应实现位置）

- **每 origin 独立配额与线程池**：每个 origin 拥有大小等于其并发上限的独立
  `ThreadPoolExecutor`，并发计数、`last_request_at`、`backoff_until` 都挂在
  `OriginRuntime` 上；一个站点被限流或占满槽位不会冻结或借走其他站点的资源。
  主线程独占状态变更与决策日志，worker 线程只做传输 I/O。
- **robots fail-closed**：robots 暂时获取失败（传输错误或 5xx）时 origin 进入
  `paused`，到退避时间后重试，期间业务 URL 一律不发；连续失败到上限后 origin
  `disabled`，任务以 `origin-disabled` 拒绝。只有 200（含空文档）与 404 会产生规则。
- **Retry-After 两种格式**：纯数字按 delta-seconds；否则按 HTTP-date 解析，
  解析失败回退到默认退避。退避只写入对应 origin 的 `backoff_until`。
- **跨域重定向**：3xx 不自动跟随，目标 URL 作为新任务进入目标 origin 的队列，
  重新经过 robots、并发、间隔、退避闸门；原站点配额不能"带过去"。目标 origin
  未登记则拒绝。
- **同轮去重但不折叠查询参数**：去重键是完整 URL，`/report?q=1` 与 `/report?q=2`
  是两个独立任务。
- **公平性**：每个调度节拍按轮换后的 origin 顺序裁决，并让每个 origin 在节拍内
  最多填满自己的并发额度；持续有任务的站点无法饿死其他站点。
- **虚拟并行延迟**：重放传输的 `latency` 通过 `FakeClock` 的并行窗口记账，
  两个并行的 3 秒请求在虚拟时间上只流逝 3 秒；flight 需同时满足"虚拟完成时刻
  已到"和"worker 已返回"才释放并发槽，因此并发上限在无真实等待下也可被观测。
- **领取即落盘**：任务进入 `claimed` 时立即写检查点，然后才发请求；新进程恢复时
  把残留 `claimed` 一律退回 `pending` 并记录 `lease-recovered`，不会永久卡住。
- **可替换性**：时钟与传输层都是构造函数注入的协议，测试全部使用虚拟实现。

## 运行演示（无需真实等待）

使用 Python 3.12+，仅依赖标准库。从项目根目录执行：

```sh
python3 -I app/demo.py all
```

`-I` 为隔离模式：忽略用户站点包与环境变量注入的路径；项目根由脚本自身按
`__file__` 显式加入 `sys.path`，不依赖当前工作目录。演示会在 `runtime/demo_fixtures`
生成独立的合成素材目录（与代码目录分开），并依次展示：

1. **公平性**：alpha 连续收到 429（Retry-After: 5），beta、gamma 仍在 alpha 恢复前
   完成采集；gamma 的 `/private` 被 robots 拒绝、从未发请求。
2. **取消**：取消信号翻转后传输层零新请求，未完成任务留下 `cancelled` 记录。
3. **跨进程恢复**：子进程 A 在 claim 落盘后、请求发出前退出；独立的子进程 B
   从检查点恢复，记录 `lease-recovered` 并把全部任务推进到终态。

也可以只跑单个场景：`python3 -I app/demo.py fairness|cancel|crash`。

## 使用命令行运行与恢复

```sh
# 新建一轮（默认读取 fixtures/ 下的合成来源与响应，状态写入 runtime/demo）
python3 -I app/runner.py run

# 发出 5 个请求后取消（退出码 130）；检查点与决策日志保留
python3 -I app/runner.py run --runtime runtime/demo --cancel-after 5

# 从检查点恢复，继续同一个 round_id
python3 -I app/runner.py resume --runtime runtime/demo

# 模拟"领取后进程被杀死"，再用 resume 恢复
python3 -I app/runner.py run --runtime runtime/demo \
    --crash-after-claim "https://alpha.invalid/report?q=1"
python3 -I app/runner.py resume --runtime runtime/demo

# 观察决策记录（可按 verdict / origin 过滤）
python3 -I app/runner.py show --runtime runtime/demo
python3 -I app/runner.py show --runtime runtime/demo --verdict reject
python3 -I app/runner.py show --runtime runtime/demo \
    --origin https://alpha.invalid:443
```

同一 runtime 目录同时只允许一个调度进程：第二个进程获取 `scheduler.lock` 失败并
退出（退出码 3）。`run` 遇到已有检查点会拒绝启动，需显式 `--force` 或改用 `resume`。

## 观察决策记录

每个 runtime 目录包含：

- `checkpoint.json`：轮次 ID、虚拟时钟状态、每个 origin 的 robots 状态/退避截止/
  在途计数/上次请求时刻，以及全部任务的状态与尝试次数（原子写入）。
- `decisions.jsonl`：每行一条决策，字段包括 `seq`（跨进程递增）、`monotonic`、
  `wall`、`round`、`origin`、`url`、`verdict`（`allow`/`delay`/`reject`）、
  `reason`、可选的 `until` 与 `detail`。

常见 `reason`：

| verdict | reason | 含义 |
| --- | --- | --- |
| allow | `robots-fetch` / `robots-ready` | 获取并解释 robots |
| allow | `dispatch` | 闸门全部通过，请求已派发（含第几次尝试） |
| allow | `redirect` | 3xx 响应，目标已重新入队 |
| delay | `min-interval` / `crawl-delay` | 未达 origin 最短间隔或 robots Crawl-delay |
| delay | `concurrency-limit` | origin 并发额度已满 |
| delay | `retry-after` | 429/503 要求等待，`until` 为恢复时刻 |
| delay | `origin-backoff` / `robots-unavailable` / `transport-error` | origin 级暂停 |
| reject | `robots-disallow` | robots 显式拒绝，请求不会发出 |
| reject | `duplicate-url` | 同一完整 URL 在本轮已出现 |
| reject | `origin-disabled` | robots 反复失败，origin fail-closed 关闭 |
| allow | `lease-recovered` | 恢复时把残留 claimed 任务退回 pending |
| reject | `cancelled` | 取消后未完成的任务 |

## 测试

```sh
python3 -m unittest discover -s tests -v
python3 -m compileall -q app tests
```

测试覆盖 robots 解释（11 例）、调度策略、真实并发与恢复（17 例，含一个真正
`subprocess` 跨进程恢复用例）、时钟与检查点（8 例）、决策日志（3 例）以及原有的
来源/重放回归（4 例），共 43 例。时间相关断言全部基于 `FakeClock` 的虚拟时间，
例如 Retry-After 秒数与 HTTP-date 分别断言 `until == now+8` 与 `until == 20.0`，
不靠真实等待证明间隔正确；并发上限用虚拟并行延迟断言"同批最多填满额度、
跨 origin 并行不串行"。

## 边界与适配说明

- `Replay` 的响应游标保存在内存中，不随检查点持久化：因此演示与测试都把崩溃点
  安排在"claim 落盘后、请求发出前"，恢复时该 URL 的响应序列尚未被消耗。接入真实
  `HttpTransport` 后不存在该限制；检查点记录了 `attempts`，可直接对接服务端幂等键。
- `HttpTransport` 已提供但默认不启用；网络适配时把 `.invalid` 替换为已授权来源，
  并以真实时钟运行。
- 运行产物统一放在 `runtime/`（已在 `.gitignore`），与原始素材分离。
