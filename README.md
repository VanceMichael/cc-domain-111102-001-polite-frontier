# 多站点披露材料采集的公平调度

这个 Python 项目保存采集侧的来源读取工具和合成资料。代码位于 app，示例输入位于 fixtures，现有读取工具的回归位于 tests。示例不代表任何真实单位的业务记录。

## 数据与接口

来源列表定义 origin、并发额度、最短间隔和初始 URL。响应文件以完整 URL 为键，每次读取消耗一条，耗尽会报错；缺少地址返回 404。逻辑时间样例起点是 2026-11-11T12:00:00Z。User-Agent 为 DisclosureBot。响应工具不调度、不跟随跳转，也不自动重试。

## 本地使用

使用 Python 3.12 或更新版本，当前工具仅依赖标准库。从项目根目录执行：

```sh
python3 -m app.inspect
python3 -m unittest discover -s tests -v
python3 -m compileall -q app tests
```

读取命令输出当前素材的结构或单次读取结果。运行过程中产生的文件放在 runtime 中，与原始素材分开。公开示例仅使用 .invalid 域名，网络适配时应替换为已授权的来源。
