# MP Switch 重构验证记录

日期：2026-10-30。插件名称为 MP Switch，ID 保持 agentconfigprofile，版本 3.0.0。

## 实现

- 拆分供应商配置、探活、模型发现、故障转移、页面与表单。
- 基于 CC Switch 源码适配熔断状态机和有序故障转移，原样复用 User-Agent 预设。
- 页面以当前配置、供应商列表和故障转移队列为主体；低频动作与日志按需展开。
- 设置分基础、自动切换、模型发现、全局参数四区，原有字段与默认值不变。
- 详情页和仪表盘清洗历史错误中的当前及备用模板密钥。

## 本轮验证

使用工作区隔离测试解释器 tmp/mp-deepseek-tests/Scripts/python.exe：

- 插件仓 `-m pytest tests/v3/agentconfigprofile -q`：92 passed。
- 后端 `-m pytest tests/test_plugin_agentconfigprofile.py -q`：25 passed，加载已安装插件文件。
- `-m ruff check plugins.v3/agentconfigprofile tests/v3/agentconfigprofile --select E4,E7,E9,F,I`：通过。
- 使用后端 `.pylintrc` 对插件目录执行 Pylint：通过。
- 两仓 `git diff --check`：通过。

## 页面证据

工作区 `tmp/mp-switch-ui/` 保存验证脚本、result.json 和截图。
复用本机 mp-web 的 Vue、Vuetify、PageRender 和 FormRender 产物。
仅移除隔离副本的主程序挂载，并把 HTTP 与等待弹窗替换为内存模拟边界。

1280/390 像素、亮/暗主题、正常/空态/长文本共 12 种详情状态，及配置页截图均无横向溢出。
四个设置标签在桌面和窄屏均检查通过；队列按钮事件、原生折叠、UA 预设选择/自定义/清空通过。
页面 JavaScript 异常为 0；外部字体和图标请求被主动阻断。

## 交付状态与边界

15 个源码和资源文件已同步至 `server/MoviePilot-V3/app/plugins/agentconfigprofile` 并逐字节核对。
`tmp/mp-switch-backup-2.8.0` 保留重构前源码，`tmp/mp-switch-before-ui` 保留界面重构前副本。
安装目录需要重载插件后使用新界面，本轮未重启真实服务、调用真实模型或改动真实配置。
未推送或发布远端；源码和测试保留在本地工作区。

## 2026-11-04 复核

继续任务后重新执行验证，旧结果不作为当前通过依据。

- 发布源码测试：92 passed；安装目录回归：25 passed。
- Ruff、按后端 `.pylintrc` 执行的 Pylint、差异空白检查均通过。
- 安装目录与发布目录的 15 个源码/资源文件逐字节一致。
- 重新运行隔离页面验收：15 张截图、8 个桌面/窄屏设置标签状态均无横向溢出。
- PageRender 队列事件与折叠、User-Agent 预设选择/自定义/清空均验证通过，页面 JavaScript 异常为 0。
- 实际服务仍需重载插件，真实模型调用和服务重启未执行。


## 2026-11-04 三模式实现与最终验收

依据用户追加要求，功能与界面参考CC Switch新增直连、路由、聚合。
直连与代理路由指针独立；模式页签只浏览，启用动作才改变实际模式。
路由与聚合共用loopback网关，真实上游使用宿主LLMHelper，SDK重试归代理统一管理。
聚合永久key模型精确归属供应商；失效标识不回退默认，全模式禁止跨供应商故障转移。
模式投影/停止恢复保护、工具/推理/usage/取消/stream不可重播均有隔离回归。

最终官方 `tests/run.py -q`（PATH加入已安装Git usr/bin供sh使用）：
CI 10 passed；v3 312 passed、2 skipped；v2 4 skipped；v1 1 skipped，整体exit 0。
安装目录回归 25 passed；Ruff与按后端.pylintrc的Pylint通过；git diff --check通过。

实际loopback网关集成三次模拟请求验证route切目标、stack指定模型、无效模型404。
OpenAI/Anthropic真实SDK使用MockTransport检查protocol与独占连接收尾，未连接真实供应商。
17种三模式页面/交互截图、8个表单页签状态均无溢出，页面JS异常0。
模式图标改插件内置SVG，不依赖远端图标请求。Google Fonts被隔离验收主动阻断。

19个源文件和资源已逐字节同步至本机安装目录，旧副本位于tmp/mp-switch-before-modes。
重载插件后生效；本轮未重启真实服务，也未切换用户真实模型配置。未推送/远端发布。
网关API为Chat Completions；内置助手从插件目录选择当前模型，未新增每会话模型菜单。

## 2026-11-04 本机重载与 GitHub 交付

用户授权重载插件并提交至 cudamin/MoviePilot-Plugins。
通过本机管理 API 单独重载 agentconfigprofile，宿主返回 success=true。
运行元数据确认为 MP Switch 3.0.0、runtime_status=active、runtime_compatible=true。
真实详情 API 正常生成直连、路由、聚合三模式；安装成员保持不变。
19 个安装源码和资源文件与本次发布目录逐字节一致。

原主树另有 5 个未推送的 DeepSeek 提交，本次交付从远端 main 创建独立检出，
仅复制 MP Switch 的源码、测试、市场条目及本记录，不合并其他本地提交。
提交前在该检出运行官方全量插件测试、Ruff、Pylint 与差异检查。
测试均使用隔离配置与模拟网络；实际模型请求仍未验收。
远端提交 SHA、分支及推送确认保存到工作区 tmp/mp-switch-github-delivery.json。

独立交付检出最终验证：CI 10 passed；v3 178 passed；v2 4 skipped；v1 1 skipped，
整体 exit 0，合计 188 passed、5 skipped。未包含其他任务的 DeepSeek 测试和源码。
安装副本 25 passed；Pylint、Ruff、提交差异检查通过。
Windows 测试环境统一 PYTHONUTF8=1，消除子进程输出 UTF-8/GBK 解码冲突。
