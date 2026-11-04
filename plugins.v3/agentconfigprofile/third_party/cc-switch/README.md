# CC Switch 源码复用记录

参考仓库：[farion1231/cc-switch](https://github.com/farion1231/cc-switch?tab=readme-ov-file)。

本次参考版本为提交 `7569b27af4f485172950fb6989f5643163842bd1`。
原项目使用 MIT License，完整版权与许可声明直接复制到本目录的 `LICENSE` 文件。

`agentconfigprofile/circuit_breaker.py` 将上游
`src-tauri/src/proxy/circuit_breaker.rs` 的状态转换与统计算法移植为 Python 标准库实现：

- 正常、打开、半开三种状态；连续失败或累计错误率触发熔断。
- 打开冷却结束后进入半开；仅允许一个恢复探测；连续恢复成功后关闭，失败则重新打开。
- 默认参数保持上游的 4 次失败、2 次恢复成功、60 秒冷却、0.6 失败比例及 10 次最小请求数。
- 候选可用性检查不占许可；实际请求获取许可；取消时释放许可且不计成功或失败。
- 热更新保留状态。使用可注入的单调时钟和 Python 互斥锁替代 Rust/Tokio 的时钟、原子计数及读写锁。

为了适配 MoviePilot 的同步线程入口，方法为同步调用。跨语言实现不引入 Tauri、桌面托盘、
CLI 配置文件、OAuth 或代理转发。插件的配置持久化、LLM 请求及界面由 MoviePilot 负责。
Python 实现还保留旧半开请求尚未归还的许可，避免手动恢复和请求完成交错造成多次恢复探测。

`src-tauri/src/proxy/provider_router.rs` 的故障转移队列行为已适配到插件：
用户显式加入或移出队列，自动切换严格按队列顺序尝试，并跳过熔断冷却中的供应商。

上游 `src/config/userAgentPresets.ts` 原样复制为本目录的 `userAgentPresets.ts`，
SHA-256 为 `49d62f3cdcc2c96263ae8fd67e381733be7846dc8cbe16fb22ad0af3ffa7f52a`。
`agentconfigprofile/presets.py` 通过标准库校验资源并解析其中的静态字符串数组，
页面的 User-Agent 可选这些预设，也可输入自定义值。默认仍为空，预设必须由用户选择。
不执行 TypeScript 或其中的任意表达式。

其他值得后续复用的上游实现包括
`src-tauri/src/services/model_fetch.rs` 的模型端点候选规则、
`src/config/presetSearchAliases.ts` 的供应商搜索别名数据。
这些内容仍仅记录为参考，不代表插件已经复制或实现相应功能。

界面组织参考 `src/components/providers/ProviderCard.tsx` 与 `ProviderCardActions.tsx`：
供应商身份、当前状态和主要操作放在列表中，次要操作收纳。
MoviePilot 的页面为 Vuetify 免构建组件树，原 React 组件不能直接运行；本次重新实现其适用的
信息层级，并使用宿主实际 PageRender/FormRender 验证。没有复制未使用的 React/Tauri 代码。


三模式参考 `src-tauri/src/mode/current.rs`、`controller.rs`、`state.rs`、`stack.rs`：
直连与代理路由指针独立；退出恢复直连；聚合成员使用永久key；非法聚合模型拒绝回退；
整个聚合模式禁用故障转移。OpenAI模型ID采用上游的`ccs-<key>/<model>`规则。
模式页签参考 `src/components/providers/mode/ModeTabs.tsx`，浏览状态与实际生效状态分离。
本地代理用已有Python SDK适配，CC Switch的Rust转发器不能直接在宿主Python进程中运行。
