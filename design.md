# Go Engine Runtime：Callback 驱动的设计

## 1. 定位与边界

产品定位：**推理服务的 engine-runtime（哨兵 / 包装器）**——伴随单个推理引擎实例运行，观察编排系统信号、执行生命周期动作、暴露进程健康与指标。hook/callback 引擎是当前版本的核心执行机制，但不是产品的全部身份；`engine` 一词指推理引擎本身（SGLang/vLLM 等），运行时代码不叫 engine。

首版的可交付内核是一套 Go runtime：**把已经注册的 Go Callback，在配置指定的时机可靠地执行。**

保留已确认的产品形态：可嵌入的 Go 库，以及使用同一套库构建的独立进程。

```text
Go 代码定义动作 → 注册 Callback 名字
                         ↑
events.yaml 定义执行时机与绑定
                         ↓
                 Runtime 串行执行
                         ↑
              进程生命周期 / 外部 signal
```

首版负责：
- 内置与用户 Callback 的组合注册、默认注册表和校验。
- start、hook、end 的绑定及串行执行。
- 超时、退避、取消、退出与诊断。
- 可选的 signal 观察和执行完成确认。
- 单 RoleInstance 的实验性接入。

首版不负责：
- SGLang/vLLM 的操作、router adapter、engine probe、心跳、引擎指标转换。
- 内置 webhook、HTTP 业务控制接口、Python 调用协议或 Python 进程管理。
- 动态加载 Go plugin、解释执行代码、shell 执行、DAG 或并行任务编排。
- 多 RI 集中调度、多副本选主、多 Pod 完成聚合、RBG controller 或 CRD 管理。

Callback 内部可以调用任何业务库，也可以自行访问 HTTP；这些属于使用者的业务实现，不构成框架的一层。

## 2. 三类信息分别管理

沿用旧设计中有价值的区分，但将 RI 从核心 API 中移出：

| 信息 | 定义位置 | 示例 |
| --- | --- | --- |
| 业务动作 | Go 代码，编译进二进制 | `prepareUpdate` |
| 事件绑定 | `events.yaml` 或 Go 的 Events 值 | signal 对应 `prepare-update` |
| 运行配置 | main 的 flag/env，映射成 Go Option | 目标 RI、超时、配置路径、诊断监听地址 |

YAML 不创建 Callback，不定义 URL，不动态加载实现，也不负责构造业务依赖。

### Callback API

```go
// pkg/runtime/callback
type Callback func(context.Context) error
func NewRegistry() *Registry
func (r *Registry) Register(name string, callback Callback) error

// pkg/runtime/plugin/noop（第一个官方内置插件，编译期注册，非动态加载）
func AddToRegistry(registry *callback.Registry) error

// pkg/runtime/event
func LoadEvents(reader io.Reader) (Events, error)
func Validate(events Events) error
func ParseSignal(signal string) (key, value string, err error)

// pkg/runtime（运行时本体与 SignalSource 契约）
func NewDefaultRegistry() *callback.Registry
func New(events event.Events, registry *callback.Registry, opts ...Option) (*Runtime, error)
func (r *Runtime) Run(ctx context.Context) error
```

- Callback 就是 Go 函数；方法值、闭包同样可以注册。
- 名字与函数分离，不强制业务类型实现 `Name()` 等接口。
- 注册表是实例级的，禁止空名字、nil、重名；Runtime 创建时复制并冻结其使用的名字映射，不使用全局 `init()` 注册。内置 Callback 与用户 Callback 采用相同注册和执行契约。
- Callback 只接收 context，不接收 RI、hook-state 或 signal，不直接参与 signal 确认。
- 依赖和业务目标在 main 中构造，再通过闭包或方法接收者绑定。框架不提供依赖注入容器。
- 返回 nil 表示动作完成；error 表示尚未完成。Callback 必须幂等并响应 context 取消。

### 默认注册表与组合扩展

借鉴 Kubernetes 的 `Scheme` / `AddToScheme` 模式，区分注册容器、内置集合和用户扩展：

- `NewRegistry()` 返回空注册表，供测试或需要完全控制动作集合的使用者使用。
- `NewDefaultRegistry()` 返回预装标准内置 Callback 的新注册表；每次调用都独立创建，不暴露进程级可变的 `DefaultRegistry` 单例。
- `noop.AddToRegistry(registry)` 将标准内置集合加入指定注册表；这是默认构造函数采用的注册逻辑，也支持显式组装。它只注册，不执行业务动作；重名返回错误，失败不留下部分内置项。
- 用户 Callback 包可以遵循 `mycallbacks.AddToRegistry(registry) error` 约定组织一组动作，也可以直接逐项 `Register`。内置插件与用户包使用同一约定。首版不额外引入 RegistryBuilder 类型，插件目录只放编译期注册的官方内置动作，不做动态加载。
- 内置或用户 Callback 重名都返回错误，不允许静默覆盖。需要完全不同的同名动作时，从空注册表显式组装，而不是提供隐式替换机制。
- 所有注册在 Runtime 创建前完成；之后向原注册表追加内容不影响已有 Runtime 的快照。
- 默认集合只包含不需要用户外部依赖配置的通用 Callback；需要业务参数、client 或目标的动作通过 Go 构造函数生成，再显式注册，不能在创建默认注册表时访问外部系统。
- 当前明确的默认动作仍为 `noop`。注册机制支持继续增加内置动作，但本次不凭空增加引擎操作或 webhook；其他内置动作的行为另行确定。
- “默认注册”不等于“默认绑定”：只有 events.yaml 或 Events 明确引用的动作才会执行。

### 组装方式

```go
registry := runtime.NewDefaultRegistry()
// 注册表已有内置 Callback，再添加使用者提供的 Go 函数。
registry.Register("prepare-update", prepareUpdate)
registry.Register("activate", activate)
registry.Register("cleanup", cleanup)

// 从文件读取 Events；也可以直接用 Go 构造 Events。
events, err := event.LoadEvents(reader)
// 检查所有错误后，创建并运行 Runtime。
rt, err := runtime.New(events, registry, runtime.WithSource(source))
err = rt.Run(ctx)
```

上面只展示 API 关系；实际示例必须逐项处理错误。`source` 是可选的信号观察对象，不是业务服务。用户有一组 Callback 时，可以用 `mycallbacks.AddToRegistry(registry)` 替代逐项注册；框架不根据 YAML 自动导入该包。

### events.yaml

```yaml
event_callbacks:
  start: []
  hook:
    - signal: "example.com/update-block=true"
      callback: prepare-update
    - signal: "example.com/ready-notify=true"
      callback: activate
  end:
    - cleanup
```

- 保留 PR #10 的 `event_callbacks.start/hook/end` 格式。
- start/end 是 Callback 名字列表；hook 每项是一个精确 `key=value` 与一个 Callback 名字。
- 同一 Callback 可以用于多个绑定。一个 signal 需要多个业务步骤时，由一个 Go Callback 组合这些步骤，不增加工作流配置语言。
- signal 按第一个 `=` 分割，key 非空、value 可以为空；禁止重复完整 signal 绑定。
- start/end/hook 缺省或空列表均合法；有 hook 而没有 source 时拒绝创建 Runtime。
- 严格拒绝未知字段、重复 YAML key、未知 Callback；启动前完整校验，不先执行一部分再发现配置错误。
- 配置仅启动时加载，无隐式默认业务操作、无热更新。

## 3. 库与独立进程如何交付

**Callback 必须先编译进二进制，YAML 只能选择其中已有的动作。**

- 库使用者：import 本项目，编写自己的 main，使用 `NewDefaultRegistry()` 获得内置集合，再构造依赖并注册用户 Callback、加载绑定和运行；需要完全自定义时用 `NewRegistry()`。
- 独立进程使用者：运行上述 main 构建出的二进制，使用配置调整动作的执行时机，不需要再部署任何配套业务控制服务。
- 本仓库提供 `cmd/patio` 作为标准组装入口，通过 `NewDefaultRegistry()` 使用标准内置集合，支持配置校验、进程生命周期和可选 RI source；当前已确定的内置动作是联调用 `noop`。
- 提供 `examples/custom-runtime` 展示注册自定义 Go Callback 后构建业务二进制；可以在外部项目完成，不要求修改框架源码。
- 标准二进制不承诺开箱提供业务能力。没有编译进去的名字必须报错，不能通过 YAML 凭空生成动作。

`noop` 只供显式联调；不得自动绑定到任何生产 signal。

CLI 提供 `run --config <path>` 和 `validate --config <path>`。运行参数包括 `--source=none|rbg`、RI namespace/name、kubeconfig、超时和诊断地址；默认 source 为 none，默认配置路径 `/etc/patio/events.yaml`，缺文件报错。

## 4. Runtime 的执行模型

### 生命周期

```text
校验注册和绑定
  → 启动诊断与可选 source，完成首次同步
  → 顺序执行 start
  → 持续处理当前匹配的 hook
  → 退出：停止新任务，取消并等待当前任务，顺序执行 end
```

- 一个 Runtime 面向一个逻辑目标；同一时刻最多运行一个 Callback。
- start 只表示本 runtime 进程启动，不表示引擎就绪或 RI PostReady。需要工作负载就绪后的动作，应绑定对应 signal。
- start 完成前不执行 hook；同一进程内只重试失败的 start/end 项，不重新执行已成功的前项。
- 每次从最新信号快照中选择配置顺序最靠前的匹配项；确认完成后重新获取快照。
- Callback 或确认失败时，阻止该轮后续项；不能悄悄跳过失败动作。
- 无关 source 通知不重置或绕过退避；信号失效时移除它的等待/重试状态。
- YAML 顺序只约束当前观察到的信号，不能保证不同时间到达的信号具有全局先后关系。

### 执行和确认分开

```text
待执行 → Callback 成功 → 待确认 → source 确认完成
            │                 │
          error             error
            ↓                 ↓
        重试 Callback       重试确认
```

- start/end 没有 signal，不需要确认。
- hook 的 Callback 成功后保留内存中的完成记录；确认临时失败只重试确认，避免立即重复业务动作。
- 信号已失效时丢弃旧记录，不确认新信号。
- 重启会丢失内存进度，仍存在的信号可能重新执行；不承诺 exactly-once 或跨进程去重。
- Callback panic 转为失败；超时或取消不能当作成功。

### 默认策略

- Callback 执行期限默认 30 秒，允许按注册名字覆盖。
- 失败退避从 1 秒指数增加到 30 秒，带抖动；有效 hook 持续重试，不自动放行。
- 启动总预算默认 5 分钟；退出与 end 共用默认 30 秒预算，均通过 Option/CLI 配置。
- 程序开始执行启动动作后发生启动失败，尝试 end 清理；配置校验失败不调用任何 Callback。

### 取消与退出

- source 发现执行依据失效时取消旧 Callback；必须等它返回，才能调用下一个。
- SIGTERM/SIGINT 或 Run context 取消均启动退出流程；end 使用独立、有期限的清理 context，不能继承已取消状态。
- Callback 不响应取消时，最多等待退出预算；仍不返回则 Runtime 报致命错误，不并发执行下一项或 end，CLI 退出。
- Go 库无法杀死用户 goroutine；嵌入方必须理解，Run 返回错误不等于已终止不合作的业务代码。
- end 不等于 PreDelete，也不覆盖 SIGKILL、节点丢失或容器强制终止。

## 5. SignalSource 是协议适配，不是业务执行

核心只定义一个小的 source 契约：
- `Run`：持续观察，发送可合并的变化通知。
- `Snapshot`：返回当前有效信号及 source 自己的观察凭据。
- `Ack`：基于原观察凭据确认完成，区分已确认、已失效和临时错误。

观察凭据只用于关联当前 source 中的一次观察，不是持久化业务操作 ID。它不传给 Callback，也不要求核心解析。

首版一个 Runtime 最多一个 source；源内部负责协议校验和安全写回，Runtime 负责绑定选择与执行策略。测试使用内存 fake source，生产首个实现为 `source/rbg`；暂不实现通用事件总线或远程触发 API。

## 6. RBG 适配层

### 观察

- 使用 `client-go` dynamic client 和限定单 RI 的 informer；不将 controller-runtime manager 作为整个 runtime 的基座。
- namespace/name 必填，LIST/WATCH 带 `metadata.name` field selector；重连重新同步。
- informer 用于唤醒，执行依据和确认前校验通过直接 GET 获取。
- 不 watch RBG/RIS/Pod，不以 Pod 镜像为权威，不接入旧 RIS lifecycle。
- GVR、wire names、快照读取和所有协议分支集中在 `source/rbg`；核心 API 不 import Kubernetes/RBG 类型。

可选的文件唤醒模式：controller 本来就会把 `signalLabels` 的 key 从 RI 镜像到成员 Pod；Pod 侧用 downwardAPI 把 `metadata.labels` 投影为文件后，source 可以轮询该文件作为唤醒机制，替代 informer。约束与取舍：

- 只替换唤醒。执行依据（Snapshot）和确认（Ack）仍通过直接 GET / PATCH 访问 RI，协议校验不变。
- hook-state 与 `spec.lifecycle` 不镜像到 Pod，因此文件内容不能作为执行依据，只能作为变更提示。
- 收益是 executor 不再对 API server 维持 LIST/WATCH 连接（N 个 executor 不再新增 N 条 watch），RBAC 收窄为 get/patch。
- 唤醒延迟受 RI→Pod 镜像与 kubelet 文件更新的最终一致传播影响；由于执行前总是重新 GET，不影响正确性。

### 触发条件

只有以下条件同时成立才提供可执行 signal：
1. hook-state 是 PreUpdate、PreRecover、PostReady 或 PreDelete。
2. 当前 hook 的 RI `spec.lifecycle` 快照声明该 signal key/value。
3. RI label 存在且值匹配；label 缺失与空 value 必须区分。

Runtime 再按 events.yaml 的精确 signal 绑定选择 Callback。

Updating、Recovering、Terminating 是合法但不可执行的状态；无 state 不执行。未知状态、活跃 hook 快照错误或同一 key 跨 hook 重复声明时 fail-closed 并提供诊断。

### 确认条件

- 重新 GET，核对原 RI UID、原 hook、当前声明与 signal key/value。
- 对象替换、hook 改变或信号失效时不写；不允许依据旧 Callback 结果清除新信号。
- 用编码器构造带最新 resourceVersion 的 Merge Patch，只将自己负责的 label 置为 null；409 后重新读取并完整校验。
- 不修改 state、status、spec、marker-key annotation、finalizer 或 Pod；无 hook-state 时不执行残留清理。
- 保留失败 signal，不自动跳过；其他未绑定 signal 原样保留。

KEP 没有 hook 操作代次，因此相同 UID、hook、key/value 的重复触发仍可能发生 ABA。上述检查不能替代业务幂等和外部副作用 fencing，不承诺协议之外的隔离保证。

### 部署和可用性

- 每个目标及其负责的 signal 只有一个逻辑执行者；不让多个 Pod 分别执行后争抢删除共享 label。
- PreDelete 按 KEP 使用独立于业务 Pod 的 executor；故障后的 PreRecover 也不能依赖已经不可用的业务 Pod。
- 单 RI 独立部署示例采用单副本、Recreate；这不等于分布式执行互斥保证。
- RBAC 仅提供 RI get/list/watch/patch，结合 namespace、resourceNames 和相匹配的 field selector 收窄范围；使用文件唤醒模式时收窄为 get/patch。原生 RBAC 不能限制 PATCH 的 label 字段。
- PostReady 不门控 RI Ready，runtime 健康状态也不代表业务动作已完成。

### 上游草案限制

PR #10 和 KEP #546 都是文档提案；已检查的本地 RI 类型尚无 `spec.lifecycle`。不通过 replace 指向文档分支假装获得实现。

先使用明确标注的测试 CRD fixture 验证协议适配，RBG source 标记为实验性。dynamic client 仅解决编译依赖，不解决集群能力缺失。真实端到端验收必须等具备该协议的 CRD/controller，并固定版本验证。

## 7. 仓库组织与依赖

module 使用当前仓库地址 `github.com/diw-zw/inference-engine-runtime-go`。仓库根只保留模块与仓库级资产；对外可导入的公共代码统一收进 `pkg/`，仓库私有实现收进 `internal/`，命令收进 `cmd/`。产品定位是推理服务的 engine-runtime（哨兵/包装器），`pkg/runtime/` 是这个运行时的伞包，`engine` 一词让给推理引擎本身（SGLang/vLLM 等）：

```text
pkg/runtime/                   运行时伞包：生命周期、执行、重试、取消、SignalSource 契约与 NewDefaultRegistry
pkg/runtime/callback/          Callback 契约与实例级注册
pkg/runtime/event/             绑定模型与严格 YAML 加载
pkg/runtime/plugin/noop/       第一个官方内置插件（编译期注册）
pkg/runtime/source/rbg/        RI 观察、匹配和确认（实验性）
cmd/patio/                     标准进程入口、flag/env 装配
internal/diagnostics/           日志、指标与健康端点（仓库私有）
examples/custom-runtime/       用户 Go Callback 的组装示例
examples/events.yaml           纯事件绑定示例
deploy/                        镜像运行与 RI 权限示例
test/integration/              envtest 和测试 CRD fixture
```

依赖方向保持无环：`pkg/runtime/plugin/noop → pkg/runtime/callback ← pkg/runtime`，`pkg/runtime → pkg/runtime/event`，`pkg/runtime/source/rbg → pkg/runtime`。单元测试与被测代码同目录；首版不拆多个 module。`pkg/runtime/plugin/` 仅表示“编译进二进制、按 AddToRegistry 约定组装的官方内置动作”，不是 Go 的动态 plugin 加载。

- Go 基线 1.25；YAML v3 严格解码；结构化日志采用 slog。
- RI 层使用 client-go/apimachinery v0.34.2；envtest 采用匹配的 controller-runtime v0.22.1，仅作为测试设施。
- `/healthz` 反映进程存活；`/readyz` 反映初始化、首次 source 同步及 start 已完成且未退出，不检查引擎。
- Prometheus 指标覆盖 Callback 次数/耗时、重试、确认结果、pending、source 错误；不使用 RI UID、任意 signal value 等高基数 label。
- 多阶段非 root 镜像；CI 执行格式检查、go vet、go test -race、构建与独立 envtest job。
- 更新现有 README 的定位和最小示例，不新增业务服务或额外运行依赖。

## 8. 实施顺序与验收

### A. 可嵌入的事件核心

实现 Callback、Registry、默认内置集合与组合注册、Events、Runtime、重试与退出，使用 fake source 验证。

验收：一个普通 Go 程序可以注册函数并执行 start/hook/end，不依赖 Kubernetes、Python、HTTP 服务或 GPU。

### B. 独立进程交付

实现 cmd/patio、严格配置校验、默认注册表装配、诊断端点、镜像和“内置 + 用户 Callback”的组合示例。

验收：使用者可在独立 Go 项目中 import 本库、注册 Callback、构建二进制；只修改 events.yaml 即可改变已注册动作的绑定与顺序。

### C. 实验性 RI Source

实现单 RI watch、协议匹配、过期取消和受条件保护的确认；通过 envtest 验证真实 API server 的 PATCH 冲突和 watch 收敛。

验收：模拟 controller 写信号后，调用本地 Go Callback，成功且原信号有效时只清除自己的 label。测试 fixture 通过不等于真实 RBG 集成通过。

### D. 真实 RBG 验证

上游实现可用后固定版本，验证更新、恢复、PostReady、独立 executor 和 background 删除链路；通过后才声明版本兼容，不阻塞 A/B 交付。

### 必测场景

- 空注册表与默认注册表；默认内置可查找但未绑定不执行；两次默认构造互不污染。
- 用户追加和包级 AddToRegistry；内置/用户名字冲突拒绝；标准内置批量注册失败无部分写入；Runtime 快照不受后续注册影响。
- 重名、nil、未知 Callback、重复 YAML key、重复 signal、空列表、空 label value。
- start/end 成功项不重复执行；hook 按配置顺序执行，失败不越过，通知不绕过退避。
- Callback 成功但 Ack 失败；重启重复执行；panic、超时、忽略 context。
- SIGTERM 发生在启动、执行、确认或退避期间；end context 有效且不与旧动作并行。
- 七种合法 hook-state、未知状态、快照缺失、跨 hook 重复 key。
- signal 改值或移除、RI 同名重建、UID/hook 变化、GET/PATCH 冲突、watch 重连。
- 无论何种失败，都不修改 controller 所有字段或清除其他消费者的信号。

## 参考

- [PR #10：事件处理与 Callback](https://github.com/rolebasedgroup/inference-engine-runtime/pull/10)：采用事件模型和绑定方式，不迁移引擎专属能力。
- [旧 hook-operator 设计](file:///Users/wangdi/github/diw-zw/rbg-hook-operator/docs/design.md)：保留代码、绑定、运行配置分离及安全写回经验，不沿用 RI 专属 Callback API。
- [KEP #546](https://github.com/sgl-project/rbg/pull/546)：仅约束 RBG source，协议实现和版本兼容需要单独验收。
