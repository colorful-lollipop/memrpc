# MemRPC 与 Virus Executor Service 架构说明

本文档以当前主线 C++ 实现为准，目标不是重复接口注释，而是把下面三件事讲清楚：

1. `memrpc` 这套共享内存 RPC 框架在代码里是怎么工作的。
2. `virus_executor_service` 如何把业务 handler、控制面和 `memrpc` 串起来。
3. 阅读和修改这些 C++ 文件时，应该先抓哪些类、线程和状态。

如果只想先看测试入口，再配合本文回看源码，请继续看 [testing_guide.md](testing_guide.md)。

## 1. 先建立整体模型

当前主线可以概括为一条非常明确的链路：

- `VesClient` 是业务侧入口，负责把 typed C++ 请求转成 `memrpc` 调用。
- `RpcClient` 是共享内存 RPC 客户端，负责 session、pending 请求、恢复和 response 分发。
- `Session` 是真正贴着共享内存布局工作的底层对象，负责 ring push/pop、共享内存 attach、eventfd 配合。
- `RpcServer` 是服务端调度器，负责从 request ring 取请求、投递到 worker、把结果写回 response ring。
- `EngineSessionService` 把 VPS 的业务 handler 注册到 `RpcServer`，同时保留 `AnyCall` 这条控制面旁路。
- `VirusExecutorService` 是系统能力壳层，负责 `OpenSession`、`Heartbeat`、`AnyCall` 和生命周期管理。

对应代码主入口：

- `memrpc/src/core/session.cpp`
- `memrpc/src/client/rpc_client.cpp`
- `memrpc/src/server/rpc_server.cpp`
- `virus_executor_service/src/client/ves_client.cpp`
- `virus_executor_service/src/service/virus_executor_service.cpp`
- `virus_executor_service/src/service/ves_session_service.cpp`
- `virus_executor_service/src/service/ves_engine_service.cpp`

## 2. 代码目录怎么读

### 2.1 `memrpc/` 的职责

`memrpc` 是基础设施层，不懂扫描业务，只懂“共享内存里有请求和响应 ring”。

- `include/memrpc/core/`
  定义协议、基础类型、bootstrap 抽象、codec、task executor。
- `include/memrpc/client/`
  定义 `RpcClient`、typed invoker、typed future、bootstrap client 封装。
- `include/memrpc/server/`
  定义 `RpcServer` 和 handler 接口。
- `src/core/`
  真正的共享内存布局、`Session`、日志、字节编解码实现。
- `src/client/`
  `RpcClient` 的线程、状态机、恢复逻辑。
- `src/server/`
  `RpcServer` 的 dispatcher、worker 投递、response writer。
- `src/bootstrap/`
  `DevBootstrapChannel` 的实现，服务于本地开发和测试。
- `virus_executor_service/src/transport/`
  `VesBootstrapChannel` / control proxy 的实现，负责病毒执行服务运行时的控制面代理。

### 2.1.1 可以把 `memrpc` 再拆成四层

如果你要更快读懂 `memrpc`，不要把它当成一个单体目录，而要按代码职责拆开：

- 协议层
  以 [`memrpc/include/memrpc/core/protocol.h`](/root/code/demo/mem/memrpc/include/memrpc/core/protocol.h) 为中心，定义 entry 布局、payload 上限和共享内存双方必须一致的 ABI。
- session 层
  以 [`memrpc/src/core/session.cpp`](/root/code/demo/mem/memrpc/src/core/session.cpp) 为中心，负责 mmap、ring cursor、eventfd 配合和 client attach。
- client/server 运行时层
  以 [`memrpc/src/client/rpc_client.cpp`](/root/code/demo/mem/memrpc/src/client/rpc_client.cpp) 和 [`memrpc/src/server/rpc_server.cpp`](/root/code/demo/mem/memrpc/src/server/rpc_server.cpp) 为中心，负责请求生命周期、调度、超时、恢复、回写。
- typed 适配层
  以 [`memrpc/include/memrpc/client/rpc_client.h`](/root/code/demo/mem/memrpc/include/memrpc/client/rpc_client.h) 里的 `WaitAndDecode()` 和业务层的 typed 注册辅助代码为中心，把业务结构体和底层字节 payload 接起来。

这四层的依赖方向也很清楚：

- 协议层是最底层，规定数据长什么样。
- session 层依赖协议层，但不知道业务类型。
- `RpcClient` / `RpcServer` 依赖 session 层，负责把“共享内存里的 entry”变成一次完整 RPC。
- typed 层只是一层薄封装，帮业务代码避免手写 encode/decode。

对 C++ 阅读者来说，这个拆法很有用，因为你通常可以先判断自己正在改哪一层，再决定要不要继续往上看。比如：

- 改 ring、大 payload、共享内存 attach，先看协议层和 session 层。
- 改 timeout、pending、恢复、event 分发，先看 `RpcClient`。
- 改 handler 调度、高低优先级、response backlog，先看 `RpcServer`。
- 改 typed API，先看 `rpc_client.h` 里的 `WaitAndDecode()` 和业务 facade，而不是先钻到共享内存底层。

### 2.2 `virus_executor_service/` 的职责

VPS 是业务层和集成层，知道 opcode 含义、扫描业务、控制面接口和测试入口。

- `include/ves/`
  业务协议、typed 请求/响应、业务 service 头文件。
- `include/client/`
  `VesClient` 对外 API。
- `include/service/`
  系统能力壳层。
- `include/transport/`
  控制面 transport、注册中心、代理/桩对象。
- `include/testkit/`
  为测试提供的轻量服务与客户端。
- `src/service/`
  `VirusExecutorService`、`EngineSessionService`、`VesEngineService`。
- `src/client/`
  `VesClient`，把 typed API 映射到 `RpcClient`。
- `src/testkit/`
  testkit handler 和 test client。
- `src/transport/`
  控制面代理、注册中心、registry backend。
- `src/app/`
  supervisor、client、stress、DT 等可执行程序入口。

## 3. 共享内存协议长什么样

先看 [`memrpc/include/memrpc/core/protocol.h`](/root/code/demo/mem/memrpc/include/memrpc/core/protocol.h)。

这里定义了当前所有上层代码都必须遵守的事实：

- `PROTOCOL_VERSION = 8`
- `RING_ENTRY_BYTES = 8192`
- 请求和响应都使用固定大小 entry
- entry 头部字段固定，payload 直接 inline 在 entry 里

核心结构有两个：

- `RequestRingEntry`
  包含 `requestId`、`execTimeoutMs`、`opcode`、`priority` 和请求 payload。
- `ResponseRingEntry`
  包含 `requestId`、框架状态、业务错误码、事件字段和响应 payload。

这说明当前主线不是“共享内存里再引用额外 slot/pool”，而是：

- 小请求直接写进 request entry
- 小响应直接写进 response entry
- 超过 inline 限制就不进入 `memrpc` 主路径

这套设计的直接结果是：

- 协议简单，`Session` 代码也更直接。
- payload 上限是硬边界，不是建议值。
- 框架不会做复杂的大包分段、slot 生命周期回收或“执行后自动旁路回拉”。

## 4. `Session` 是真正的底层核心

看 [`memrpc/src/core/session.cpp`](/root/code/demo/mem/memrpc/src/core/session.cpp) 和 [`memrpc/src/core/session.h`](/root/code/demo/mem/memrpc/src/core/session.h)。

### 4.1 `Session` 负责什么

`Session` 的职责非常集中：

- attach 共享内存和相关 fd
- 校验协议版本和布局
- 解析三条 ring 的 cursor 和 entry 区域
- 提供 `PushRequest` / `PopRequest` / `PushResponse` / `PopResponse`
- 处理 client 附着状态
- 在共享内存对象或 owner 死亡时给上层返回明确错误

它不做的事也同样重要：

- 不负责编码 typed 对象
- 不维护 pending future
- 不做自动恢复
- 不做 handler 调度

### 4.2 `Attach()` 的真实流程

`Session::Attach()` 的关键步骤：

1. `Reset()` 清理旧映射和 fd。
2. `MapAndValidateHeader()` 先只 mmap 头部，验证 `magic` 和 `protocolVersion`。
3. `RemapWithActualLayout()` 读取 header 中的 ring 大小和 payload 大小，重新按完整布局 mmap。
4. 如果是 client 角色，再走 `TryAcquireClientAttachment()`，确保当前 session 同一时刻只有一个活跃 client 挂接。

这里的实现细节很值得注意：

- 它先映射头部，再按真实布局 remap，而不是一开始就盲目映射整块区域。
- `TryAcquireClientAttachment()` 会检查 `activeClientPid` 是否还活着，避免共享内存里遗留陈旧占用。
- `pthread_mutex_timedlock` 和 owner-dead 处理让共享内存里的互斥锁在对端异常退出后仍然能给出“对端断开”语义。

### 4.3 ring push/pop 怎么实现

`PushRingEntry()` 和 `PopRingEntry()` 是最底层的数据面逻辑：

- `tail - head >= capacity` 判满
- `tail == head` 判空
- 按 `index % capacity` 访问固定大小 entry
- 用原子 `head` / `tail` 配合 acquire/release 内存序

这也是为什么上层文档经常强调“固定 entry 模型”：

- 它让 ring 的逻辑近似于 lock-free 单元复制
- 复杂度被留在 session 建立、超时和恢复，而不是留在 payload 生命周期

## 5. `RpcClient` 的实现重点

看 [`memrpc/src/client/rpc_client.cpp`](/root/code/demo/mem/memrpc/src/client/rpc_client.cpp) 和 [`memrpc/include/memrpc/client/rpc_client.h`](/root/code/demo/mem/memrpc/include/memrpc/client/rpc_client.h)。

`RpcClient` 是整个系统最值得花时间读的类。因为它同时承担：

- session 生命周期所有权
- 请求发布
- pending future 管理
- response 分发
- 超时 watchdog
- idle close
- engine death / health check 驱动的恢复状态机

### 5.1 `RpcCall` / `RpcReply` 设计意图

`RpcCall` 里最重要的 timeout 只有 `execTimeoutMs`：

- `execTimeoutMs`
  从请求发布成功开始，到拿到最终 reply 为止的超时。

请求发布前的 admission 等待、以及服务端排队阶段，现在都固定为无限等待；如果调用方需要自己控制等待预算，应在外层使用 `RpcFuture::WaitFor(...)`。`execTimeoutMs` 决定的是“客户端认为这次调用应如何结束”，而不是“服务端一定会被取消”。例如 `execTimeoutMs` 超时后，服务端执行可能还在继续，迟到响应会被客户端丢弃。

### 5.2 `RpcFuture` 的消费语义

`RpcFuture` 是 `RpcClient` 暴露给调用方的等待对象，只支持等待式消费：

- `Wait()` / `WaitAndTake()` / `WaitFor()`

### 5.3 `RpcClient` 内部真正维护的对象

`RpcClient::Impl` 里几组内部结构可以帮助快速理解代码：

- `SessionSnapshot`
  当前 session 的 fd 和状态快照。
- `PendingSubmit`
  还未成功进入 ring 的待提交请求。
- `PendingRequest`
  已经获得 `requestId` 并进入跟踪表，等待 response 的请求。
- `RecoveryRuntimeSnapshot`
  当前恢复状态对外可见快照。

从阅读顺序上，建议优先找下面几类函数：

- session 打开/关闭相关函数
- 请求发布和 pending 表管理函数
- response loop / watchdog loop
- recovery 状态迁移函数

### 5.4 为什么 `RpcClient` 是生命周期唯一所有者

头文件里已经把状态写得很清楚：

- `Uninitialized`
- `Active`
- `Disconnected`
- `Cooldown`
- `IdleClosed`
- `Recovering`
- `Closed`

这不是为了“状态机好看”，而是为了把过去容易散落在业务层的恢复逻辑统一收回框架层。

现在的边界是：

- `RpcClient` 决定 session 何时可用、何时失效、何时可重连。
- 业务层最多只提供策略，即 `RecoveryPolicy`。
- `VesClient` 不再自己维护另一套 engine-dead 布尔值或 restart 循环。

这让测试也更聚焦：测试恢复时，应该优先断言 `RpcClient` 的 `RecoveryRuntimeSnapshot` 和 `RecoveryEventReport`，而不是去猜业务 facade 内部状态。

### 5.5 一次正常调用在客户端怎么走

以 `InvokeAsync()` 为主线，可以把客户端流程理解为：

1. 调用方构造 `RpcCall`。
2. `RpcClient` 检查自身生命周期状态。
3. 如果需要，会先通过 bootstrap 打开 session。
4. 为请求分配 `requestId`，建立 `PendingRequest`。
5. 把 `RequestRingEntry` 写入 high/normal request ring。
6. 通过 request eventfd 唤醒服务端。
7. response 线程从 response ring 读到 `ResponseRingEntry`。
8. 按 `requestId` 找到 pending future，设置 reply 并唤醒等待者。

如果第 5 步 ring 满：

- 客户端不会忙等死循环
- 会结合 admission timeout 和 request credit eventfd 做等待

如果第 7 步之前 exec timeout 到达：

- 对应 future 先以 `ExecTimeout` 完成
- 真正迟到的响应会被识别为过期并丢弃

### 5.6 恢复逻辑和健康检查怎么接到一起

`RpcClient` 的恢复来源主要有三类：

- 调用失败，例如 `ExecTimeout`
- bootstrap 报告 `EngineDeath`
- bootstrap 的 `CheckHealth()` 返回 timeout / unhealthy / session mismatch

实现上有几个关键判断：

- `ReplayHintForStatus()`
  把错误映射成“可不可以安全重放”的提示，而不是自动重放。
- `RecoveryTriggerForStatus()`
  把失败原因映射到恢复状态机触发源。
- `ShouldRetryRecoveryStatus()`
  判断某个状态是否意味着当前调用应该在恢复窗口内继续等。

这里的设计重点是保守：

- 框架会尝试恢复 session
- 但不会擅自重放一个可能已经被旧服务端看到的业务请求

## 6. `RpcServer` 的实现重点

看 [`memrpc/src/server/rpc_server.cpp`](/root/code/demo/mem/memrpc/src/server/rpc_server.cpp) 和 [`memrpc/include/memrpc/server/rpc_server.h`](/root/code/demo/mem/memrpc/include/memrpc/server/rpc_server.h)。

### 6.1 `RpcServer` 负责什么

`RpcServer` 的职责是：

- attach server 侧 session
- 从请求 ring 拉取请求
- 按优先级投递给 worker
- 收集 handler 结果
- 把 reply / event 写回 response ring

`RpcServer` 不负责：

- 创建共享内存资源
- 解释业务 opcode 语义
- 管理上层恢复策略

### 6.2 为什么有两个 executor

`ServerOptions` 里有：

- `highWorkerThreads`
- `normalWorkerThreads`
- `highExecutor`
- `normalExecutor`

对应当前架构里的一个明确目标：

- 高优先级请求和普通请求分流
- 普通请求阻塞时，不拖慢高优请求

默认实现是 `ThreadPoolExecutor`，它本质上是一个简单、明确、可预测的线程池：

- 有界队列
- 无法提交时返回失败
- 可等待容量恢复

这与 `RpcClient` 一样，也是“保守控制背压”，而不是“无限排队”。

### 6.3 服务端完整调用链

一条请求在服务端的路径大致是：

1. dispatcher 线程轮询 request eventfd 或直接检查 ring。
2. 优先消费 high ring，再消费 normal ring。
3. 把 `RequestRingEntry` 转成 `RpcServerCall`。
4. 找到对应 `RpcHandler`。
5. 投递到 high/normal executor。
6. worker 执行 handler，得到 `RpcServerReply`。
7. 构造 `ResponseRingEntry` 放入 completion queue。
8. response writer 线程把结果写回 response ring。
9. 若 response ring 满，则等待 response credit eventfd。

这条链路里有两个单独线程非常关键：

- dispatcher thread
- response writer thread

它们把“取请求”和“写响应”拆开，可以避免 handler 执行线程直接阻塞在 ring credit 上。

### 6.4 `PublishEvent()` 为什么复用 response ring

当前实现里，事件不是单独搞第四条 ring，而是复用 response ring，靠 `ResponseMessageKind::Event` 区分。

这样做的好处：

- 协议面更小
- 客户端响应分发线程天然已经在消费这个 ring
- 事件和响应走统一背压机制

代价是：

- 事件和响应共享 response 带宽
- 事件过多时会与响应竞争 credit

当前主线接受这个权衡，因为它更符合“共享内存主路径保持简单”的目标。

## 7. bootstrap 层做什么

看 [`memrpc/include/memrpc/core/bootstrap.h`](/root/code/demo/mem/memrpc/include/memrpc/core/bootstrap.h) 和 `memrpc/src/bootstrap/*`。

`IBootstrapChannel` 只有四类职责：

- `OpenSession()`
- `CloseSession()`
- `CheckHealth()`
- `SetEngineDeathCallback()`

也就是说，bootstrap 层不是传业务请求的地方，它只是：

- 帮 client 拿到 session 入口句柄
- 给 client 一个通用健康判断
- 把“子进程/远端死亡”通知回 `RpcClient`

测试里常见的 `DevBootstrapChannel`，本质上是本地开发和测试环境下的 bootstrap 实现；Virus Executor Service 运行时通过 `VesBootstrapChannel` 直接调用控制面，framework 里不再保留 `SaBootstrapChannel` 替代。

## 8. VPS 侧是怎么把业务接进来的

### 8.1 `VirusExecutorService`：系统能力壳层

看 [`virus_executor_service/src/service/virus_executor_service.cpp`](/root/code/demo/mem/virus_executor_service/src/service/virus_executor_service.cpp)。

这个类同时继承：

- `OHOS::SystemAbility`
- `VesControlStub`

所以它天然是控制面的实现者。它提供四个关键入口：

- `OpenSession()`
- `CloseSession()`
- `Heartbeat()`
- `AnyCall()`

这里最重要的不是业务复杂度，而是分层清楚：

- `OpenSession()` 先让 `VesEngineService::ConfigureEngines()` 校验业务配置，再把工作交给 `EngineSessionService`。
- `Heartbeat()` 不触碰共享内存请求链路，它读取运行态统计，对外输出一个健康快照。
- `AnyCall()` 是明确保留的控制面旁路，直接调用 `EngineSessionService::InvokeAnyCall()`。

### 8.2 `VesEngineService`：纯业务 handler 提供者

看 [`virus_executor_service/src/service/ves_engine_service.cpp`](/root/code/demo/mem/virus_executor_service/src/service/ves_engine_service.cpp)。

它的职责很干净：

- 保存当前 engine 配置
- 提供业务方法，如 `ScanFile()`
- 注册 typed handler
- 需要时通过 `VesEventPublisher` 发布业务事件

`RegisterHandlers()` 是它和 `memrpc` 对接的关键点。VES 业务 handler 通过 `RegisterVesTypedHandler()` 统一处理普通内联 payload 或大请求 file payload envelope，再调用 typed C++ 业务函数。
这意味着业务代码平时写的是普通 C++ 结构和函数，而不是手动拼装字节流；file payload 只是 VES transport 适配层细节。

`ScanFile()` 当前是样例业务实现：

- 根据 path 评估样例规则
- 可触发 sleep 或 crash
- 返回 threat level

它非常适合文档和测试，因为它把“正常返回、长耗时、故障注入”都集中在一处。

### 8.3 `EngineSessionService`：VPS 和 `RpcServer` 的连接器

看 [`virus_executor_service/src/service/ves_session_service.cpp`](/root/code/demo/mem/virus_executor_service/src/service/ves_session_service.cpp)。

这是 VPS 侧最关键的桥接类。它干了三件事：

1. 惰性初始化 bootstrap 和 `RpcServer`
2. 把所有 registrar 的 handler 注册到 `RpcServer`
3. 维护 `AnyCall` 映射和事件发布线程

`EnsureInitialized()` 的流程值得重点看：

1. 若没有 bootstrap，则构造 `DevBootstrapChannel`
2. 先 `OpenSession()` 一次，确保底层资源已建立
3. 取 `serverHandles()` 构造 `RpcServer`
4. 遍历 `registrars_`
5. 同时向两个 sink 注册 handler
   - `RpcServerHandlerSink`
   - `AnyCallHandlerSinkImpl`
6. `rpcServer_->Start()`

这里体现了当前 VPS 架构的一个核心设计：

- 共享内存主路径和 `AnyCall` 旁路共用同一套 opcode/handler 语义
- 业务 handler 不需要写两份

也就是说，业务层不需要关心“我现在是 MemRPC 还是控制面”，它只需要关心：

- 这个 opcode 的 typed request / reply 是什么
- handler 怎么实现

### 8.4 `EventPublisherLoop()` 的意义

`EngineSessionService` 里还有一个后台线程 `EventPublisherLoop()`。它周期性构造事件并调用 `PublishEventBlocking()`。

这个线程的意义不是“业务必须随机发事件”，而是：

- 提供事件通路的真实运行样本
- 让测试验证客户端事件消费、队列竞争和断连场景

## 9. `VesClient` 的代码应该怎么理解

看 [`virus_executor_service/src/client/ves_client.cpp`](/root/code/demo/mem/virus_executor_service/src/client/ves_client.cpp)。

### 9.1 `VesClient` 不是第二个 `RpcClient`

`VesClient` 自己不重新实现 RPC，它只是：

- 持有一个 `RpcClient`
- 持有 `VesBootstrapChannel`
- 构造 VPS 需要的 `RecoveryPolicy`
- 提供 typed API，比如 `ScanFile()`
- 必要时回退到控制面的 `AnyCall`

因此读这段代码时，重点不是“它做了多少事”，而是“它没有越界去做哪些事”：

- 不自己维护恢复线程
- 不自己持有共享内存
- 不重新实现 future/pending

### 9.1.1 `VesClient` 在 VPS 里到底扮演什么角色

`VesClient` 最容易被误读成“业务客户端实现”。更准确地说，它是一个面向业务调用者的组合型 facade：

- 向上，它暴露 typed C++ API，例如 `ScanFile`。
- 向下，它同时管理 `RpcClient` 和控制面 proxy。
- 在中间，它负责决定当前请求应该走共享内存主路径还是 `AnyCall` 旁路。

所以 `VesClient` 的核心价值不在于“自己实现了一套 RPC”，而在于把三类能力收敛到一个调用口：

- typed codec
- recovery policy 装配
- transport 选择

如果你在代码里看到 `VesClient` 变得越来越像第二个 `RpcClient`，通常就说明分层开始跑偏了。

### 9.2 `Init()` 做了什么

`VesClient::Init()` 的流程：

1. 构造 `VesBootstrapChannel`
2. 把 channel 注入 `client_`
3. 安装健康快照回调
4. 构造并设置 `RecoveryPolicy`
5. 调用 `client_.Init()`
6. 让 `VesBootstrapChannel` 建立并持有当前 control

因此当 `VesClient` 初始化完成时，实际上已完成两层准备：

- 控制面可用
- 共享内存 session 已由 `RpcClient` 打开

### 9.3 typed API 如何决定走哪条路

`VesClient::InvokeApi()` 是理解客户端行为的关键。

它的思路是：

1. 先把 typed request 编码成字节数组。
2. 如果编码失败，直接返回错误。
3. 通过 `client_.RetryUntilRecoverySettles()` 包住一次调用，复用 `RpcClient` 的恢复等待逻辑。
4. 小请求走 `memrpc` typed invoke。
5. 如果请求大于 `DEFAULT_MAX_REQUEST_BYTES`，则调用控制面的 `AnyCall()`。

这就是当前文档里一直强调的那条架构边界：

- 小包走共享内存主路径
- 超大请求走同步 `AnyCall`
- 不做“执行完发现响应太大再切控制面”的复杂补救

### 9.3.1 `InvokeApi()` 里实际发生了哪些代码动作

`InvokeApi()` 是理解 `VesClient` 的最好切入口，可以按源码顺序把它拆成下面几步：

1. 检查 `reply` 指针是否合法。
2. 在 `client_.RetryUntilRecoverySettles()` 提供的统一恢复语义里执行真正调用。
3. 调用 `MemRpc::EncodeMessage<Request>()` 把 typed 请求编码成 `std::vector<uint8_t>`。
4. 判断 payload 是否超过 `DEFAULT_MAX_REQUEST_BYTES`。
5. 若不超限，则走 `MemRpc::InvokeTypedSync` 或等价的 `RpcClient` 主路径。
6. 若超限，则构造 `VesAnyCallRequest`，通过控制面 `AnyCall()` 完成调用。
7. 成功后再把 reply payload decode 回 typed C++ 对象。

这里有两个实现上的关键点：

- `VesClient` 判断的是“请求是否适合进入 `memrpc`”，而不是“任何失败都自动切控制面”。
- `AnyCall` 旁路复用相同 opcode 语义，所以业务 handler 不需要因为 transport 不同写两套实现。

这就是为什么 `VesClient` 看起来代码量不大，但实际上是整个 VPS 客户端侧分层边界最关键的地方。

### 9.4 `CurrentControl()` 为什么要缓存 fallback control

`CurrentControl()` 只从 `bootstrapChannel_` 取当前 control；channel 不存在时直接返回空。

这反映的设计是：

- 控制面的 source of truth 由 `VesBootstrapChannel` 统一维护
- engine death / control 失效后，由 channel 在下一次显式需要时按需刷新

### 9.4.1 `Connect()`、`BuildControlLoader()` 和 `CurrentControl()` 该怎么一起看

如果要真正读透 `VesClient`，建议把这三个点放在一起看：

- `Connect()`
  负责给调用者一个“拿来就能初始化”的入口。它内部会创建 `VesClient`，再调用 `Init()`。
- `BuildControlLoader()`
  负责把“如何拿到系统能力 remote 对象”封装成一个可重试的 loader。它统一通过 SA manager 加载 control remote，并在需要时重新加载。
- `CurrentControl()`
  负责在运行时获取当前可用的 control proxy；它只信任 `bootstrapChannel_`，不会在 facade 层自行重建 control。

把这三者连起来看，就能明白 `VesClient` 处理控制面的思路不是“每次临时现连”，而是：

- 初始化阶段建立一个尽量稳定的 control 获取机制。
- 运行阶段只通过 bootstrap channel 获取当前 control。
- channel 生命周期结束后，`VesClient` 不再提供 control。

这套设计和 `RpcClient` 的 session 恢复策略是互补的：

- `RpcClient` 负责共享内存 session 生命周期。
- `VesClient` 负责业务 facade 和控制面取用点。

### 9.4.2 一个 `VesClient::ScanFile()` 的最小阅读路径

如果你只想最快读懂 `VesClient`，建议按下面顺序打开源码：

1. [`virus_executor_service/include/client/ves_client.h`](/root/code/demo/mem/virus_executor_service/include/client/ves_client.h)
2. [`virus_executor_service/src/client/ves_client.cpp`](/root/code/demo/mem/virus_executor_service/src/client/ves_client.cpp)
3. [`memrpc/include/memrpc/client/rpc_client.h`](/root/code/demo/mem/memrpc/include/memrpc/client/rpc_client.h)
4. [`virus_executor_service/include/transport/ves_control_interface.h`](/root/code/demo/mem/virus_executor_service/include/transport/ves_control_interface.h)

因为这条阅读链能同时看到：

- typed API 长什么样
- 请求如何被编码
- 共享内存主路径如何发起
- 控制面兜底请求长什么样

## 10. testkit 为什么重要

很多人第一次看仓库时，会把 `testkit` 当成“只是测试辅助代码”。其实它是理解框架的最好切入口。

看：

- [`virus_executor_service/src/testkit/testkit_service.cpp`](/root/code/demo/mem/virus_executor_service/src/testkit/testkit_service.cpp)
- [`virus_executor_service/src/testkit/testkit_async_client.cpp`](/root/code/demo/mem/virus_executor_service/src/testkit/testkit_async_client.cpp)
- [`virus_executor_service/src/testkit/testkit_client.cpp`](/root/code/demo/mem/virus_executor_service/src/testkit/testkit_client.cpp)

### 10.1 `TestkitService`

`TestkitService` 提供三类极简 handler：

- `Echo`
- `Add`
- `Sleep`

再加一组可选 fault injection handler：

- `CrashForTest`
- `HangForTest`
- `OomForTest`
- `StackOverflowForTest`

这组代码的价值在于：

- 它比真实 VPS 业务更简单，便于聚焦 `memrpc` 行为
- 它覆盖了正常、慢请求、崩溃、卡死等关键路径

### 10.2 `TestkitAsyncClient` 和 `TestkitClient`

这两个类展示了两种非常典型的客户端封装方式：

- `TestkitAsyncClient`
  直接返回 `TypedFuture<T>`，展示异步接口如何建立在 `RpcClient` 之上。
- `TestkitClient`
  在 async client 之上再包一层同步 API，展示业务 facade 如何把 future 转为阻塞式 typed 接口。

`VesClient` 的设计其实和 `TestkitClient` 是同一种思路，只是加入了恢复策略和 `AnyCall` 旁路。

## 11. 一次完整的 ScanFile 调用怎么走

下面用 VPS 最典型的 `ScanFile` 路径把整个系统串起来。

### 11.1 客户端侧

1. 调用方执行 `VesClient::ScanFile()`。
2. 内部进入 `InvokeApi()`。
3. `ScanTask` 被编码为字节数组。
4. 若 payload 足够小，交给 `RpcClient`。
5. `RpcClient` 生成 `RequestRingEntry` 并写入 request ring。

### 11.2 服务端侧

6. `RpcServer` dispatcher 从 request ring 取出 entry。
7. 通过 opcode 找到 `VesEngineService::RegisterHandlers()` 注册的 typed handler。
8. worker 线程执行 `VesEngineService::ScanFile()`。
9. typed reply 被编码为 `ResponseRingEntry` payload。
10. response writer 线程把响应写回 response ring。

### 11.3 返回客户端

11. `RpcClient` response loop 读取 `ResponseRingEntry`。
12. 按 `requestId` 找到 pending request。
13. 解码得到 `ScanFileReply`。
14. `VesClient::ScanFile()` 返回 typed 结果。

如果第 3 步编码后超限：

- 不会继续写 request ring
- 直接走 `AnyCall()` 控制面

## 12. 阅读这套 C++ 代码的推荐顺序

如果你是第一次深入这个仓库，建议按下面顺序读：

1. `memrpc/include/memrpc/core/protocol.h`
   先理解 entry、payload 上限和协议边界。
2. `memrpc/src/core/session.cpp`
   再理解共享内存 attach 和 ring 操作。
3. `memrpc/src/server/rpc_server.cpp`
   理解请求是如何被消费和回写的。
4. `memrpc/src/client/rpc_client.cpp`
   理解 pending、future、watchdog、恢复。
5. `virus_executor_service/src/service/ves_engine_service.cpp`
   看业务 handler 是怎么注册的。
6. `virus_executor_service/src/service/ves_session_service.cpp`
   看 VPS 如何把 handler 接到 `RpcServer`。
7. `virus_executor_service/src/client/ves_client.cpp`
   看 typed facade 如何使用 `RpcClient`。
8. `virus_executor_service/src/testkit/*`
   用最简单的业务例子验证你对框架的理解。

## 13. 修改代码时最容易踩的边界

### 13.1 不要让业务层重新发明恢复状态机

恢复、cooldown、idle close、session reopen 都应留在 `RpcClient`。  
业务层应该提供 policy，不应再维护第二套生命周期状态。

### 13.2 不要绕开 `protocol.h` 私自扩大 payload

如果需要更大的请求/响应：

- 要么修改协议和测试矩阵
- 要么明确走控制面旁路

不要在业务层偷偷塞额外共享内存对象或暗中截断 payload。

### 13.3 不要让 handler 直接感知 transport 细节

handler 应只关心 typed request / reply。  
`AnyCall` 和 `memrpc` 的双通路存在意义，就是让业务 handler 不需要写两份 transport 逻辑。

### 13.4 事件和响应共享 response ring

如果要引入大量事件发布，必须同时考虑：

- response ring 容量
- response credit
- 客户端 response loop 的消费速度

否则很容易把“事件增加”误写成“业务 handler 变慢”。

## 14. 和测试文档怎么配合看

本文档解释的是“代码为什么这样设计、调用链怎么走”。  
测试层面的命令、标签、代表性测试和推荐验证矩阵，请继续看 [testing_guide.md](testing_guide.md)。
