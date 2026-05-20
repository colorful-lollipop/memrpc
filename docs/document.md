# Virus Executor Service 代码串讲文档

> 本文档详细梳理了基于共享内存RPC的病毒扫描服务架构，以`scanFile`为例讲解核心流程。

---

## 目录

1. [项目概述](#1-项目概述)
2. [共享内存结构](#2-共享内存结构)
3. [服务拉起流程](#3-服务拉起流程)
4. [scanFile完整调用链](#4-scanfile完整调用链)
5. [异常文件处理流程](#5-异常文件处理流程)
6. [冷却(Cooldown)流程](#6-冷却cooldown流程)
7. [通信流程](#7-通信流程)
8. [自启与恢复流程](#8-自启与恢复流程)

---

## 1. 项目概述

### 1.1 整体架构

```
+-----------------------------------------------------------------------------+
|                              应用层 (Application)                            |
|  +-----------------+    +-----------------+    +-------------------------+  |
|  |   VesClient     |    |   Supervisor    |    | VirusExecutorService    |  |
|  |   (客户端)       |    |   (监管进程)     |    | (引擎服务进程)          |  |
|  +--------+--------+    +--------+--------+    +-------------------------+  |
|           |                      |                                           |
+-----------+----------------------+-------------------------------------------+
|                      MemRpc Framework (共享内存RPC框架)                       |
+-----------+----------------------+-------------------------------------------+
|           |                      |                                           |
|  +--------+-------+    +---------+----------+    +------------------------+ |
|  |   RpcClient    |    |   Shared Memory    |    |       RpcServer        | |
|  |  (客户端线程)   |    |   (共享内存区域)    |    |    (服务端线程)         | |
|  | +------------+ |    |  +-------+-------+ |    | +--------------------+ | |
|  | |SubmitWorker| |    |  |High   |Normal | |    | |DispatcherThread    | | |
|  | | (提交线程)  | |    |  |ReqRing|ReqRing| |    | | (请求分发线程-1个)  | | |
|  | +------------+ |    |  +---+---+---+---+ |    | +---------+----------+ | |
|  | +------------+ |    |      |       |     |    |           |            | |
|  | |ResponseWorker| |   |      v       v     |    |  +--------v--------+   | |
|  | | (响应线程)  | |    |  +---+---+---+---+ |    |  | ThreadPoolExecutor| | |
|  | +------------+ |    |  |RespRing|EventFd| |    |  | (High+Normal线程池)| | |
|  | +------------+ |    |  +---+---+---+---+ |    |  +--------+----------+ | |
|  | |WatchdogThread| |   |      |       |     |    |           |            | |
|  | |(监控线程)   | |    +------+-------+-----+    |  +--------v--------+   | |
|  | +------------+ |                             |  |ResponseWriter    | | |
|  +----------------+                             |  | (响应写入线程-1个)  | | |
|                                                  |  +------------------+ | |
|                                                  +------------------------+ |
+-----------------------------------------------------------------------------+
```

```

### 1.2 核心模块职责

| 模块 | 职责 | 关键文件 |
|------|------|----------|
| **VesClient** | 对外提供`ScanFile`等API，封装RPC调用 | `ves_client.h/cpp` |
| **RpcClient** | memrpc客户端，管理共享内存会话、请求生命周期，以及服务端生命周期（恢复/拉起/关闭） | `rpc_client.h/cpp` |
| **RpcServer** | memrpc服务端，处理请求队列并分发到处理器 | `rpc_server.cpp` |
| **EngineSessionService** | 引擎会话管理，创建RpcServer并注册处理器 | `ves_session_service.cpp` |
| **VesEngineService** | 业务逻辑处理，实现`ScanFile`扫描 | `ves_engine_service.cpp` |
| **Supervisor** | 监管进程，负责启动引擎和客户端进程 | `ves_supervisor_main.cpp` |
| **RegistryServer** | SA注册中心，通过Unix Socket管理服务生命周期 | `registry_server.cpp` |

---

## 2. 共享内存结构

### 2.1 共享内存布局

共享内存是客户端和服务端通信的核心载体，采用**无锁环形队列**设计：


```
+----------------------------------------------------------------------------+
|                         共享内存布局 (Shared Memory Layout)                  |
+----------------------------------------------------------------------------+
|                                                                            |
|  +---------------------------------------------------------------------+   |
|  |                    SharedMemoryHeader (256+ bytes)                   |   |
|  |  +---------------------------------------------------------------+  |   |
|  |  | Magic (4B)        = 0x4d454d52 ("MEMR")                        |  |   |
|  |  | Version (4B)      = 7                                          |  |   |
|  |  | SessionId (8B)    唯一会话标识                                  |  |   |
|  |  | SessionState (4B) Alive(0) / Broken(1)  [atomic]               |  |   |
|  |  | ClientAttached (4B) 客户端是否已连接                            |  |   |
|  |  | ActiveClientPid (4B) 活跃客户端PID                              |  |   |
|  |  +---------------------------------------------------------------+  |   |
|  |  | RingSize 配置:                                                  |  |   |
|  |  |   highRingSize = 8    (高优队列大小)                            |  |   |
|  |  |   normalRingSize = 8  (普通队列大小)                            |  |   |
|  |  |   responseRingSize = 8 (响应队列大小)                           |  |   |
|  |  |   maxRequestBytes = 8168 (最大请求数据)                         |  |   |
|  |  |   maxResponseBytes = 8156 (最大响应数据)                        |  |   |
|  |  +---------------------------------------------------------------+  |   |
|  |  | RingCursor (高优请求环): head(4B) + tail(4B) + capacity(4B)     |  |   |
|  |  | RingCursor (普通请求环): head + tail + capacity                 |  |   |
|  |  | RingCursor (响应环): head + tail + capacity                     |  |   |
|  |  +---------------------------------------------------------------+  |   |
|  |  | pthread_mutex_t clientStateMutex  进程共享robust mutex          |  |   |
|  |  +---------------------------------------------------------------+  |   |
|  +---------------------------------------------------------------------+   |
|                                    |                                       |
|                                    v                                       |
|  +---------------------------------------------------------------------+   |
|  |              High Priority Request Ring (高优先级请求环)              |   |
|  |  +---------+ +---------+ +---------+        +---------+            |   |
|  |  | Entry 0 | | Entry 1 | | Entry 2 |  ...   | Entry 7 |  (8 entries)|   |
|  |  | 8192B   | | 8192B   | | 8192B   |        | 8192B   |            |   |
|  |  +---------+ +---------+ +---------+        +---------+            |   |
|  |  每个Entry = 8192 bytes (RING_ENTRY_BYTES)                          |   |
|  +---------------------------------------------------------------------+   |
|                                    |                                       |
|                                    v                                       |
|  +---------------------------------------------------------------------+   |
|  |              Normal Priority Request Ring (普通优先级请求环)          |   |
|  |  (相同结构，独立的RingCursor，8 entries)                             |   |
|  +---------------------------------------------------------------------+   |
|                                    |                                       |
|                                    v                                       |
|  +---------------------------------------------------------------------+   |
|  |              Response/Event Ring (响应/事件环)                        |   |
|  |  服务端到客户端的单向通信，包含Reply和Event两种消息                    |   |
|  +---------------------------------------------------------------------+   |
|                                                                            |
+----------------------------------------------------------------------------+
```

### 2.2 Ring Entry 详细结构

**RequestRingEntry (8192 bytes)**:
```cpp
struct RequestRingEntry {
    // HEADER_BYTES = 22 bytes
    uint64_t requestId;        // 请求唯一ID
    uint32_t execTimeoutMs;    // 执行超时时间
    uint16_t opcode;           // 操作码 (如 ScanFile=102)
    uint8_t  priority;         // 优先级 (0=Normal, 1=High)
    uint8_t  payloadKind;      // 0=inline payload, 1=file payload ref
    uint32_t payloadSize;      // 有效载荷大小
    uint8_t  reserved0;
    uint8_t  reserved1;
    
    // INLINE_PAYLOAD_BYTES = 8192 - 22 = 8170 bytes
    std::array<uint8_t, 8170> payload;  // 内联数据区
};
```

**ResponseRingEntry (8192 bytes)**:
```cpp
struct ResponseRingEntry {
    // HEADER_BYTES = 32 bytes
    uint64_t requestId;        // 对应请求的ID
    uint32_t statusCode;       // 状态码 (StatusCode)
    uint32_t eventDomain;      // 事件域 (Event类型时使用)
    uint32_t eventType;        // 事件类型
    uint32_t flags;            // 标志位
    uint32_t resultSize;       // 结果数据大小
    ResponseMessageKind messageKind;  // Reply(0) / Event(1)
    uint8_t  payloadKind;      // 0=inline payload, 1=file payload ref
    uint8_t  reserved;
    
    // INLINE_PAYLOAD_BYTES = 8192 - 32 = 8160 bytes
    std::array<uint8_t, 8160> payload;  // 响应/事件数据
};
```

### 2.3 BootstrapHandles (引导句柄)

客户端和服务端建立连接时通过Unix Socket传递的句柄集合：

| 字段 | 说明 | 方向 |
|------|------|------|
| `shmFd` | 共享内存文件描述符 | Client <-> Server |
| `highReqEventFd` | 高优先级请求事件通知 | Client -> Server |
| `normalReqEventFd` | 普通优先级请求事件通知 | Client -> Server |
| `respEventFd` | 响应/事件通知 | Server -> Client |
| `reqCreditEventFd` | 请求队列空位通知 | Server -> Client |
| `respCreditEventFd` | 响应队列空位通知 | Client -> Server |
| `sessionId` | 唯一会话标识 | Client <-> Server |

### 2.3.1 各EventFd详细作用与工作机制

| EventFd | 触发场景 | 监听方 | 作用价值 |
|---------|---------|--------|---------|
| `highReqEventFd` | Client写入High Ring后signal | Server DispatcherThread | **减少CPU空转**: Server无需轮询共享内存，等待通知即可处理高优先级请求 |
| `normalReqEventFd` | Client写入Normal Ring后signal | Server DispatcherThread | **优先级隔离**: 独立的通知通道，高优请求不受普通请求影响，确保响应及时性 |
| `respEventFd` | Server写入Response Ring后signal | Client ResponseWorker | **异步响应**: Client无需轮询，收到通知后立即读取响应，降低延迟 |
| `reqCreditEventFd` | Server消费请求后signal | Client SubmitWorker | **背压控制**: 队列满时Client阻塞等待，Server消费后通知Client继续写入，防止内存溢出 |
| `respCreditEventFd` | Client消费响应后signal | Server ResponseWriter | **流控平衡**: Server响应队列满时阻塞，Client消费后通知Server继续写入 |

**EventFd工作原理示意图**：

```
+--------+                      +--------+
| Client |                      | Server |
+---+----+                      +----+---+
    |                                |
    | 1. Write HighReq to SHM        |
    +---------------------------->   |
    |                                |
    | 2. Signal highReqEventFd       |
    +---------------------------->   |
    |                                |
    |                         [poll唤醒]
    |                                |
    |                          Read  |
    |  <------------------------------+
    |                                |
    |                          Process
    |                                |
    |                          [队列满?]
    |                                |
    |  <------------------------------+
    |    4. Wait respCreditEventFd   | 3. Write Response
    |    (阻塞等待空位)               |    (QueueFull)
    |                                |
    |  <------------------------------+
    |    5. Signal respCreditEventFd | [poll等待]
    |    (消费响应后通知)              |
    |                                |
    | 6. Read Response               |
    | <-------------------------------+
    |                                |
```

**为什么使用5个EventFd而不是轮询？**

1. **性能**: 轮询需要CPU不断检查共享内存状态，EventFd基于内核通知，无请求时CPU占用为0
2. **实时性**: 事件通知是即时的，轮询有检测延迟
3. **背压**: EventFd天然支持阻塞等待，简化流控实现
4. **优先级**: 独立的High/Normal EventFd实现真正的优先级隔离


---


## 3. 服务拉起与连接流程

### 3.1 VesClient::Connect() 连接流程

当应用程序调用`VesClient::Connect()`时，完整的拉起和连接流程如下：

```
+--------+     +--------------+     +------------------+     +----------------------+
|  App   |     |   VesClient  |     |  SystemAbility   |     | VirusExecutorService |
|        |     |   (客户端)    |     |  Manager (SAM)   |     |   (引擎服务进程)      |
+---+----+     +------+-------+     +--------+---------+     +----------+-----------+
    |                 |                      |                        |
    | Connect()       |                      |                        |
    +---------------->+                      |                        |
    |                 |                      |                        |
    |                 | Init()               |                        |
    |                 +------+               |                        |
    |                 |      | 创建RpcClient |                        |
    |                 |      | 设置恢复策略   |                        |
    |                 |<-----+               |                        |
    |                 |                      |                        |
    |                 | RpcClient::Init()    |                        |
    |                 +------+               |                        |
    |                 |      | 检查会话状态   |                        |
    |                 |      | 无会话，需要恢复|                        |
    |                 |<-----+               |                        |
    |                 |                      |                        |
    |                 | EnsureLiveSession()  |                        |
    |                 +------+               |                        |
    |                 |      | BeginSessionOpen|                       |
    |                 |      | (Recovering)   |                        |
    |                 |<-----+               |                        |
    |                 |                      |                        |
    |                 | OpenSession()        |                        |
    |                 +--------------------->+                        |
    |                 |                      |                        |
    |                 |                      | CheckSystemAbility()   |
    |                 |                      | 检查SA是否存在         |
    |                 |                      +-----------+            |
    |                 |                      |           |            |
    |                 |                      | 不存在    | 已存在     |
    |                 |                      |           |            |
    |                 |                      | LoadSystemAbility()    |
    |                 |                      +-----------+----------->+
    |                 |                      |                       |
    |                 |                      |                       | OnStart()
    |                 |                      |                       | 初始化引擎
    |                 |                      |                       | 创建RpcServer
    |                 |                      |                       |
    |                 |                      |                       | Publish()
    |                 |                      |<----------------------+
    |                 |                      |                       |
    |                 |                      | 返回远程对象引用      |
    |                 |<---------------------+                       |
    |                 |                      |                       |
    |                 | 创建共享内存         |                       |
    |                 | 初始化BootstrapHandles|                      |
    |                 |                      |                       |
    |                 | FinalizeSessionOpen()|                       |
    |                 | (Active)             |                       |
    |                 |                      |                       |
    |                 | 启动SubmitWorker     |                       |
    |                 | 启动ResponseWorker   |                       |
    |                 | 启动WatchdogThread   |                       |
    |                 |                      |                       |
    | 返回VesClient   |                      |                       |
    |<----------------+                      |                       |
+---+----+     +------+-------+     +--------+---------+     +----------+-----------+
```

### 3.2 服务端关闭与自动恢复流程（简述）

当引擎进程崩溃或需要关闭时，客户端通过恢复策略处理。详细流程见第6章[冷却(Cooldown)流程](#6-冷却cooldown流程)和第8章[自启与恢复流程](#8-自启与恢复流程)：

```
+--------+     +--------------+     +------------------+     +----------------------+
| Client |     |   RpcClient  |     |  DeathRecipient  |     | VirusExecutorService |
+---+----+     +------+-------+     +--------+---------+     +----------+-----------+
    |                 |                      |                        |
    |                 |                      |                        | 引擎崩溃
    |                 |                      |                        | (abort/exit)
    |                 |                      |<-----------------------+
    |                 |                      |                        |
    |                 |                      | OnRemoteDied()         |
    |                 |                      | 死亡通知回调           |
    |                 |<---------------------+                        |
    |                 |                      |                        |
    |                 | HandleEngineDeath()  |                        |
    |                 |                      |                        |
    |                 | 1. 关闭当前会话      |                        |
    |                 | 2. 标记pending失败   |                        |
    |                 | 3. 调用onEngineDeath |                        |
    |                 |    (Restart+200ms)   |                        |
    |                 |                      |                        |
    |                 | StartRecovery(200ms) |                        |
    |                 | (进入Cooldown)       |                        |
    |                 |                      |                        |
    |                 | [等待200ms冷却]       |                        |
    |                 |                      |                        |
    |                 | EnsureLiveSession()  |                        |
    |                 | (Recovering)         |                        |
    |                 |                      |                        |
    |                 | LoadSystemAbility()  |                        |
    |                 +---------------------------------------------->+
    |                 |                      |                        | 新进程启动
    |                 | 返回新会话句柄       |                        |
    |                 |<------------------------------------------------+
    |                 |                      |                        |
    | 恢复完成         |                      |                        |
    | 继续请求处理     |                      |                        |
    |<----------------+                      |                        |
+---+----+     +------+-------+     +--------+---------+     +----------+-----------+
```

### 3.3 关键代码分析

**VesClient连接入口** (`ves_client.cpp`):
```cpp
std::unique_ptr<VesClient> VesClient::Connect(VesClientOptions options, 
                                              VesClientConnectOptions connectOptions) {
    // 1. 构建ControlLoader，通过SAM加载SA
    auto client = std::make_unique<VesClient>(
        BuildControlLoader(connectOptions), std::move(options));
    
    // 2. 初始化，建立连接（会触发SA拉起）
    if (client->Init() != MemRpc::StatusCode::Ok) {
        return nullptr;
    }
    return client;
}

MemRpc::StatusCode VesClient::Init() {
    // 1. 创建BootstrapChannel（内部通过SAM操作）
    bootstrapChannel_ = std::make_shared<VesBootstrapChannel>(
        controlLoader_, options_.openSessionRequest, ...);
    
    // 2. 设置到RpcClient
    client_.SetBootstrapChannel(bootstrapChannel_);
    
    // 3. 配置恢复策略（包含服务端生命周期管理）
    client_.SetRecoveryPolicy(BuildRecoveryPolicy(options_));
    
    // 4. 初始化RpcClient，触发会话建立
    // 如果服务端未运行，会通过SAM自动拉起
    return client_.Init();
}
```

**ControlLoader - SA加载入口** (`ves_client.cpp`):
```cpp
// 通过鸿蒙SAM框架加载SystemAbility
VesClient::ControlLoader BuildControlLoader(VesClientConnectOptions connectOptions) {
    return [connectOptions]() -> OHOS::sptr<IVirusProtectionExecutor> {
        auto sam = OHOS::SystemAbilityManagerClient::GetInstance()
                       .GetSystemAbilityManager();
        
        // 1. 先检查SA是否已存在
        auto remote = sam->CheckSystemAbility(VIRUS_PROTECTION_EXECUTOR_SA_ID);
        
        if (remote != nullptr) {
            // SA已存在，先关闭旧会话
            auto control = OHOS::iface_cast<IVirusProtectionExecutor>(remote);
            control->CloseSession();
            std::this_thread::sleep_for(100ms);
        }
        
        // 2. 加载SA（如果不存在会触发SA启动）
        remote = sam->LoadSystemAbility(VIRUS_PROTECTION_EXECUTOR_SA_ID, 
                                        connectOptions.loadTimeoutMs);
        return OHOS::iface_cast<IVirusProtectionExecutor>(remote);
    };
}
```

**恢复策略 - 服务端生命周期管理** (`ves_client.cpp`):
```cpp
MemRpc::RecoveryPolicy BuildRecoveryPolicy(const VesClientOptions& options) {
    MemRpc::RecoveryPolicy policy = options.recoveryPolicy;
    
    // 执行超时后重启服务端
    if (!policy.onFailure) {
        policy.onFailure = [](const MemRpc::RpcFailure& failure) {
            if (failure.status == MemRpc::StatusCode::ExecTimeout) {
                return MemRpc::RecoveryDecision{
                    MemRpc::RecoveryAction::Restart, 
                    DEFAULT_RESTART_DELAY_MS  // 200ms
                };
            }
            return MemRpc::RecoveryDecision{
                MemRpc::RecoveryAction::Ignore, 0
            };
        };
    }
    
    // 引擎死亡后自动拉起新进程
    if (!policy.onEngineDeath) {
        policy.onEngineDeath = [](const MemRpc::EngineDeathReport& report) {
            return MemRpc::RecoveryDecision{
                MemRpc::RecoveryAction::Restart,
                DEFAULT_RESTART_DELAY_MS  // 200ms
            };
        };
    }
    
    return policy;
}
```

## 4. scanFile完整调用链

### 4.1 整体流程图

```
+--------+     +------------+     +---------------------------------------+     +-------------+
|   App  |     |  VesClient |     |              RpcClient                  |     | SubmitWorker |
+---+----+     +------+-----+     +------------------+--------------------+     +------+-------+
    |                 |                 |                  |                    |
    | ScanFile()      |                 |                  |                    |
    +---------------->+                 |                  |                    |
    |                 | InvokeApi()     |                  |                    |
    |                 | (EncodeInvoke)  |                  |                    |
    |                 +---------------->+                  |                    |
    |                 |                 | RetryUntilRecoverySettles()          |
    |                 |                 | +----------------------------------+   |
    |                 |                 | | 循环：                           |   |
    |                 |                 | |   1. 执行InvokeAsync()          |   |
    |                 |                 | |   2. 检查返回状态               |   |
    |                 |                 | |   3. 如需要恢复则等待           |   |
    |                 |                 | |   4. 重试直到成功或超时         |   |
    |                 |                 | +----------------------------------+   |
    |                 |                 |                  |                    |
    |                 |                 |     InvokeAsync()+------------------->+
    |                 |                 |                  |                    |
    |                 |                 |                  |                    | TryPushRequest()
    |                 |                 |                  |                    +------------------>
    |                 |                 |                  |                    |
    |                 |                 |                  |                    | signal eventfd
    |                 |                 |                  |                    +------------------>


继续到服务端:

+-------------+     +----------------+     +-----------+     +------------------+
| Dispatcher  |     |ThreadPoolExecutor|    |  Handler  |     | VesEngineService  |
+------+------+     +--------+-------+     +-----+-----+     +---------+--------+
       |                     |                  |                    |
       | poll eventfd唤醒     |                  |                    |
       |<-----------------------------------------------------------+
       |                     |                  |                    |
       | PopRequest()        |                  |                    |
       |<-----------------------------------------------------------+
       |                     |                  |                    |
       | TrySubmit(task)     |                  |                    |
       +-------------------->+                  |                    |
       |                     |                  |                    |
       |                     | 执行Handler      |                    |
       |                     +----------------->+                    |
       |                     |                  |  ScanFile(request) |
       |                     |                  +------------------->+
       |                     |                  |                    |
       |                     |                  |                    | IsScanEngineEnabled()
       |                     |                  |                    |
       |                     |                  |                    | EvaluateSamplePath()
       |                     |                  |                    |
       |                     |                  | 返回ScanFileReply  |
       |                     |                  |<-------------------+
       |                     | 返回RpcReply     |                    |
       |                     |<-----------------+                    |
       |                     |                  |                    |
       |                     | EnqueueCompletion|                    |
       |                     | signal respEventFd                   |
       |                     +--------------------------------------->


返回到客户端:

+-------------+     +-----------+     +------------+     +--------+
|  Response   |     |  RpcClient |     |  VesClient |     |  App   |
+------+------+     +-----+------+     +------+-----+     +---+----+
       |                  |                  |                |
       | poll eventfd唤醒  |                  |                |
       |<------------------------------------------------------+
       |                  |                  |                |
       | PopResponse()    |                  |                |
       |<------------------------------------------------------+
       |                  |                  |                |
       | 通知RpcFuture    |                  |                |
       +----------------->+                  |                |
       |                  |                  |                |
       |                  | WaitAndDecode()  |                |
       |                  +----------------->+                |
       |                  |                  | 返回StatusCode  |
       |                  |<-----------------+                |
       |                  |                  | 返回ScanFileReply
       |                  |                  +--------------->+
```

### 4.1.1 冷却状态与队列满处理（关键流程）

在实际的`ScanFile`调用过程中，需要处理冷却状态和队列满的情况。以下分三层展示：

**第一层：调用层 - RetryUntilRecoverySettles 重试逻辑**

```
+--------+     +------------+     +-----------+
|   App  |     |  VesClient |     |  RpcClient |
+---+----+     +------+-----+     +-----+------+
    |                 |                 |
    | ScanFile()      |                 |
    +---------------->+                 |
    |                 | InvokeApi()     |
    |                 | RetryUntilRecoverySettles()
    |                 | (外层重试包装器) |
    |                 +------+          |
    |                 |      | 调用执行  |
    |                 |      v          |
    |                 |  +-----------------------------+
    |                 |  | 执行invoke，检查结果         |
    |                 |  | CooldownActive? -> 等待冷却 |
    |                 |  | PeerDisconnected? -> 等待恢复|
    |                 |  | 其他状态 -> 直接返回        |
    |                 |  +-----------------------------+
    |                 |      |          |
    |                 |      | 成功/失败 |
    | 返回结果        |<-----+          |
    |<----------------+                 |
+---+----+     +------+-----+     +-----+------+
```

**第二层：提交层 - SubmitWorker 循环处理**

```
+--------+     +------------+     +-----------+     +------------------+
|   App  |     |  VesClient |     |  RpcClient |     | SubmitWorker     |
+---+----+     +------+-----+     +-----+------+     +------+-----------+
    |                 |                 |                   |
    | ScanFile()      |                 |                   |
    +---------------->+                 |                   |
    |                 | InvokeApi()     |                   |
    |                 +---------------->+                   |
    |                 |                 |                   |
    |                 | RetryUntilRecoverySettles()        |
    |                 | (自动处理恢复状态)  |               |
    |                 +------+          |                   |
    |                 |      | 检查状态  |                   |
    |                 |      v          |                   |
    |                 |  +-----------------------------+    |
    |                 |  | 状态检查                    |    |
    |                 |  | Cooldown? -> 等待冷却结束   |    |
    |                 |  | Recovering? -> 等待恢复完成 |    |
    |                 |  | NoSession? -> 尝试建立会话  |    |
    |                 |  | Active? -> 继续提交         |    |
    |                 |  +-----------------------------+    |
    |                 |      |          |                   |
    |                 |      | Active   |                   |
    |                 |      v          |                   |
    |                 | InvokeAsync()   |                   |
    |                 +---------------->+                   |
    |                 |                 | 提交到submitQueue_|
    |                 |                 +------------------>+
    |                 |                 |                   |
    |                 |                 | SubmitOne()       |
    |                 |                 | 循环处理提交      |
    |                 |                 +------+            |
    |                 |                 |      |            |
    |                 |                 |      v            |
    |                 |                 | TryAdmitSubmit()  |
    |                 |                 +------+            |
    |                 |                 |      |            |
    |                 |                 |      v            |
    |                 |                 | TryPushRequest()  |
    |                 |                 | (写入共享内存)    |
    |                 |                 +------+            |
    |                 |                 |      |QueueFull   |
    |                 |                 |      v            |
    |                 |                 | WaitForReqCredit()|
    |                 |                 | poll(reqCreditFd) |
    |                 |                 | (阻塞等待空位)    |
    |                 |                 +------+            |
    |                 |                 |      |Ready/Retry |
    |                 |                 |      v            |
    |                 |                 | return QueueFull  |
    |                 |                 +------+            |
    |                 |                 |      |            |
    |                 |                 |      v            |
    |                 |                 | SubmitOne()中continue|
    |                 |                 | 重试TryAdmitSubmit() |
    |                 |                 |      |成功        |
    |                 |                 |<-----+            |
    |                 | 返回RpcFuture   |                   |
    |                 |<----------------+                   |
    | 返回结果        |                   |                   |
    |<----------------+                   |                   |
+---+----+     +------+-----+     +-----+------+     +------+-----------+
```

**第三层：底层 - 队列满时的背压处理**

```
+-------------+     +------------------+     +-------------+
| SubmitWorker|     |  Request Ring    |     |  Dispatcher |
+------+------+     +--------+---------+     +------+------+
       |                     |                      |
       | TryPushRequest()    |                      |
       +-------------------->+                      |
       |                     |                      |
       | 返回QueueFull       |                      |
       |<--------------------+                      |
       |                     |                      |
       | WaitForRequestCredit|                      |
       | poll(reqCreditFd)   |                      |
       | 阻塞等待...          |                      |
       |                     |                      |
       |                     |<---------------------+
       |                     | PopRequest()         |
       |                     | signal reqCreditFd   |
       |<--------------------+                      |
       | poll返回           |                      |
       |                     |                      |
       | 重试TryPushRequest  |                      |
       +-------------------->+                      |
       | 成功                |                      |
       |<--------------------+                      |
+------+------+     +--------+---------+     +------+------+
```

**冷却流程详细说明**：
1. **RetryUntilRecoverySettles()**：外层重试包装器，调用Invoke后检查结果，如果返回`CooldownActive`或`PeerDisconnected`且处于恢复状态，则等待并重试
2. **Cooldown状态**：调用返回`CooldownActive`，进入`WaitForCooldownToSettle`等待，冷却结束后重试Invoke
3. **Recovering状态**：调用返回`PeerDisconnected`，进入`WaitOneRecoveryRetryTick`等待，恢复完成后重试Invoke
4. **SubmitWorker层**：`WaitUntilSessionReadyForSubmit`在提交前检查状态，确保会话就绪后才尝试PushRequest

> **RpcFuture 说明**：异步调用返回的 `RpcFuture` 对象表示一个待完成的请求，支持：
> - `IsReady()`：检查是否就绪（不阻塞）
> - `Wait(&reply)`：阻塞等待结果（右值引用限定，只能调用一次）

**队列满处理详细说明**：
1. **TryPushRequest()**：尝试写入共享内存Ring
2. **QueueFull**：如果队列满，返回QueueFull状态
3. **WaitForRequestCredit()**：SubmitWorker阻塞等待`reqCreditEventFd`
4. **服务端通知**：当服务端消费请求后，通过`reqCreditEventFd`通知客户端
5. **重试**：收到通知后重试PushRequest


### 4.2 关键代码详解

**客户端调用入口** (`ves_client.cpp`):
```cpp
// 1. 对外API入口
MemRpc::StatusCode VesClient::ScanFile(const ScanTask& scanTask,
                                       ScanFileReply* reply,
                                       MemRpc::Priority priority,
                                       uint32_t execTimeoutMs) {
    return InvokeApi<ScanTask, ScanFileReply>(
        static_cast<MemRpc::Opcode>(VesOpcode::ScanFile),
        scanTask, reply, priority, execTimeoutMs);
}

// 2. 通用API调用模板
template <typename Request, typename Reply>
MemRpc::StatusCode VesClient::InvokeApi(MemRpc::Opcode opcode,
                                        const Request& request,
                                        Reply* reply,
                                        MemRpc::Priority priority,
                                        uint32_t execTimeoutMs) {
    // 2.1 序列化请求
    std::vector<uint8_t> payload;
    EncodeInvokePayload(opcode, request, &payload);
    
    // 2.2 memrpc 自动选择 inline payload 或 file payload envelope。
    
    // 2.3 执行 memrpc 调用，自动处理恢复
    return client_.RetryUntilRecoverySettles([&]() {
        return InvokeMemRpcApi(context, opcode, priority, execTimeoutMs, payload, reply);
    });
}
```

**服务端Handler注册** (`ves_engine_service.cpp`):
```cpp
void VesEngineService::RegisterHandlers(RpcHandlerSink* sink) {
    RegisterVesTypedHandler<ScanTask, ScanFileReply>(
        sink,
        static_cast<MemRpc::Opcode>(VesOpcode::ScanFile),
        [this](const ScanTask& request) { return ScanFile(request); });
}

// 业务逻辑实现
ScanFileReply VesEngineService::ScanFile(const ScanTask& request) const {
    ScanFileReply result;
    
    // 1. 检查引擎状态
    if (!initialized() || !IsScanEngineEnabled()) {
        result.code = -1;
        return result;
    }
    
    // 2. 评估样本路径
    const auto behavior = EvaluateSamplePath(request.path);
    
    // 3. 处理特殊样本
    if (behavior.shouldCrash) {
        HILOGE("ScanFile(%s): crash requested", request.path.c_str());
        std::abort();  // 模拟崩溃
    }
    if (behavior.sleepMs > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(behavior.sleepMs));
    }
    
    // 4. 返回结果
    result.code = 0;
    result.threatLevel = behavior.threatLevel;
    return result;
}
```


---

## 5. 异常文件处理流程

### 5.1 样本规则评估

```
+---------------+     +------------------+     +------------------+
| 开始扫描文件  |---->| 路径包含"crash"? |---->| 设置shouldCrash  |
+---------------+     +--------+---------+     | =true            |
                      |否                    +--------+---------+
                      v                               |
               +------------------+                  |
               | 路径包含"sleep"?  |<-----------------+
               +--------+---------+                  |
               |是                |否                 |
               v                  v                  |
        +-------------+    +------------------+      |
        | 解析sleepN  |    | 包含"virus"/     |      |
        | 中的N      |    | "eicar"?         |      |
        +--------+----+    +--------+---------+      |
                 |         |是        |否            |
                 v         v          v              |
        +------------------+  +------------------+   |
        | 设置sleepMs=N    |  | 设置threatLevel=1|   |
        | threatLevel=1   |  +--------+---------+   |
        +--------+---------+           |             |
                 |                     v             |
                 |            +------------------+   |
                 |            | threatLevel=0    |   |
                 |            +--------+---------+   |
                 |                     |             |
                 v                     v             v
               +------------------------------------------+
               |           返回ScanFileReply               |
               +------------------------------------------+
                               |
                               v
                      +----------------+
                      | abort()崩溃     |<--- shouldCrash=true
                      +----------------+
```

> **说明：样本规则是测试框架功能**
> 
> `crash`、`sleepN`、`virus`等样本规则**仅用于测试**，通过文件名触发特定行为来验证系统的异常处理能力。这些不是真实的安全检测逻辑，而是测试工具。

### 5.2.1 RegistryServer 说明

`RegistryServer` 是 `VesBootstrapChannel` 内部的组件，负责通过 Unix Socket 与 SA (System Ability) 框架通信：
- 监听引擎进程死亡事件
- 触发 `OnRemoteDied()` 回调
- 与第3.1节图中的 `SystemAbility Manager (SAM)` 是同一架构层面的不同表述


### 5.2 样本行为代码

```cpp
// ves_sample_rules.cpp
SampleBehavior EvaluateSamplePath(const std::string& path) {
    SampleBehavior behavior;
    
    // 1. 崩溃样本 - 用于测试恢复机制
    if (path.find("crash") != std::string::npos) {
        behavior.shouldCrash = true;
    }
    
    // 2. 耗时样本 - 用于测试超时处理
    behavior.sleepMs = ParseSleepMs(path);  // 解析sleepN
    
    // 3. 病毒样本 - 模拟检出威胁
    if (path.find("virus") != std::string::npos || 
        path.find("eicar") != std::string::npos) {
        behavior.threatLevel = 1;
    }
    
    // 4. 耗时样本也视为威胁
    if (behavior.sleepMs > 0) {
        behavior.threatLevel = 1;
    }
    
    return behavior;
}
```

### 5.3 崩溃后的恢复流程

当服务端处理"crash"样本时会调用`abort()`导致进程崩溃，客户端通过以下机制检测并恢复：

```
+--------+     +-------------------+     +---------------+     +---------------+
| Client |     | VesControlProxy   |     | RegistryServer|     | 新引擎进程    |
+---+----+     +---------+---------+     +-------+-------+     +-------+-------+
    |                    |                      |                     |
    |                    | MonitorSocket()      |                     |
    |                    | poll检测POLLHUP      |                     |
    |                    |                      |                     |
    | OnRemoteDied()     |                      |                     |
    |<-------------------+                      |                     |
    |                    |                      |                     |
    | HandleEngineDeath()|                      |                     |
    |                    |                      |                     |
    | CloseLiveSession   |                      |                     |
    | IfSnapshotMatches()|                      |                     |
    |                    |                      |                     |
    | 标记pending请求    |                      |                     |
    | 为CrashedDuringExec|                      |                     |
    |                    |                      |                     |
    | onEngineDeath回调  |                      |                     |
    | 默认Restart+200ms  |                      |                     |
    |                    |                      |                     |
    | StartRecovery()    |                      |                     |
    |                    |                      |                     |
    | OpenSession()      |                      |                     |
    | LoadSystemAbility()|                      |                     |
    +------------------->| LoadSystemAbility()  |                     |
    |                    +--------------------->|                     |
    |                    |                      | LoadCallback触发    |
    |                    |                      | fork() + exec()     |
    |                    |                      +-------------------->+
    |                    |                      |                     |
    |                    |                      | 新引擎就绪          |
    |                    |<-------------------------------------------+
    |                    |                      |                     |
    | 返回新会话句柄     |                      |                     |
    |<-------------------+                      |                     |
    |                    |                      |                     |
    | 恢复完成，继续调用 |                      |                     |
    |                    |                      |                     |
+---+----+     +---------+---------+     +-------+-------+     +-------+-------+

### 5.4 客户端对各种异常的统一处理机制

上述的`crash`、`sleep`等样本触发的异常，客户端通过统一的恢复机制处理：

| 样本类型 | 触发异常 | 客户端检测方式 | 处理机制 | 结果 |
|---------|---------|--------------|---------|------|
| `crash` | 服务端进程崩溃(abort) | `DeathRecipient::OnRemoteDied()` | `onEngineDeath`回调→Restart+200ms冷却→重新拉起SA | 自动恢复，调用方无感知（有延迟） |
| `sleepN` | 请求执行超时 | `WatchdogThread`超时检测 | `onFailure`回调→Restart+200ms冷却→重新拉起SA | 返回ExecTimeout错误，触发恢复 |
| `virus` | 正常返回结果 | 无异常 | 正常处理 | 返回扫描结果 |

**关键异常处理代码（简化示意）**：

```cpp
// RpcClient::Impl::HandleEngineDeathLocked() - 处理引擎崩溃（简化示意）
void HandleEngineDeathLocked(uint64_t observedSessionId, uint64_t deadSessionId) {
    // 实际代码：包含observedSessionId == 0检查、sessionId匹配检查等保护逻辑
    
    // 1. 确定死亡的sessionId
    const uint64_t resolvedDeadSessionId = deadSessionId != 0 ? deadSessionId : observedSessionId;
    
    EngineDeathReport report;
    report.deadSessionId = resolvedDeadSessionId;
    
    // 2. 关闭匹配的会话
    sessionTransport_.CloseLiveSessionIfSnapshotMatches(observedSessionId);
    
    // 3. 标记所有pending请求为CrashedDuringExecution
    ResolveAllPending(StatusCode::CrashedDuringExecution);
    
    // 4. 调用恢复策略
    const RecoveryPolicy policy = LoadRecoveryPolicy();
    if (!policy.onEngineDeath) {
        EnterNoSessionWithoutLiveSessionLocked();
        return;
    }
    
    const RecoveryDecision decision = policy.onEngineDeath(report);
    if (decision.action == RecoveryAction::Ignore) {
        EnterNoSessionWithoutLiveSessionLocked();
        return;
    }
    
    // 5. 应用恢复决策（进入Cooldown或Recovering）
    ApplyRecoveryDecisionLocked(decision, observedSessionId, 
                                PendingRequestRecoveryAction::KeepCurrentState);
}

// RpcClient::Impl::MaybeRunPendingTimeouts() - 超时检测（简化示意）
void MaybeRunPendingTimeouts() {
    const auto now = std::chrono::steady_clock::now();
    // 获取所有超时的请求
    std::vector<PendingRequest> expired = requestStore_.TakeExpired(now);
    for (auto& pending : expired) {
        // 返回ExecTimeout并触发恢复决策
        FailAndResolve(pending.info, StatusCode::ExecTimeout, pending.future);
    }
}

// FailAndResolve -> ApplyFailureRecoveryDecision 调用链（简化示意）
void ApplyFailureRecoveryDecision(const PendingInfo& info, StatusCode status) {
    const RecoveryPolicy policy = LoadRecoveryPolicy();
    if (!policy.onFailure) {
        return;
    }
    
    RpcFailure failure;
    failure.status = status;
    failure.opcode = info.opcode;
    failure.requestId = info.requestId;
    
    // 实际代码：使用决策结果触发恢复流程
    const RecoveryDecision decision = policy.onFailure(failure);
    if (decision.action == RecoveryAction::Restart) {
        ScheduleRecovery(decision.delayMs);
    }
}
```

**对调用方的影响**：
- **崩溃场景**：已发送的pending请求立即返回`CrashedDuringExecution`错误；新请求在恢复期间会被阻塞（`WaitForCooldownToSettle`），恢复完成后继续执行
- **超时场景**：超时请求返回`ExecTimeout`错误，同时触发恢复流程重启服务端，下一次调用会连接到新服务
- **连续崩溃保护**：如果服务端连续崩溃，客户端会进入`NoSession`状态，后续调用快速返回错误，避免无限重试
- **重要区别**：崩溃会中断正在执行的请求（返回错误），而超时是在等待期间检测并返回错误


## 6. 冷却(Cooldown)流程

> **本章定位**：本章详细讲解冷却机制的原理和状态机，是第4.1.1节冷却流程的深入展开。

### 6.1 冷却机制概述

冷却机制用于在故障恢复后避免立即重试导致的频繁抖动，给系统留出稳定时间。

### 6.2 冷却状态机

#### 6.2.1 基本状态转换

```
                    +---------------+
         +--------->|  Uninitialized |<---------+
         |          |   (未初始化)   |          |
         |          +-------+--------+          |
         |                  | Init()            |
         |                  v                   |
         |          +-------+--------+          |
         |          |  Recovering    |          |
         |          |   (恢复中)      |          |
         |          +-------+--------+          |
         |                  | 恢复成功           |
         |                  v                   |
    Shutdown()      +-------+--------+     连接失败/断开
         |          |     Active     |----------+
         |          |    (活跃)       |
         |          +-------+--------+
         |                  |
         |        +---------+---------+
         |        v                   v
         |   +---------+      +-----------+
         +-->+  Closed |      |  Cooldown |<----+
             | (已关闭) |      |  (冷却中)  |     |
             +---------+      +-----+-----+     |
                   ^                |           | delayMs
                   |                v           |
                   |          +-----------+     |
                   +----------+  NoSession |-----+
                              | (无会话)   | 冷却结束/恢复失败
                              +-----------+
                                   |
                                   v
                              +-----------+
                              | IdleClosed|
                              |(空闲关闭) |
                              +-----------+
```

#### 6.2.2 新任务在冷却期的处理逻辑（关键）

```
+--------+     +------------------+     +------------------+     +--------+
|  App   |     |    RpcClient     |     | ClientRecovery   |     | Session|
|        |     |                  |     |     State        |     |Transport|
+---+----+     +--------+---------+     +--------+---------+     +----+---+
    |                   |                        |                    |
    | ScanFile()        |                        |                    |
    +------------------->+                       |                    |
    |                   | InvokeAsync()          |                    |
    |                   +----------------------->|                    |
    |                   |                        |                    |
    |                   | EnsureLiveSession()    |                    |
    |                   +----------------------->|                    |
    |                   |                        |                    |
    |                   | 检查状态:Cooldown      |                    |
    |                   |<-----------------------+                    |
    |                   |                        |                    |
    |                   | CooldownActive()?      |                    |
    |                   | return true            |                    |
    |                   |<-----------------------+                    |
    |                   |                        |                    |
    |                   | 返回CooldownActive     |                    |
    |                   | (新任务不触发拉起!)     |                    |
    |                   |                        |                    |
    |                   | WaitForCooldown        |                    |
    |                   | ToSettle()             |                    |
    |                   +----------------------->|                    |
    |                   |                        |                    |
    |                   | 挂起等待...             |                    |
    |                   | (条件变量等待)          |                    |
    |                   |                        |                    |
    |                   | 冷却时间到/被唤醒       |                    |
    |                   |<-----------------------+                    |
    |                   |                        |                    |
    |                   | 状态变为Recovering     |                    |
    |                   | BeginSessionOpen()     |                    |
    |                   +----------------------->|                    |
    |                   |                        |                    |
    |                   |                        | OpenSession()      |
    |                   +------------------------------------------->|
    |                   |                        |                    |
    |                   |                        | 返回Ok             |
    |                   |<--------------------------------------------------+
    |                   |                        |                    |
    |                   | FinalizeSessionOpen()  |                    |
    |                   | 状态:Active            |                    |
    |                   +----------------------->|                    |
    |                   |                        |                    |
    |                   | 继续InvokeAsync        |                    |
    |                   | 提交请求到共享内存      |                    |
    |                   |                        |                    |
    | 返回结果          |                        |                    |
    |<-------------------+                       |                    |
    |                   |                        |                    |
+---+----+     +--------+---------+     +--------+---------+     +----+---+
```

**关键说明：**
1. **新任务不触发新的拉起**：在Cooldown状态下，新任务不会触发新的`StartRecovery()`，而是进入`WaitForCooldownToSettle()`等待当前冷却完成
2. **任务挂起而非失败**：调用线程会被阻塞在`WaitForCooldownToSettle()`，直到冷却结束或超时
3. **冷却结束后自动继续**：冷却结束后，会自动进入`Recovering`状态，然后`OpenSession()`，最后恢复为`Active`状态继续处理请求

#### 6.2.3 冷却期间的新任务处理代码流程

```cpp
// RpcClient::InvokeAsync 调用链
StatusCode RpcClient::Impl::InvokeAsyncInternal(const RpcCall& call, RpcFuture* future) {
    // 1. 检查当前状态
    RecoveryRuntimeSnapshot snapshot = GetRecoveryRuntimeSnapshot();
    
    // 2. 如果处于冷却状态，等待冷却结束（不触发新的恢复）
    if (snapshot.lifecycleState == ClientLifecycleState::Cooldown) {
        StatusCode waitStatus = WaitForCooldownToSettle(deadline);
        if (waitStatus != StatusCode::Ok) {
            return waitStatus;  // 冷却等待失败
        }
    }
    
    // 3. 如果处于Recovering状态，等待恢复完成
    if (snapshot.lifecycleState == ClientLifecycleState::Recovering) {
        StatusCode waitStatus = WaitOneRecoveryRetryTick(deadline);
        if (waitStatus != StatusCode::Ok) {
            return waitStatus;
        }
    }
    
    // 4. 现在应该是Active状态，提交请求
    return SubmitRequest(call, future);
}
```

### 6.3 冷却实现代码（简化示意）

```cpp
// ClientRecoveryState 类核心逻辑（实际代码更复杂，以下为简化示意）

class ClientRecoveryState {
public:
    // 启动恢复，可能进入Cooldown或Recovering
    void StartRecovery(uint32_t delayMs, uint64_t currentSessionId) {
        const uint64_t cooldownUntilMs = MonotonicNowMs() + delayMs;
        
        // 如果有延迟，进入Cooldown状态
        auto nextState = (delayMs == 0) 
            ? ClientLifecycleState::Recovering 
            : ClientLifecycleState::Cooldown;
        
        // 实际代码：TransitionLifecycle参数顺序和逻辑更复杂
        TransitionLifecycle(nextState, delayMs, currentSessionId, 
                           CooldownWindowChange::Set, cooldownUntilMs);
    }
    
    // 检查是否处于冷却期
    bool CooldownActive() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return MonotonicNowMs() < cooldownUntilMs_;
    }
    
    // 等待冷却结束（简化示意）
    StatusCode WaitForCooldownToSettle(...) {
        std::unique_lock<std::mutex> lock(mutex_);
        
        while (true) {
            if (lifecycleState_ != ClientLifecycleState::Cooldown) {
                return RecoveryStateWaitResultLocked();
            }
            
            if (MonotonicNowMs() >= cooldownUntilMs_) {
                return StatusCode::Ok;  // 冷却结束
            }
            
            // 实际代码：等待条件变量或超时，处理workersRunning等状态
            recoveryStateCv_.wait_until(lock, wakeAt, [...]);
        }
    }
};
```

### 6.4 冷却期间请求处理

```cpp
// RpcClient::SubmitWorker::WaitUntilSessionReadyForSubmit()
StatusCode WaitUntilSessionReadyForSubmit() {
    while (true) {
        const StatusCode sessionStatus = owner_->EnsureLiveSession();
        
        if (sessionStatus == StatusCode::Ok) {
            return StatusCode::Ok;  // 会话就绪
        }
        
        if (sessionStatus == StatusCode::CooldownActive) {
            // 处于冷却期，等待冷却结束
            const StatusCode waitStatus = owner_->WaitForCooldownToSettle(...);
            if (waitStatus == StatusCode::Ok) {
                continue;  // 冷却结束，重试
            }
            return waitStatus;
        }
        
        // 其他错误处理...
    }
}
```


---

## 7. 通信流程

### 7.1 客户端到服务端请求流程

```
+---------------------------------------------------------------+
|                        客户端到服务端请求                       |
+---------------------------------------------------------------+
|                                                               |
|  +-------------------+     +-----------------------------+    |
|  |   RpcClient       |     |       Shared Memory         |    |
|  |  +-------------+  |     |  +---------+  +---------+   |    |
|  |  | SubmitWorker|  |     |  |High Ring|  |NormalRing|  |    |
|  |  |  (提交线程)  |  |     |  | 8 entries| | 8 entries|  |    |
|  |  +------+------+  |     |  +----+----+  +----+----+   |    |
|  |         |         |     |       |            |        |    |
|  |  TryPushRequest() |     |       v            v        |    |
|  |  写入请求到Ring   |------> PopRequest()     PopRequest()  |
|  |         |         |     |       ^            ^        |    |
|  |  成功?  |         |     |       |            |        |    |
|  |    |    |         |     +-------|------------|--------+    |
|  |   是   否         |             |            |             |
|  |    |    |         |     +-------+------------+--------+    |
|  |    v    v         |     |    RpcServer               |    |
|  |  返回  WaitForReq |     |  +---------------------+   |    |
|  |        Credit()   |     |  | DispatcherThread    |   |    |
|  |        等待通知   |<----|  | (poll eventfd唤醒)  |   |    |
|  |        ^          |     |  +----------+----------+   |    |
|  |        |          |     |             |              |    |
|  +--------+----------+     |    分发到ThreadPoolExecutor |    |
|                            |             |              |    |
|                            +-------------+--------------+    |
|                                          |                   |
|                                          v                   |
|                                  reqCreditEventFd           |
|                                  队列有空位时通知             |
+------------------------------------------+-------------------+
```

### 7.2 服务端到客户端响应流程

```
+---------------------------------------------------------------+
|                        服务端到客户端响应                       |
+---------------------------------------------------------------+
|                                                               |
|  +------------------------+     +-------------------------+   |
|  |     RpcServer          |     |      Shared Memory      |   |
|  |  +------------------+  |     |  +------------------+   |   |
|  |  | 业务Handler执行  |  |     |  |  Response Ring   |   |   |
|  |  |  (ScanFile等)    |  |     |  |  (8 entries)     |   |   |
|  |  +--------+---------+  |     |  +--------+---------+   |   |
|  |           |            |     |           |             |   |
|  |  EnqueueCompletion()   |     |  PushResponse()         |   |
|  |  响应入队              |---->|           |             |   |
|  |           v            |     |    signal respEventFd   |   |
|  |  +------------------+  |     |           |             |   |
|  |  | ResponseWriter   |  |     |           v             |   |
|  |  |   (响应线程)     |  |     +-----------|-------------+   |
|  |  +--------+---------+  |                 |               |   |
|  |           |            |     +-----------v-------------+ |   |
|  |  PopCompletion()       |     |   RpcClient             | |   |
|  |  取出响应              |     |  +------------------+   | |   |
|  |           |            |     |  | ResponseWorker   |   | |   |
|  |  PushResponse()        |     |  | (poll eventfd唤醒)|   | |   |
|  |  写入共享内存         |---->|  +--------+---------+   | |   |
|  |           |            |     |           |             | |   |
|  |  signal  |            |     |  PopResponse()          | |   |
|  |  eventfd |            |     |           |             | |   |
|  +----------|------------+     |  通知对应RpcFuture     | |   |
|             |                  |           |             | |   |
|             v                  +-----------|-------------+ |   |
|    respCreditEventFd                       v               |   |
|    客户端消费后通知               通知应用层回调              |   |
+--------------------------------+---------------------------+   |
```

### 7.3 背压(Backpressure)机制

当队列满时，系统通过eventfd进行流控：

```
+--------+     +-------------+     +-------------+     +-------------+
| Submit |     |  Request    |     | Dispatcher  |     |   Executor  |
| Worker |     |    Ring     |     |   Thread    |     |  (处理任务) |
+---+----+     +------+------+     +------+------+     +------+------+
    |                 |                   |                   |
    | TryPushRequest()|                   |                   |
    +---------------->+                   |                   |
    |                 |                   |                   |
    | 返回QueueFull   |                   |                   |
    |<----------------+                   |                   |
    |                 |                   |                   |
    | WaitForRequest  |                   |                   |
    | Credit()        |                   |                   |
    | poll(eventfd)   |                   |                   |
    +---------------->+                   |                   |
    |                 |                   |                   |
    | 阻塞等待...     |                   | PopRequest()      |
    |                 |<------------------+                   |
    |                 |                   |                   |
    |                 |                   | Submit到Executor  |
    |                 |                   +------------------>+
    |                 |                   |                   |
    |                 |                   | 处理完成          |
    |                 |                   | Signal reqCredit  |
    |                 |                   | EventFd           |
    |                 |<------------------+                   |
    |                 |                   |                   |
    | poll返回，有容量 |                   |                   |
    |<----------------+                   |                   |
    |                 |                   |                   |
    | 重试PushRequest |                   |                   |
    +---------------->+                   |                   |
    |                 |                   |                   |
    | 成功            |                   |                   |
    |<----------------+                   |                   |
+---+----+     +------+------+     +------+------+     +------+------+
```


---

## 8. 自启与恢复流程

> **本章定位**：本章专注于恢复机制的完整流程和策略配置，是第3.2节的深入展开。

### 8.1 恢复触发条件

```
+------------------+     +------------------+     +------------------+
|   onFailure      |     |   onEngineDeath  |     |     onIdle       |
|  (请求执行失败)   |     |  (引擎崩溃/死亡)  |     |   (空闲超时)     |
+--------+---------+     +--------+---------+     +--------+---------+
         |                        |                        |
         v                        v                        v
+--------+---------+     +--------+---------+     +--------+---------+
| Restart + 200ms  |     | Restart + 200ms  |     |   IdleClose      |
|  (执行超时)      |     |   (默认策略)     |     | (60秒空闲)       |
+------------------+     +------------------+     +------------------+
         |                        |                        |
         v                        v                        v
+-------------------------------------------------------------+
|                      ClientRecoveryState                     |
|  StartRecovery(delayMs) -> Cooldown/Recovering -> Active    |
+-------------------------------------------------------------+
```

### 8.2 完整恢复流程

```
+--------+     +----------+     +----------+     +----------+
|   App  |     | VesClient|     | RpcClient|     |RecoveryState
+---+----+     +-----+----+     +-----+----+     +-----+----+
    |                |                |                |
    | ScanFile()     |                |                |
    +--------------->+                |                |
    |                | InvokeAsync()  |                |
    |                +--------------->+                |
    |                |                |                |
    |                |                | 发现会话断开    |
    |                |                | PeerDisconnected|
    |                |                |                |
    |                |                | HandleEngineDeath
    |                |                +--------------->+
    |                |                |                |
    |                |                | StartRecovery  |
    |                |                | (200ms)        |
    |                |                +--------------->+
    |                |                |                |
    |                |                | 状态:Cooldown  |
    |                |                | 计算cooldownUntilMs
    |                |                |<---------------+
    |                |                |                |
    |                |                | CloseLiveSession
    |                |                | IfSnapshotMatches
    |                |                |                |
    |                |                | pending_.TakeAll
    |                |                | 标记请求失败    |
    |                |                |                |
    |                |                | WaitForCooldown|
    |                |                | ToSettle()     |
    |                |                +--------------->+
    |                |                |                |
    |                |                | 等待200ms...   |
    |                |                |                |
    |                |                | 冷却结束       |
    |                |                |<---------------+
    |                |                |                |
    |                |                | OpenSession()  |
    |                |                | LoadSystemAbility
    |                |                +--------------->
    |                |                |                |
    |                |                |                | 触发SA加载
    |                |                |                +------>
    |                |                |                | (fork+exec)
    |                |                |                |
    |                |                | 返回Bootstrap  |
    |                |                | Handles        |
    |                |                |<---------------+
    |                |                |                |
    |                |                | session_.Attach
    |                |                | 映射共享内存   |
    |                |                |                |
    |                |                | FinalizeSession|
    |                |                | Open(Active)   |
    |                |                +--------------->+
    |                |                |                |
    |                |                | 状态:Active    |
    |                |                |<---------------+
    |                |                |                |
    |                | 继续ScanFile   |                |
    |                |<---------------+                |
    | 返回结果       |                |                |
    |<---------------+                |                |
+---+----+     +-----+----+     +-----+----+     +-----+----+
```

### 8.3 恢复策略配置

```cpp
// ves_client.cpp - BuildRecoveryPolicy()
MemRpc::RecoveryPolicy BuildRecoveryPolicy(const VesClientOptions& options) {
    MemRpc::RecoveryPolicy policy = options.recoveryPolicy;
    
    // 1. 失败恢复策略 - 执行超时后重启
    if (!policy.onFailure) {
        policy.onFailure = [](const MemRpc::RpcFailure& failure) {
            if (failure.status == MemRpc::StatusCode::ExecTimeout) {
                return MemRpc::RecoveryDecision{
                    MemRpc::RecoveryAction::Restart, 
                    DEFAULT_RESTART_DELAY_MS  // 200ms
                };
            }
            return MemRpc::RecoveryDecision{
                MemRpc::RecoveryAction::Ignore, 0
            };
        };
    }
    
    // 2. 引擎死亡恢复策略 - 总是重启
    if (!policy.onEngineDeath) {
        policy.onEngineDeath = [](const MemRpc::EngineDeathReport& report) {
            HILOGW("engine death: session=%llu", report.deadSessionId);
            return MemRpc::RecoveryDecision{
                MemRpc::RecoveryAction::Restart,
                DEFAULT_RESTART_DELAY_MS  // 200ms
            };
        };
    }
    
    // 3. 空闲恢复策略 - 60秒空闲后关闭
    if (!policy.onIdle && options.idleShutdownTimeoutMs > 0) {
        policy.onIdle = [timeout = options.idleShutdownTimeoutMs](uint64_t idleMs) {
            if (idleMs < timeout) {
                return MemRpc::RecoveryDecision{
                    MemRpc::RecoveryAction::Ignore, 0
                };
            }
            return MemRpc::RecoveryDecision{
                MemRpc::RecoveryAction::IdleClose, 0
            };
        };
    }
    
    return policy;
}
```

---

## 9. 总结

### 9.1 核心设计要点

1. **双优先级队列**: 独立的High/Normal请求环，避免普通请求阻塞高优先级请求
2. **零拷贝内联数据**: 小数据(8KB)直接内联在Ring Entry中，无需额外内存分配
3. **EventFd通知机制**: 基于Linux eventfd的轻量级进程间通知，替代轮询
4. **Robust Mutex**: 使用PTHREAD_MUTEX_ROBUST处理客户端崩溃后的锁恢复
5. **Session隔离**: 每个会话有唯一的sessionId，防止过时操作影响新会话
6. **背压机制**: 队列满时通过eventfd进行流控，避免内存无限增长
7. **细粒度恢复策略**: 支持业务自定义的故障恢复决策(onFailure/onIdle/onEngineDeath)

### 9.2 关键性能参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `RING_ENTRY_BYTES` | 8192 | 每个Ring Entry大小 |
| `DEFAULT_HIGH_RING_SIZE` | 8 | 高优先级队列大小 |
| `DEFAULT_NORMAL_RING_SIZE` | 8 | 普通优先级队列大小 |
| `DEFAULT_RESPONSE_RING_SIZE` | 8 | 响应队列大小 |
| `DEFAULT_MAX_REQUEST_BYTES` | 8168 | 最大请求数据(内联) |
| `DEFAULT_MAX_RESPONSE_BYTES` | 8156 | 最大响应数据(内联) |
| `DEFAULT_RESTART_DELAY_MS` | 200 | 默认恢复冷却时间 |
| `idleShutdownTimeoutMs` | 60000 | 空闲关闭超时 |

### 9.3 线程模型说明

| 组件 | 线程数 | 说明 |
|------|--------|------|
| **SubmitWorker** | 1 | 客户端提交线程，循环处理submitQueue_ |
| **ResponseWorker** | 1 | 客户端响应线程，poll响应eventfd |
| **WatchdogThread** | 1 | 客户端看门狗，检测请求超时 |
| **DispatcherThread** | 1 | 服务端分发线程，poll请求eventfd |
| **ResponseWriter** | 1 | 服务端响应写入线程 |
| **ThreadPoolExecutor** | 8+8 | 服务端高优/普通任务线程池（各默认8线程） |

**RequestId 生成**：单调递增的64位整数，每个请求唯一，用于匹配请求和响应。

### 9.4 调试与测试

项目中提供了多种测试工具：

- **样本规则测试**: 使用`crash`、`sleep100`、`virus`等特殊文件名测试各类场景
- **混沌测试**: `VES_ENABLE_CLOSE_SESSION_CHAOS`环境变量模拟关闭会话时的竞态
- **压测工具**: `virus_executor_service_stress_client`进行并发压力测试
- **确定性测试**: DT测试确保恢复逻辑的正确性

---

*文档生成时间: 2026-04-13*
*基于代码版本: memrpc + virus_executor_service*
