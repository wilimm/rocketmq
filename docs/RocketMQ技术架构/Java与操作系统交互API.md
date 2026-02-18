# Java 与操作系统交互 API

本文档介绍 RocketMQ 中用到的所有与操作系统交互的 Java API，包括文件 I/O、内存管理、网络 I/O、线程调度等。

## 1. 文件系统交互

### 1.1 File 类

File 类提供文件和目录的元数据操作，不涉及文件内容读写：

```java
File file = new File("/data/rocketmq/commitlog/00000000000000000000");

// 元数据操作
file.exists();           // 文件是否存在
file.length();           // 文件大小
file.lastModified();     // 最后修改时间
file.getName();          // 文件名
file.getParent();        // 父目录
file.isDirectory();      // 是否目录

// 目录操作
file.mkdir();            // 创建目录
file.mkdirs();           // 创建多级目录
file.listFiles();        // 列出目录内容

// 文件操作
file.createNewFile();    // 创建空文件
file.delete();           // 删除文件
file.renameTo(dest);     // 重命名
```

**系统调用对照**：

| Java API | 系统调用 | 说明 |
|----------|----------|------|
| `file.exists()` | `stat()` / `access()` | 检查文件状态 |
| `file.mkdir()` | `mkdir()` | 创建目录 |
| `file.delete()` | `unlink()` | 删除文件 |
| `file.renameTo()` | `rename()` | 重命名文件 |
| `file.lastModified()` | `stat()` | 获取文件元数据 |

### 1.2 RandomAccessFile

RandomAccessFile 支持随机位置读写，是 FileChannel 的基础：

```java
RandomAccessFile raf = new RandomAccessFile(file, "rw");

// 定位
raf.seek(position);      // 移动文件指针

// 读写
raf.write(data);         // 写入
raf.read(buffer);        // 读取
raf.writeInt(value);     // 写入 int
raf.readLong();          // 读取 long

// 获取 FileChannel
FileChannel channel = raf.getChannel();
```

### 1.3 FileChannel

FileChannel 是高效的文件读写通道，支持内存映射和零拷贝：

```java
FileChannel channel = new RandomAccessFile(file, "rw").getChannel();

// 普通读写
channel.read(byteBuffer, position);
channel.write(byteBuffer);

// 内存映射（mmap）
MappedByteBuffer mappedBuffer = channel.map(
    FileChannel.MapMode.READ_WRITE,  // 读写模式
    0,                                // 起始位置
    fileSize                          // 映射大小
);

// 强制刷盘
channel.force(false);  // fdatasync()，只刷数据
channel.force(true);   // fsync()，刷数据+元数据

// 零拷贝传输（Linux）
channel.transferFrom(srcChannel, position, count);  // 从其他通道接收
channel.transferTo(position, count, destChannel);   // 传输到其他通道
```

**系统调用对照**：

| Java API | 系统调用 | 说明 |
|----------|----------|------|
| `channel.read()` | `pread()` | 从指定位置读取 |
| `channel.write()` | `pwrite()` | 写入到指定位置 |
| `channel.map()` | `mmap()` | 内存映射 |
| `channel.force(true)` | `fsync()` | 同步数据+元数据 |
| `channel.force(false)` | `fdatasync()` | 仅同步数据 |
| `channel.transferTo()` | `sendfile()` | 零拷贝传输 |

### 1.4 MappedByteBuffer

MappedByteBuffer 通过 mmap 将文件直接映射到内存：

```plain
┌─────────────────────────────────────────────────────────────────────────────┐
│                        mmap 内存映射原理                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   传统文件读写                                                               │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐            │
│   │ 用户缓冲区 │ → │ 内核缓冲区 │ → │ PageCache │ → │   磁盘   │            │
│   └──────────┘    └──────────┘    └──────────┘    └──────────┘            │
│        ↑              ↑                                                   │
│      复制1          复制2    ← 共 2 次数据复制                              │
│                                                                             │
│   mmap 内存映射                                                              │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐                            │
│   │ 用户缓冲区 │ ════════════ │ PageCache │ → │   磁盘   │                │
│   │(MappedByteBuffer)        └──────────┘    └──────────┘                │
│   └──────────┘         ↑                                                   │
│        ↑               │                                                   │
│        └───────────────┘  共享同一块物理内存，零拷贝                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

```java
// 创建映射
MappedByteBuffer mappedBuffer = fileChannel.map(
    FileChannel.MapMode.READ_WRITE, 0, fileSize);

// 写入（直接写入映射内存）
mappedBuffer.put(data);
mappedBuffer.putInt(value);

// 读取
byte b = mappedBuffer.get();
int i = mappedBuffer.getInt();

// 刷盘
mappedBuffer.force();  // 调用 msync()

// 切片视图（零拷贝）
ByteBuffer slice = mappedBuffer.slice();
```

**限制**：
- 单次映射最大 2GB（int 类型限制）
- 映射后文件大小不能改变
- 映射内存不归 GC 管理

### 1.5 Buffer API 详解

Buffer 是 NIO 数据读写的核心组件，RocketMQ 大量使用 ByteBuffer 进行消息存储和索引管理。

#### 1.5.1 Buffer 创建

| API | 内存位置 | 特点 | RocketMQ 使用场景 |
|-----|----------|------|-------------------|
| `ByteBuffer.allocate(size)` | JVM 堆内存 | 分配快，GC 管理；IO 需复制 | 索引单元组装、临时缓冲区 |
| `ByteBuffer.allocateDirect(size)` | 堆外内存 | IO 零拷贝；分配慢，需手动释放 | 内存池、时间轮、高性能 IO |
| `ByteBuffer.wrap(data)` | JVM 堆内存 | 包装已有数组，零分配 | 消息 ID 生成、锁文件写入 |

**源码示例**：

```java
// ConsumeQueue.java:86 - 索引单元 Buffer（堆内存）
this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);

// TransientStorePool.java:51 - 堆外内存池
ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

// TimerWheel.java:71 - 时间轮堆外内存
this.byteBuffer = ByteBuffer.allocateDirect(wheelLength);

// DefaultMessageStore.java:331 - 包装字节数组
lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes()));
```

#### 1.5.2 核心属性与模式切换

Buffer 通过三个指针属性管理数据的读写边界：

```plain
Buffer 结构示意
┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
│ A │ B │ C │ D │ E │ F │ G │   │   │   │
└───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
0   1   2   3   4   5   6   7   8   9
        ↑               ↑               ↑
    position=2      limit=7      capacity=10
```

| 属性 | 含义 | 作用 |
|------|------|------|
| **capacity** | 容量 | Buffer 能容纳的最大数据量，创建后不变 |
| **limit** | 界限 | 第一个不能读/写的位置，即有效数据的边界 |
| **position** | 位置 | 下一个要读/写的位置，会自动移动 |

**写入模式与读取模式**：

```plain
写入模式（flip 之前）              读取模式（flip 之后）
┌───┬───┬───┬───┬───┬───┬───┐    ┌───┬───┬───┬───┬───┬───┬───┐
│ A │ B │ C │ D │ E │ F │   │    │ A │ B │ C │ D │ E │ F │   │
└───┴───┴───┴───┴───┴───┴───┘    └───┴───┴───┴───┴───┴───┴───┘
                    ↑     ↑        ↑               ↑
              position=6 limit=7   position=0   limit=6
              
position 随写入移动                position 随读取移动
limit = capacity                   limit 标记可读边界
```

**模式切换方法**：

| 方法 | 操作 | 典型场景 | RocketMQ 使用 |
|------|------|----------|---------------|
| `flip()` | limit=position, position=0 | 写→读切换 | BatchConsumeQueue, ConsumeQueue, TimerCheckpoint |
| `clear()` | position=0, limit=capacity | 重置 Buffer | CommitLog, TimerMessageStore, DLedgerCommitLog |
| `compact()` | 复制未读数据到开头，position=remaining | 保留未读数据 | AutoSwitchHAConnection, AutoSwitchHAClient |
| `rewind()` | position=0, limit 不变 | 重读数据 | 较少使用 |

**源码示例**：

```java
// BatchConsumeQueue.java:512 - 写入前准备
this.byteBufferItem.flip();
this.byteBufferItem.limit(CQ_STORE_UNIT_SIZE);

// ConsumeQueue.java:703 - 写入完成后切换
this.byteBufferIndex.flip();

// AutoSwitchHAConnection.java:354 - 网络读取后压缩（保留未读数据）
byteBufferRead.compact();
```

#### 1.5.3 数据读写操作

**写入方法**：

| API | 说明 | RocketMQ 使用场景 |
|-----|------|-------------------|
| `putInt/putLong/putShort/putByte` | 写入基本类型 | 索引单元、时间槽、检查点 |
| `put(byte[])` | 写入字节数组 | 消息追加 |
| `put(index, value)` | 绝对位置写入 | 时间轮更新 |

**读取方法**：

| API | 说明 | RocketMQ 使用场景 |
|-----|------|-------------------|
| `getInt/getLong/getShort/getByte` | 读取基本类型 | 索引恢复、消息解析 |
| `get(byte[])` | 读取字节数组 | 消息读取 |

**源码示例**：

```java
// BatchConsumeQueue.java:514-523 - 写入索引单元
this.byteBufferItem.putLong(offset);        // 消息物理偏移量
this.byteBufferItem.putInt(size);           // 消息大小
this.byteBufferItem.putLong(tagsCode);      // 标签哈希
this.byteBufferItem.putLong(storeTime);     // 存储时间
this.byteBufferItem.putLong(msgBaseOffset); // 消息基偏移
this.byteBufferItem.putShort(batchSize);    // 批次大小
this.byteBufferItem.putInt(INVALID_POS);    // 压缩偏移（初始值 -1）
this.byteBufferItem.putInt(0);              // 预留字段

// BatchConsumeQueue.java:189-197 - 恢复时读取索引单元
byteBuffer.position(i);
long offset = byteBuffer.getLong();      // 消息物理偏移量
int size = byteBuffer.getInt();          // 消息大小
byteBuffer.getLong();                    // tagscode
byteBuffer.getLong();                    // timestamp
long msgBaseOffset = byteBuffer.getLong();
short batchSize = byteBuffer.getShort();
```

#### 1.5.4 指针操作

| API | 说明 | RocketMQ 使用场景 |
|-----|------|-------------------|
| `position(int)` | 设置当前位置 | 定位到指定索引单元 |
| `position()` | 获取当前位置 | 记录读取进度 |
| `limit(int)` | 设置边界 | 限制读取范围 |
| `remaining()` | 获取剩余可读字节数 | 检查是否有足够空间 |
| `hasRemaining()` | 判断是否还有剩余数据 | 迭代器判断 |

**源码示例**：

```java
// DefaultMappedFile.java:421-425 - 双重 slice 读取
ByteBuffer byteBuffer = this.mappedByteBuffer.slice();  // 第1次：创建独立视图
byteBuffer.position(pos);                               // 定位到目标位置
ByteBuffer byteBufferNew = byteBuffer.slice();          // 第2次：position 重置为 0
byteBufferNew.limit(size);                              // 设置读取长度

// BatchConsumeQueue.java:860 - 迭代器判断
return sbr.getByteBuffer().hasRemaining();
```

#### 1.5.5 视图操作

**slice() 方法**：创建共享底层数据的子缓冲区视图，不复制数据，实现零拷贝。

```plain
原始 Buffer                          slice() 后的新视图
┌───┬───┬───┬───┬───┬───┬───┐        ┌───┬───┬───┬───┬───┐
│ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │   →    │ 2 │ 3 │ 4 │ 5 │ 6 │
└───┴───┴───┴───┴───┴───┴───┘        └───┴───┴───┴───┴───┘
      ↑               ↑                ↑               ↑
  position=2      limit=7          position=0   limit/capacity=5
```

**双重 slice**：无参 `slice()` 只能从当前 position 开始切片，需要从中间位置切出数据时，需要双重 slice：

```java
// DefaultMappedFile.java:421-425 - 从位置 pos 切出 size 字节
ByteBuffer byteBuffer = this.mappedByteBuffer.slice();  // 第1次：创建独立视图
byteBuffer.position(pos);                               // 定位到目标位置
ByteBuffer byteBufferNew = byteBuffer.slice();          // 第2次：position 重置为 0
byteBufferNew.limit(size);                              // 设置读取长度
```

**为什么不能直接修改原始 Buffer？**

原始 Buffer 是全局共享的，多线程并发读取时会互相干扰：

```plain
线程A：读取 Msg3 (position=3000)
线程B：读取 Msg5 (position=5000)

T1: 线程A 执行 buffer.position(3000)   → position = 3000
T2: 线程B 执行 buffer.position(5000)   → position = 5000（覆盖了线程A的设置）
T3: 线程A 执行 buffer.get()            → 读到了 Msg5 的数据！错误
```

**mark()/reset() 方法**：标记当前位置，之后可以恢复。

```java
// CommitLog.java:1784-1803 - 批量消息写入时标记/恢复
messagesByteBuff.mark();                   // 标记当前位置
// ... 处理消息 ...
if (空间不足) {
    messagesByteBuff.reset();              // 恢复到标记位置
    byteBuffer.reset();                    // 忽略之前追加的消息
}
```

| API | 说明 | RocketMQ 使用场景 |
|-----|------|-------------------|
| `slice()` | 创建共享底层数据的视图 | 消息读取、索引迭代 |
| `mark()` | 标记当前位置 | 批量写入、消息检查 |
| `reset()` | 恢复到标记位置 | 写入失败回滚 |

#### 1.5.6 刷盘操作

| API | 说明 | RocketMQ 使用场景 |
|-----|------|-------------------|
| `mappedByteBuffer.force()` | 强制刷盘（msync） | DefaultMappedFile, TimerWheel, StoreCheckpoint |
| `fileChannel.force(false)` | fdatasync，仅刷数据 | DefaultMappedFile |
| `fileChannel.force(true)` | fsync，刷数据+元数据 | DefaultMessageStore |

**源码示例**：

```java
// DefaultMappedFile.java:307-311 - 刷盘逻辑
if (writeBuffer != null || this.fileChannel.position() != 0) {
    this.fileChannel.force(false);      // fdatasync：使用堆外缓冲区时刷 FileChannel
} else {
    this.mappedByteBuffer.force();       // msync：直接使用 mmap 映射时刷映射内存
}

// StoreCheckpoint.java:87 - 检查点刷盘
this.mappedByteBuffer.force();
```

## 2. 内存管理

### 2.1 Heap Buffer vs Direct Buffer

```plain
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Heap Buffer vs Direct Buffer                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Heap Buffer (堆内存)                                                       │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                        JVM 堆内存                                    │   │
│   │   ┌─────────────┐                                                   │   │
│   │   │ byte[] 数组  │ ←──┐                                              │   │
│   │   └─────────────┘    │  IO 操作时需要复制到堆外                       │   │
│   └──────────────────────│──────────────────────────────────────────────┘   │
│                          ▼                                                  │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                        堆外内存                                      │   │
│   │   ┌─────────────┐                                                   │   │
│   │   │ 临时缓冲区   │ ──→ 磁盘/网络                                     │   │
│   │   └─────────────┘                                                   │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│   Direct Buffer (堆外内存)                                                   │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                        JVM 堆内存                                    │   │
│   │   ┌─────────────┐                                                   │   │
│   │   │ 引用对象     │ (仅持有引用)                                       │   │
│   │   └─────────────┘                                                   │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                          │                                                  │
│                          ▼ 直接访问                                          │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                        堆外内存                                      │   │
│   │   ┌─────────────┐                                                   │   │
│   │   │ DirectBuffer │ ──→ 磁盘/网络 (零拷贝)                            │   │
│   │   └─────────────┘                                                   │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

| 类型 | 内存位置 | 分配方式 | 优缺点 |
|------|----------|----------|--------|
| **Heap Buffer** | JVM 堆内存 | `ByteBuffer.allocate(size)` | 分配快，GC 管理；IO 需复制 |
| **Direct Buffer** | 堆外内存 | `ByteBuffer.allocateDirect(size)` | IO 零拷贝；分配慢，需手动释放 |

### 2.2 Direct Buffer 内存释放

Direct Buffer 不受 GC 直接管理，需要特殊方式释放：

```java
// 分配堆外内存
ByteBuffer directBuffer = ByteBuffer.allocateDirect(1024 * 1024);

// 获取内存地址（需要 sun.misc.Unsafe 或 DirectBuffer 接口）
long address = ((DirectBuffer) directBuffer).address();

// 手动释放（JDK 内部机制）
((DirectBuffer) directBuffer).cleaner().clean();
```

### 2.3 Unsafe 类

Unsafe 提供底层内存操作能力，绕过 JVM 安全检查：

```java
// 获取 Unsafe 实例
Field field = Unsafe.class.getDeclaredField("theUnsafe");
field.setAccessible(true);
Unsafe unsafe = (Unsafe) field.get(null);

// 内存操作
long address = unsafe.allocateMemory(size);  // 分配堆外内存
unsafe.freeMemory(address);                   // 释放内存
unsafe.copyMemory(src, srcOffset, dest, destOffset, length);  // 内存复制

// CAS 操作
unsafe.compareAndSwapInt(obj, offset, expect, update);
unsafe.compareAndSwapLong(obj, offset, expect, update);

// 内存屏障
unsafe.storeFence();   // 写屏障
unsafe.loadFence();    // 读屏障
unsafe.fullFence();    // 全屏障
```

**RocketMQ 应用**：用于高性能的内存操作和并发控制。

## 3. 系统调用（JNA）

### 3.1 JNA 简介

JNA (Java Native Access) 允许 Java 直接调用本地库函数，无需编写 JNI 代码：

```java
public interface LibC extends Library {
    LibC INSTANCE = (LibC) Native.loadLibrary(
        Platform.isWindows() ? "msvcrt" : "c", LibC.class);
    
    int mlock(Pointer addr, NativeLong len);
    int munlock(Pointer addr, NativeLong len);
    int madvise(Pointer addr, NativeLong len, int advice);
}
```

### 3.2 mlock/munlock

锁定内存，防止被 swap 到磁盘：

```java
// 获取 Direct Buffer 的内存地址
long address = ((DirectBuffer) mappedByteBuffer).address();
Pointer pointer = new Pointer(address);

// 锁定内存
LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

// 解锁内存
LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
```

```plain
┌─────────────────────────────────────────────────────────────────────────────┐
│                        mlock 的作用                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   不使用 mlock                                                               │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                        物理内存                                      │   │
│   │   ┌─────────────────────────────────────────────────────────────┐   │   │
│   │   │ MappedByteBuffer (可能被 swap 到磁盘)                        │   │   │
│   │   └─────────────────────────────────────────────────────────────┘   │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                    ↓ swap                                  │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                        磁盘 Swap 分区                                │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│   问题：访问被 swap 的内存时，需要从磁盘加载，产生 IO 延迟                      │
│                                                                             │
│   使用 mlock                                                                 │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                        物理内存                                      │   │
│   │   ┌─────────────────────────────────────────────────────────────┐   │   │
│   │   │ MappedByteBuffer (锁定，不会被 swap) 🔒                       │   │   │
│   │   └─────────────────────────────────────────────────────────────┘   │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│   优势：内存访问始终快速，无 IO 延迟                                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.3 madvise

向操作系统提供内存访问模式建议：

```java
// 预读建议：即将访问这块内存
LibC.INSTANCE.madvise(pointer, new NativeLong(fileSize), LibC.MADV_WILLNEED);

// 顺序访问建议
LibC.INSTANCE.madvise(pointer, new NativeLong(fileSize), LibC.MADV_SEQUENTIAL);

// 随机访问建议
LibC.INSTANCE.madvise(pointer, new NativeLong(fileSize), LibC.MADV_RANDOM);

// 不需要建议：可以释放
LibC.INSTANCE.madvise(pointer, new NativeLong(fileSize), LibC.MADV_DONTNEED);
```

| madvice 参数 | 含义 | 使用场景 |
|-------------|------|----------|
| `MADV_WILLNEED` | 即将访问，预读到内存 | 文件预热 |
| `MADV_SEQUENTIAL` | 顺序访问 | 顺序读取大文件 |
| `MADV_RANDOM` | 随机访问 | 随机读取 |
| `MADV_DONTNEED` | 不再需要，可释放 | 释放缓存 |

## 4. 网络 I/O

### 4.1 SocketChannel / ServerSocketChannel

NIO 提供非阻塞网络 I/O：

```java
// 服务端
ServerSocketChannel serverChannel = ServerSocketChannel.open();
serverChannel.bind(new InetSocketAddress(port));
serverChannel.configureBlocking(false);  // 非阻塞模式

// 客户端
SocketChannel clientChannel = SocketChannel.open();
clientChannel.connect(new InetSocketAddress(host, port));
clientChannel.configureBlocking(false);

// 读写
int read = clientChannel.read(buffer);
int written = clientChannel.write(buffer);
```

### 4.2 Selector 多路复用

Selector 实现单线程管理多个 Channel：

```java
Selector selector = Selector.open();

// 注册 Channel 到 Selector
channel.register(selector, SelectionKey.OP_READ);

// 轮询就绪的 Channel
while (true) {
    int ready = selector.select();  // 阻塞直到有 Channel 就绪
    Set<SelectionKey> keys = selector.selectedKeys();
    
    for (SelectionKey key : keys) {
        if (key.isReadable()) {
            // 处理读事件
            SocketChannel channel = (SocketChannel) key.channel();
            channel.read(buffer);
        }
        if (key.isWritable()) {
            // 处理写事件
        }
    }
    keys.clear();
}
```

**系统调用对照**：

| Java API | 系统调用 | 说明 |
|----------|----------|------|
| `Selector.open()` | `epoll_create()` | 创建 epoll 实例 |
| `channel.register()` | `epoll_ctl()` | 注册文件描述符 |
| `selector.select()` | `epoll_wait()` | 等待事件 |

**RocketMQ 应用**：RocketMQ 使用 Netty 框架，底层基于 NIO Selector 实现高性能网络通信。

## 5. 线程与并发

### 5.1 Thread 类

Java 线程映射到操作系统原生线程：

```java
Thread thread = new Thread(() -> {
    // 线程执行逻辑
});
thread.start();   // 创建并启动线程
thread.join();    // 等待线程结束
thread.interrupt();  // 中断线程
```

**系统调用对照**：

| Java API | 系统调用 | 说明 |
|----------|----------|------|
| `thread.start()` | `pthread_create()` | 创建线程 |
| `thread.join()` | `pthread_join()` | 等待线程结束 |
| `Thread.sleep()` | `nanosleep()` | 线程休眠 |
| `Thread.yield()` | `sched_yield()` | 让出 CPU |

### 5.2 synchronized

synchronized 通过操作系统互斥锁实现：

```java
// 对象锁
synchronized (obj) {
    // 临界区
}

// 方法锁
public synchronized void method() {
    // 临界区
}
```

**实现原理**：
- 偏向锁：无竞争时，不加锁
- 轻量级锁：CAS 尝试获取
- 重量级锁：操作系统互斥锁（mutex）

### 5.3 CAS 与原子类

CAS (Compare-And-Swap) 是无锁并发的基础：

```java
// 使用 AtomicInteger
AtomicInteger counter = new AtomicInteger(0);
counter.incrementAndGet();           // i++
counter.compareAndSet(expect, update);  // CAS

// 使用 Unsafe 直接操作
Unsafe unsafe = ...;
unsafe.compareAndSwapInt(obj, offset, expect, update);
```

**系统调用**：CAS 通过 CPU 指令（x86 的 `cmpxchg`）实现，不是系统调用。

### 5.4 Lock 接口

ReentrantLock 提供比 synchronized 更灵活的锁：

```java
ReentrantLock lock = new ReentrantLock();

lock.lock();
try {
    // 临界区
} finally {
    lock.unlock();
}

// 尝试获取锁
if (lock.tryLock(1, TimeUnit.SECONDS)) {
    try {
        // 临界区
    } finally {
        lock.unlock();
    }
}
```

**实现原理**：基于 AQS (AbstractQueuedSynchronizer)，使用 CAS + Park 实现。

### 5.5 LockSupport

LockSupport 提供线程阻塞/唤醒能力：

```java
// 阻塞当前线程
LockSupport.park();
LockSupport.parkNanos(nanos);
LockSupport.parkUntil(deadline);

// 唤醒线程
LockSupport.unpark(thread);
```

**系统调用对照**：

| Java API | 系统调用 | 说明 |
|----------|----------|------|
| `LockSupport.park()` | `pthread_cond_wait()` | 线程等待 |
| `LockSupport.unpark()` | `pthread_cond_signal()` | 唤醒线程 |

## 6. 时间相关

### 6.1 System.currentTimeMillis()

获取当前时间戳（毫秒）：

```java
long timestamp = System.currentTimeMillis();
```

**系统调用**：Linux 上调用 `clock_gettime(CLOCK_REALTIME)` 或 `gettimeofday()`。

### 6.2 System.nanoTime()

获取高精度时间（纳秒），适合测量时间间隔：

```java
long start = System.nanoTime();
// ... 操作 ...
long elapsed = System.nanoTime() - start;  // 耗时（纳秒）
```

**注意**：`nanoTime()` 不保证与墙上时钟一致，仅用于测量时间间隔。

## 7. API 与系统调用总览

### 7.1 文件系统

| Java API | 系统调用 | 说明 |
|----------|----------|------|
| `File.exists()` | `stat()` / `access()` | 检查文件状态 |
| `File.mkdir()` | `mkdir()` | 创建目录 |
| `File.delete()` | `unlink()` | 删除文件 |
| `FileChannel.read()` | `pread()` | 从指定位置读取 |
| `FileChannel.write()` | `pwrite()` | 写入到指定位置 |
| `FileChannel.map()` | `mmap()` | 内存映射 |
| `MappedByteBuffer.force()` | `msync()` | 同步映射内存到磁盘 |
| `FileChannel.force(true)` | `fsync()` | 同步数据+元数据 |
| `FileChannel.force(false)` | `fdatasync()` | 仅同步数据 |
| `FileChannel.transferTo()` | `sendfile()` | 零拷贝传输 |

### 7.2 内存管理

| Java API | 系统调用 | 说明 |
|----------|----------|------|
| `ByteBuffer.allocateDirect()` | `malloc()` | 分配堆外内存 |
| `Unsafe.allocateMemory()` | `malloc()` | 分配堆外内存 |
| `Unsafe.freeMemory()` | `free()` | 释放堆外内存 |
| JNA `mlock()` | `mlock()` | 锁定内存 |
| JNA `munlock()` | `munlock()` | 解锁内存 |
| JNA `madvise()` | `madvise()` | 内存访问建议 |

### 7.3 网络 I/O

| Java API | 系统调用 | 说明 |
|----------|----------|------|
| `SocketChannel.open()` | `socket()` | 创建套接字 |
| `channel.bind()` | `bind()` | 绑定地址 |
| `channel.connect()` | `connect()` | 建立连接 |
| `channel.read()` | `recv()` / `recvfrom()` | 接收数据 |
| `channel.write()` | `send()` / `sendto()` | 发送数据 |
| `Selector.select()` | `epoll_wait()` | 多路复用等待 |

### 7.4 线程与并发

| Java API | 系统调用 | 说明 |
|----------|----------|------|
| `Thread.start()` | `pthread_create()` | 创建线程 |
| `Thread.join()` | `pthread_join()` | 等待线程结束 |
| `Thread.sleep()` | `nanosleep()` | 线程休眠 |
| `synchronized` (重量级) | `pthread_mutex_*` | 互斥锁 |
| `LockSupport.park()` | `pthread_cond_wait()` | 线程等待 |
| `LockSupport.unpark()` | `pthread_cond_signal()` | 唤醒线程 |
| CAS | CPU 指令 `cmpxchg` | 原子比较交换 |

## 8. RocketMQ 应用总结

```plain
┌─────────────────────────────────────────────────────────────────────────────┐
│                    RocketMQ 中的 Java 与操作系统交互                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   消息存储层                                                                 │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │ CommitLog                                                            │   │
│   │   ├── MappedByteBuffer (mmap) → 零拷贝写入                           │   │
│   │   ├── force() → 持久化刷盘                                           │   │
│   │   ├── slice() → 零拷贝读取                                           │   │
│   │   └── mlock() → 防止内存被 swap                                      │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│   内存管理层                                                                 │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │ TransientStorePool                                                   │   │
│   │   ├── DirectBuffer → 堆外写入缓冲区                                  │   │
│   │   └── mlock() → 锁定缓冲区，保证写入性能                              │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│   网络通信层                                                                 │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │ Netty (基于 NIO)                                                     │   │
│   │   ├── SocketChannel → 非阻塞网络 I/O                                 │   │
│   │   ├── Selector → 多路复用                                            │   │
│   │   └── DirectBuffer → 零拷贝网络传输                                  │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│   并发控制层                                                                 │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │ 线程模型                                                             │   │
│   │   ├── ReentrantLock → 写入互斥                                       │   │
│   │   ├── CAS → 无锁并发                                                 │   │
│   │   └── Thread Pool → 异步处理                                         │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**核心优化点**：

| 优化点 | 技术 | 效果 |
|--------|------|------|
| **零拷贝写入** | mmap / DirectBuffer | 减少用户态↔内核态数据复制 |
| **零拷贝读取** | slice() | 避免消息数据复制 |
| **内存锁定** | mlock | 防止关键内存被 swap |
| **文件预热** | madvise(WILLNEED) | 预加载文件到内存 |
| **异步刷盘** | 后台线程 force() | 写入不阻塞 |
| **多路复用** | Selector/epoll | 单线程处理大量连接 |
| **无锁并发** | CAS | 减少锁竞争 |
