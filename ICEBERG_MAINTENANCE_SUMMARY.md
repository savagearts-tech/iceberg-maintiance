# Iceberg 存储维护设计

## 概述

本项目是一个纯 Java 的 Iceberg 表存储维护工具，通过 **JDBC Catalog**、**分区裁剪扫描**、**两阶段孤儿文件清理**等机制，实现安全、高效、轻量的数据生命周期管理。核心定位：零外部计算引擎依赖，可部署为 K8s Sidecar。

## 架构图

```
┌─────────────────────────────────────────────────────────────┐
│                        CLI (命令行入口)                       │
│  IcebergMaintenanceCli                                      │
│    ├── expire       快照过期                                  │
│    ├── scan-orphans 孤儿文件检测                              │
│    ├── cleanup      清理执行                                  │
│    └── report       报告生成                                  │
└───────────────────┬─────────────────────────────────────────┘
                    │
    ┌───────────────┼───────────────┐
    ▼               ▼               ▼
┌─────────┐  ┌────────────┐  ┌──────────────┐
│ Common  │  │   Scan     │  │   Cleanup    │
│ 共享工具 │  │ 元数据扫描  │  │  清理执行     │
└─────────┘  └────────────┘  └──────────────┘
```

## 模块职责

| 模块 | 核心类 | 职责 |
|------|--------|------|
| **common** | `UriNormalizer`, `RetentionConfig`, `JdbcCatalogConfig`, `PartitionPrefixGenerator`, `TableIdentifierParser` | 共享工具：URI 归一化、配置管理、JDBC 目录初始化、分区路径生成 |
| **scan** | `PartitionedTableScanner`, `FileSetBuilder` | 元数据扫描：分区裁剪、引用文件集构建、S3 Prefix 推导 |
| **cleanup** | `SnapshotExpiryService`, `L1LogicalExpiryScanner`, `L2PhysicalScanner`, `OrphanFileDetector`, `PhysicalDeletionService`, `MetadataReferenceChainWalker`, `CoolingPeriodFilter`, `DirectoryGuard` | 清理执行：快照过期、孤儿检测、物理删除、安全过滤 |
| **cli** | `IcebergMaintenanceCli`, `ExpireCommand`, `ScanOrphansCommand`, `CleanupCommand`, `HealthServer` | 命令行入口、命令编排、Sidecar 健康检查 |

---

## 核心流水线：3 阶段清理模型

整个维护清理流程分为三个阶段，依次执行：

```
阶段一：快照过期 (Snapshot Expiry)
│
▼
阶段二：孤儿文件检测 (Orphan Detection)
│  ├── L1 逻辑扫描 (Iceberg Metadata Scan)
│  └── L2 物理扫描 (S3 ListObjectsV2)
│
▼
阶段三：安全删除 (Physical Deletion)
   ├── DirectoryGuard（目录守卫）
   ├── CoolingPeriodFilter（冷却期过滤）
   └── S3 DeleteObjects（批量删除）
```

---

### 阶段一：快照过期（Snapshot Expiry）

**类**: `SnapshotExpiryService`

#### 双重约束策略 (Dual Retention)

核心逻辑：一个快照被删除当且仅当 **两个条件同时满足**：

```
expireOlderThan(90d)   ← 快照年龄超过 90 天
    AND
retainLast(5)          ← 且不在最近 5 个快照之内
```

实现上直接委托给 Iceberg 内置的 `ExpireSnapshots` API：

```java
table.expireSnapshots()
    .expireOlderThan(cutoffMillis)
    .retainLast(config.retainLast())
    .cleanExpiredFiles(false)    // 数据文件清理交给孤儿文件流水线
    .commit();
```

> `cleanExpiredFiles(false)` 的设计考量：快照过期只清理元数据引用，**不**删除底层数据文件。数据文件的物理清理统一由孤儿检测流水线处理——这是"关注点分离"原则的体现。

**Dry-Run 模式**：`SnapshotExpiryService.dryRun()` 只计算可过期快照数量，不执行实际删除，供运维预览。

---

### 阶段二：孤儿文件检测（Orphan Detection）

这是整个工具最核心的阶段，分为 L1 和 L2 两个子阶段。

#### L1：逻辑扫描

**类**: `L1LogicalExpiryScanner` → `FileSetBuilder`

通过 Iceberg `TableScan` API 扫描所有活跃快照的 manifest 文件，收集当前**被引用的所有数据文件路径**：

```java
try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
    for (FileScanTask task : tasks) {
        files.add(UriNormalizer.normalize(task.file().path().toString()));
    }
}
```

输出：`Set<String>` — 所有被 Iceberg 元数据引用的文件路径（归一化为 `s3a://`）。

#### L2：物理扫描

**类**: `L2PhysicalScanner`

通过 S3 `ListObjectsV2` API 枚举存储后端上的实际文件：

```java
Set<String> physicalFiles = l2.listFiles(prefixes);
```

关键优化：**Scan-to-Prefix 链式优化**

```
TableScan.filter(event_date >= '2026-05-24')
       ↓  (命中分区列表)
[2026-05-24, 2026-05-25, 2026-05-26]
       ↓  (PartitionPrefixGenerator)
[data/event_date=2026-05-24/, data/event_date=2026-05-25/, data/event_date=2026-05-26/]
       ↓  (L2 物理扫描，并行 ListObjectsV2)
合并后的物理文件集合 → 与元数据文件集合比对
```

**`PartitionPrefixGenerator`** 自动从 Iceberg 表元数据中读取分区列名和转换类型（`day`、`hour` 等），生成正确的 S3 Prefix。例如 `day(event_date)` 的分区，S3 目录路径使用 epoch day：

```
day("event_date") partition:
  Iceberg 内部值: 20597  (epoch day)
  S3 目录:       event_date_day=20597/
  withPartitionPath() 输入: event_date_day=2026-05-20
```

> 注意：`day(ts)` 变换的内部值是 epoch day，`partitionToPath()` 输出 epoch day 字符串。因此 S3 目录是 `event_date_day=20597/` 而非 `event_date_day=2026-05-20/`。

#### 孤儿判定

**类**: `OrphanFileDetector`

对 `data/` 和 `metadata/` 采用**不同的判定策略**：

| 目录 | 判定方法 | 原理 |
|------|---------|------|
| `data/` | **Set 差集** | `物理文件集合 − 元数据引用集合 = 孤儿`。数据文件是叶子节点，无嵌套依赖，差集判定安全 |
| `metadata/` | **引用链遍历** | 通过 `TableOperations.current()` → `previousFiles()` 回溯元数据指针链，收集所有活跃元数据文件。不在链中的物理文件才是孤儿 |

```java
// data/ 孤儿判定
Set<String> dataOrphans = physicalFiles.stream()
    .filter(p -> p.contains("/data/"))
    .filter(p -> !normalizedReferenced.contains(p))
    .collect(Collectors.toSet());

// metadata/ 孤儿判定
Set<String> metadataOrphans = physicalFiles.stream()
    .filter(p -> p.contains("/metadata/"))
    .filter(p -> !activeMetadataChain.contains(p))
    .collect(Collectors.toSet());
```

**元数据引用链遍历** (`MetadataReferenceChainWalker`)：

```java
TableMetadata metadata = ops.current();
// 当前元数据文件
active.add(metadata.metadataFileLocation());
// 回溯历史元数据文件
for (MetadataLogEntry entry : metadata.previousFiles()) {
    active.add(entry.file());
}
```

这样做的原因是：metadata/ 文件之间形成有向无环图（DAG），一个正在使用的元数据文件被误删会导致整张表不可读。用 Set 差集会因为路径格式差异而误删，用引用链遍历则精确。

---

### 阶段三：物理删除（Physical Deletion）

**类**: `PhysicalDeletionService`

物理删除前经过两道安全门，按顺序执行：

```
OrphanFileDetector 检测出的孤儿文件列表
    │
    ▼
① DirectoryGuard（目录守卫）
    │  ├── 拒绝根目录文件（s3a://bucket/table/xxx）
    │  ├── 允许 data/ 路径
    │  └── 允许 metadata/ 路径
    │
    ▼
② CoolingPeriodFilter（冷却期过滤）
    │  └── lastModified < Now - 3d 才放行
    │
    ▼
③ S3 DeleteObjects 批量删除（每批最多 1000 个 key）
```

**`DirectoryGuard`** 的安全逻辑：

```java
// 只允许 data/ 和 metadata/ 下的文件
if (afterBucket.startsWith("data/") || afterBucket.startsWith("metadata/")) {
    return true;   // 放行
}
// 根目录文件 → 拒绝
return false;
```

**`CoolingPeriodFilter`** 的安全逻辑：

```java
HeadObjectResponse response = s3Client.headObject(bucket, key);
Instant lastModified = response.lastModified();
boolean eligible = lastModified.isBefore(Instant.now().minus(coolingPeriod));
// coolingPeriod 默认 3 天，可配置
```

两层安全过滤互为补充：
- DirectoryGuard 防止**目录级别**的误删（删到 metadata/ 外的系统文件）
- CoolingPeriodFilter 防止**时间维度**的误删（删除正在被写入的"热数据"）

---

## 关键技术细节

### URI 协议头对齐

**类**: `UriNormalizer`

MinIO SDK 返回的物理 Key 使用 `s3://` 协议头，而 Iceberg 元数据中存储的是 `s3a://` 路径。如果不做对齐，`Set.contains()` 比对会认为 `s3://bucket/a.parquet` ≠ `s3a://bucket/a.parquet`，导致**所有文件都被判定为孤儿**，引发"全删"灾难。

```java
public static String normalize(String uri) {
    if (uri.startsWith("s3://"))
        return "s3a://" + uri.substring(uri.indexOf("://") + 3);
    if (uri.startsWith("s3a://"))
        return uri;
    return "s3a://" + uri;  // bare path
}
```

### JDBC Catalog 配置

**类**: `JdbcCatalogConfig`

直接使用 Iceberg 内置的 `JdbcCatalog`，零封装：

```java
JdbcCatalog catalog = new JdbcCatalog();
catalog.initialize("iceberg_maintenance", Map.of(
    "uri",       "jdbc:postgresql://host/db",
    "warehouse", "s3a://bucket/warehouse",
    "user",      "user",
    "password",  "pass"
));
```

配置项通过环境变量 `ICE_BERG_*` 或 JVM 参数 `-D` 传入。

### 配置项清单

| 配置 | 环境变量 | 说明 | 默认值 |
|------|---------|------|--------|
| `jdbc.url` | `ICE_BERG_JDBC_URL` | JDBC 连接串 | **必填** |
| `jdbc.user` | `ICE_BERG_JDBC_USER` | 数据库用户 | **必填** |
| `jdbc.password` | `ICE_BERG_JDBC_PASSWORD` | 数据库密码 | **必填** |
| `warehouse` | `ICE_BERG_WAREHOUSE` | Iceberg 仓库路径 | **必填** |
| `table.name` | `ICE_BERG_TABLE_NAME` | 数据库名.表名 | **必填** |
| `table.dataPrefix` | `ICE_BERG_TABLE_DATAPREFIX` | S3 数据目录前缀 | **必填** |
| `s3.region` | `ICE_BERG_S3_REGION` | S3 区域 | `us-east-1` |
| `dryRun` | `ICE_BERG_DRYRUN` | 预览模式 | `true` |
| `coolingPeriodDays` | `ICE_BERG_COOLING_PERIOD_DAYS` | 冷却期天数 | `3` |

---

## 测试策略

### 单元测试（43 个）

| 模块 | 测试数 | 覆盖内容 |
|------|--------|---------|
| common | 27 | URI 归一化、分区前缀生成、配置校验、表名解析 |
| cleanup | 16 | 目录守卫、孤儿文件检测、清理报告 |

### 端到端测试（4 个，运行于本地 MinIO + H2）

完整的 E2E 测试覆盖从建表到物理删除的全流程（`IcebergMaintenanceE2ETest`），使用本地 MinIO 作为 S3 存储，H2 内存数据库作为 JDBC Catalog 后端。

---

## 项目文件结构

```
D:.
├── pom.xml                          # Maven 父 POM（模块聚合）
├── common/pom.xml                   # 共享工具模块
│   └── src/main/java/.../
│       ├── UriNormalizer.java       # URI 协议头归一化（s3:// → s3a://）
│       ├── RetentionConfig.java     # 双约束保留策略配置
│       ├── JdbcCatalogConfig.java   # JDBC Catalog 初始化
│       ├── PartitionPrefixGenerator.java  # 分区值 → S3 Prefix
│       └── TableIdentifierParser.java    # 表名解析
├── scan/pom.xml                     # 扫描模块
│   └── src/main/java/.../
│       ├── PartitionedTableScanner.java  # 分区裁剪扫描
│       └── FileSetBuilder.java          # 引用文件集构建
├── cleanup/pom.xml                  # 清理模块
│   └── src/main/java/.../
│       ├── SnapshotExpiryService.java    # 快照过期（双约束）
│       ├── L1LogicalExpiryScanner.java   # L1 逻辑扫描
│       ├── L2PhysicalScanner.java        # L2 物理扫描（S3 ListObjects）
│       ├── OrphanFileDetector.java       # 孤儿文件判定
│       ├── PhysicalDeletionService.java  # 物理删除 + 安全过滤
│       ├── MetadataReferenceChainWalker.java  # 元数据引用链遍历
│       ├── CoolingPeriodFilter.java      # 冷却期过滤
│       ├── DirectoryGuard.java           # 目录守卫
│       └── CleanupReport.java            # 清理报告
├── cli/pom.xml                      # CLI 模块
│   └── src/main/java/.../
│       ├── IcebergMaintenanceCli.java    # 主入口
│       ├── ExpireCommand.java           # expire 命令
│       ├── ScanOrphansCommand.java      # scan-orphans 命令
│       ├── CleanupCommand.java          # cleanup 命令
│       └── HealthServer.java            # Sidecar 健康检查
├── Dockerfile                       # Docker 镜像构建
└── k8s-cronjob.yaml                 # K8s CronJob 部署配置
```
