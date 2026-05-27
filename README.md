# Iceberg Maintenance Tool

纯 Java 的 Iceberg 表存储维护工具。零 Spark/Flink 依赖，基于 JDBC Catalog + S3，可部署为 K8s Sidecar 或 CronJob。

## 功能

| 功能 | 说明 |
|------|------|
| **快照过期 (Snapshots Expiry)** | 双重约束：`expireOlderThan(90d)` + `retainLast(5)`，AND 关系 |
| **分区裁剪扫描 (Partition Pruning)** | `TableScan.filter()` 精确加载指定分区元数据，内存占用降低 90%+ |
| **僵尸文件清理 (Orphan File Cleanup)** | L1 逻辑扫描 → L2 S3 物理比对，闭环清理残留文件 |
| **冷却期保护 (Cooling Period)** | 物理删除要求 `last_modified < Now - 3d`，避让热数据 |
| **URI 协议头对齐** | 自动将 `s3://` 转换为 `s3a://`，消除路径比对失效导致的误删风险 |
| **并行多表处理** | `--all --parallelism N` 同时处理多张表，失败隔离不扩散 |
| **元数据文件清理** | 通过 Iceberg 引用链遍历安全清理过期元数据文件 |

## 架构

```
┌─────────────────────────────────────────────────────┐
│                       CLI                            │
│  IcebergMaintenanceCli                               │
│    expire | scan-orphans | cleanup | list-tables     │
└─────────┬───────────────────────┬───────────────────┘
          │                       │
          ▼                       ▼
┌─────────────────┐   ┌─────────────────────────┐
│   Common 共享层   │   │  ParallelMaintenance    │
│ UriNormalizer    │   │  Executor               │
│ RetentionConfig  │   │  └── CompletableFuture  │
│ PartitionPrefix  │   │      + fixedThreadPool  │
│   Generator      │   └─────────────────────────┘
│ JdbcCatalogConfig│
│ CatalogLister    │
└─────────────────┘
          │
          ▼
┌─────────────────┐   ┌─────────────────────────┐
│   Scan 扫描模块   │   │  Cleanup 清理模块         │
│ PartitionedTable │   │  SnapshotExpiryService  │
│   Scanner        │   │  ┌─ L1 逻辑扫描 ──────┐│
│   └─ planFiles() │   │  │ FileSetBuilder      ││
│     缓存复用     │   │  └────────────────────┘│
│ FileSetBuilder   │   │  ┌─ L2 物理扫描 ──────┐│
│ PartitionPrefix  │   │  │ L2PhysicalScanner  ││
│   Generator      │   │  │  + lastModified    ││
│                  │   │  │    缓存避免 HEAD   ││
│                  │   │  └────────────────────┘│
│                  │   │  ┌─ 孤儿判定 ─────────┐│
│                  │   │  │ data/  → Set 差集  ││
│                  │   │  │ meta/  → 引用链遍历 ││
│                  │   │  └────────────────────┘│
│                  │   │  ┌─ 安全删除 ─────────┐│
│                  │   │  │ DirectoryGuard     ││
│                  │   │  │ CoolingPeriodFilter││
│                  │   │  │ S3 DeleteObjects   ││
│                  │   │  └────────────────────┘│
└─────────────────┘   └─────────────────────────┘
```

## 快速开始

### 前提

- Java 21+
- PostgreSQL 或其他支持 JDBC 的关系型数据库（Iceberg JDBC Catalog 后端）
- S3 兼容对象存储（MinIO / 阿里云 OSS / AWS S3）

### 构建

```bash
mvn clean package -DskipTests
```

生成 fat JAR：`cli/target/iceberg-cli-1.0.0-SNAPSHOT.jar`（~28MB）

### 配置

通过系统属性 `-D` 或环境变量 `ICE_BERG_*` 配置：

| 配置 | 环境变量 | 说明 | 默认值 |
|------|---------|------|--------|
| `jdbc.url` | `ICE_BERG_JDBC_URL` | JDBC 连接串 | **必填** |
| `jdbc.user` | `ICE_BERG_JDBC_USER` | 数据库用户 | **必填** |
| `jdbc.password` | `ICE_BERG_JDBC_PASSWORD` | 数据库密码 | **必填** |
| `warehouse` | `ICE_BERG_WAREHOUSE` | Iceberg 仓库路径 | **必填** |
| `table.name` | `ICE_BERG_TABLE_NAME` | 库名.表名 | 单表模式必填 |
| `table.dataPrefix` | `ICE_BERG_TABLE_DATAPREFIX` | S3 数据目录前缀 | 自动派生 |
| `s3.region` | `ICE_BERG_S3_REGION` | AWS 区域 | `us-east-1` |
| `dryRun` | `ICE_BERG_DRYRUN` | 预览模式 | `true` |
| `coolingPeriodDays` | `ICE_BERG_COOLING_PERIOD_DAYS` | 冷却期天数 | `3` |

### 使用

```bash
# 查看所有命令
java -jar iceberg-cli.jar

# 列出 catalog 中所有表
java -jar iceberg-cli.jar list-tables

# 过期单表快照（dry-run）
java -jar iceberg-cli.jar expire my_db.my_table

# 过期单表快照（实际执行）
java -jar iceberg-cli.jar expire my_db.my_table -DdryRun=false

# 过期所有表（并行，默认 cpu 核数）
java -jar iceberg-cli.jar expire --all

# 过期所有表（限制 4 并发）
java -jar iceberg-cli.jar expire --all --parallelism 4

# 扫描某 namespace 下的表
java -jar iceberg-cli.jar scan-orphans --all --namespace alpha

# 针对某前缀的表做清理
java -jar iceberg-cli.jar cleanup --all --table-prefix trace_ -DdryRun=false

# 按正则匹配全限定表名
java -jar iceberg-cli.jar expire --all --table-pattern "fds_db\\.trace_.*"
```

## 核心流程

### 三阶段清理模型

```
阶段一：快照过期
  SnapshotExpiryService
    expireOlderThan(cutoff) AND retainLast(N)
    cleanExpiredFiles(false)  ← 数据文件留给孤儿检测处理

阶段二：孤儿文件检测
  L1（逻辑扫描）：
    table.newScan().planFiles() → 被引用的文件集合
  
  L2（物理扫描）：
    S3 ListObjectsV2 → 物理文件集合
  
  孤儿判定：
    data/ 文件：    物理 − 引用 = 孤儿  (Set 差集)
    metadata/ 文件：物理 − 引用链 = 孤儿 (MetadataReferenceChainWalker)

阶段三：安全删除
  DirectoryGuard       ← 拒绝根目录文件
  CoolingPeriodFilter  ← lastModified < Now - 3d
  S3 DeleteObjects     ← 每批 1000 个 key
```

### Scan-to-Prefix 链式优化

L1 扫描结果（命中分区列表）直接派生 S3 Prefix，传递给 L2 物理扫描器，代替全 `data/` 前缀扫描：

```
TableScan.filter(event_date < '2026-05-24')
       ↓
[2026-05-20, 2026-05-21, 2026-05-22, 2026-05-23]
       ↓  (PartitionPrefixGenerator)
[data/event_date=2026-05-20/, data/event_date=2026-05-21/, data/event_date=2026-05-22/, data/event_date=2026-05-23/]
       ↓  (L2 并行 ListObjectsV2)
物理文件集合 → 与元数据引用集合比对
```

假设表有 1000 个分区但过期策略只清理 30 天前的旧分区，扫描量减少 97%。

### 性能优化

| 优化 | 说明 |
|------|------|
| `planFiles()` 缓存复用 | `PartitionedTableScanner` 缓存一次扫描结果，`scanDataFiles()` 与 `derivePrefixes()` 共享，避免重复 I/O |
| lastModified 缓存 | L2 扫描时从 `ListObjectsV2` 响应中缓存 `lastModified`，`CoolingPeriodFilter` 优先查缓存，无缓存才退化到 HEAD 请求 |
| 线程池安全关闭 | `L2PhysicalScanner` 使用 `try-finally` + `Future.get()` 收集异常 + `shutdownNow()` 兜底 |
| 并行多表处理 | `ParallelMaintenanceExecutor` 基于 `CompletableFuture` + `fixedThreadPool`，失败隔离不扩散 |

## 模块说明

```
iceberg-maintaince/
├── pom.xml                  # 父 POM
├── common/                  # 共享工具
│   ├── UriNormalizer        # s3:// → s3a:// 归一化
│   ├── RetentionConfig      # 双约束保留策略配置
│   ├── JdbcCatalogConfig    # JDBC Catalog 初始化
│   ├── PartitionPrefixGenerator  # 分区值 → S3 Prefix
│   ├── CatalogLister        # 多表发现
│   ├── TableFilter          # 命名空间/前缀/正则过滤
│   └── TableIdentifierParser # 表名解析
├── scan/                    # 扫描模块
│   ├── PartitionedTableScanner  # 分区裁剪扫描（结果缓存）
│   ├── PartitionFilterBuilder   # 分区谓词构造
│   └── FileSetBuilder       # 引用文件集构建
├── cleanup/                 # 清理模块
│   ├── SnapshotExpiryService    # 快照过期
│   ├── L1LogicalExpiryScanner   # L1 逻辑扫描
│   ├── L2PhysicalScanner        # L2 物理扫描（lastModified 缓存）
│   ├── OrphanScanPipeline       # 孤儿检测管线
│   ├── OrphanFileDetector       # 孤儿文件判定
│   ├── PhysicalDeletionService  # 物理删除（安全过滤）
│   ├── MetadataReferenceChainWalker  # 元数据引用链遍历
│   ├── CoolingPeriodFilter      # 冷却期过滤
│   ├── DirectoryGuard           # 目录守卫
│   └── CleanupReport            # 清理报告
└── cli/                     # CLI 入口
    ├── IcebergMaintenanceCli    # 主入口
    ├── ParallelMaintenanceExecutor  # 并行表执行器
    ├── TableTaskResult          # 单表执行结果
    ├── ExpireCommand            # expire 命令
    ├── ScanOrphansCommand       # scan-orphans 命令
    ├── CleanupCommand           # cleanup 命令
    └── HealthServer             # Sidecar 健康检查
```

## 部署

### Docker

```bash
docker build -t iceberg-maintenance .
docker run --rm \
  -e ICE_BERG_JDBC_URL=jdbc:postgresql://host:5432/iceberg \
  -e ICE_BERG_JDBC_USER=user \
  -e ICE_BERG_JDBC_PASSWORD=pass \
  -e ICE_BERG_WAREHOUSE=s3a://bucket/warehouse \
  -e ICE_BERG_TABLE_NAME=db.table \
  -e ICE_BERG_DRYRUN=true \
  iceberg-maintenance expire
```

### K8s CronJob

```bash
kubectl apply -f k8s-cronjob.yaml
```

每天早上 2:00 执行快照过期，配置在 `k8s-cronjob.yaml` 中。

## 开发

### 测试

```bash
# 单元测试（43 个）
mvn test

# 端到端测试（需本地 MinIO 运行在 localhost:9000）
mvn test -pl cli -Dtest="IcebergMaintenanceE2ETest"
mvn test -pl cli -Dtest="MultiTableE2ETest"

# 全部测试
mvn test
```

### 依赖

- Java 21+
- Iceberg 1.10.1（API + Core）
- AWS SDK v2 (S3)
- Logback
- H2（测试用 JDBC Catalog 后端）

## 设计文档

详细设计文档位于 `openspec/changes/archive/`：

- [proposal.md](openspec/changes/archive/2026-05-27-fds-iceberg-jdbc-maintenance/proposal.md)
- [design.md](openspec/changes/archive/2026-05-27-fds-iceberg-jdbc-maintenance/design.md)
- [ICEBERG_MAINTENANCE_SUMMARY.md](ICEBERG_MAINTENANCE_SUMMARY.md)
