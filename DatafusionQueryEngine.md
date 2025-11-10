# DataFusion SQL 到执行全链路梳理（含文件与函数索引）

本文梳理一条 SQL 从客户端提交到 DataFusion 内部“逻辑计划 → 逻辑优化 → 物理计划生成 → 物理优化 →（基于分区的）分布式计划与分发 → 执行/调度”的完整路径，并在每一步指出核心文件与关键函数，简述其职责与产出。

> 说明：DataFusion 本身是单进程内的向量化执行引擎，“分布式”在本文语境中指基于分区（partitions）的并行/重分区流水线。跨进程的调度/分发不在本工程（DataFusion 核心）提供，通常由上层系统（例如 Ballista）承担。

---

## 1. 客户端 SQL → 逻辑计划 LogicalPlan

入口 API
- 文件：`datafusion/core/src/execution/context/mod.rs`
- 函数：`SessionContext::sql(&self, sql: &str)`
  - 主要功能：高层入口。解析 SQL、构造并验证逻辑计划，然后返回一个 `DataFrame` 封装后续执行。
  - 关键调用：
    - `self.state().create_logical_plan(sql).await?` 生成 `LogicalPlan`
    - `self.execute_logical_plan(plan).await` 处理 DDL/Prepare/Execute 等语句或返回 DataFrame

生成逻辑计划
- 文件：`datafusion/core/src/execution/session_state.rs`
- 函数：`SessionState::create_logical_plan(&self, sql: &str)`
  - 主要功能：把 SQL 字符串解析为 DataFusion AST Statement，再下推到 SQL→Rel 规划器，产出 `LogicalPlan`。
  - 关键调用链：
    - `sql_to_statement(sql, dialect)` 使用 DataFusion SQL Parser 解析（见下）
    - `statement_to_plan(statement)` 使用 `SqlToRel` 规划为 `LogicalPlan`

注意（调用顺序澄清）：
- 你可能看到文中先提到了 `create_logical_plan`，再提到 `statement_to_plan`。这是因为 `create_logical_plan` 是会话层的“总控”方法，它的内部实现顺序正是：先 `sql_to_statement` → 再 `SqlToRel::statement_to_plan` → 得到 `LogicalPlan`。因此从对外 API 视角先看到 `create_logical_plan` 并不表示顺序错误，真实的调用链条在其内部已按“解析 → 规划”的自然顺序执行。

SQL 解析与 SQL→Rel 规划
- SQL Parser：
  - 文件：`datafusion/sql/src/parser.rs`
  - 结构：`DFParserBuilder`、若干 DataFusion 扩展语句（如 EXPLAIN、COPY）
  - 主要功能：基于 `sqlparser` 解析 SQL 为 DataFusion 自有 `Statement`
- SQL→Rel 规划器：
  - 文件：`datafusion/sql/src/planner.rs`、`datafusion/sql/src/query.rs`
  - 结构/函数：
    - `SqlToRel<'_, S>`：SQL 到 `LogicalPlan` 的核心规划器
    - `SqlToRel::statement_to_plan`：将 AST `Statement` 转为 `LogicalPlan`
    - `SqlToRel::query_to_plan`（见 `query.rs`）：处理 `SELECT/Query` 树，构造 `LogicalPlan` 子树（Select/OrderBy/Limit 等）
  - 主要功能：结合 `ContextProvider`（提供 catalog/schema/table 等）把 SQL AST 逐步翻译为 DataFusion 的 `LogicalPlan`

## 2. 逻辑计划分析与优化（Analyzer + Optimizer）

- 文件：`datafusion/core/src/execution/session_state.rs`
- 函数：`SessionState::optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan>`
  - 主要功能：
    1) 先运行 Analyzer（类型推断/校正、函数重写等）；
    2) 再运行 Rule-based Optimizer，对 `LogicalPlan` 进行等价改写与下推。

Analyzer（类型/表达式等规范化）
- 文件：`datafusion/optimizer/*`（Analyzer 规则同目录下，本文不一一展开）

逻辑优化器与规则
- 文件：`datafusion/optimizer/src/optimizer.rs`
- 结构/函数：
  - `Optimizer`：规则列表与执行框架
  - `Optimizer::new()`：内置推荐规则序列（如 `SimplifyExpressions`、`PushDownFilter`、`PushDownLimit`、`ExtractEquijoinPredicate`、`ReplaceDistinctWithAggregate` 等）
  - `OptimizerRule` trait：每条规则实现 `rewrite` 把一个 `LogicalPlan` 改写为等价且更高效的计划
- 主要功能：在不改变语义的前提下对逻辑计划进行投影合并、谓词下推、去重/连接规约、空关系传播等改写，为物理规划创造更好的形状。

## 3. 物理计划生成（LogicalPlan → ExecutionPlan）

- 文件：`datafusion/core/src/physical_planner.rs`
- 主要接口：
  - trait `PhysicalPlanner`：`create_physical_plan(&self, logical_plan, session_state)`
  - 默认实现 `DefaultPhysicalPlanner`
- 顶层函数：`DefaultPhysicalPlanner::create_physical_plan`
  - 步骤：
    1) `create_initial_plan`：自底向上 DFS 把每个 `LogicalPlan` 节点映射为对应 `ExecutionPlan`
    2) `optimize_physical_plan`：调用物理优化器（见下一节）

逻辑→物理节点映射核心
- 函数：`DefaultPhysicalPlanner::map_logical_node_to_physical`
  - 主要功能：对每种 `LogicalPlan` 节点选择并构造相应的物理算子：
    - `TableScan` → Provider 的 `scan()`
    - `Projection` → `ProjectionExec`（文件：`datafusion/physical-plan/src/projection.rs`）
    - `Filter` → `FilterExec`（文件：`datafusion/physical-plan/src/filter.rs`）
    - `Sort` → `SortExec`（文件：`datafusion/physical-plan/src/sorts/sort.rs`）
    - `Aggregate` → `AggregateExec`（Partial → Final/FinalPartitioned 两阶段）
    - `Join` → `HashJoinExec`/`SortMergeJoinExec`/`NestedLoopJoinExec`（根据配置/统计选择）
    - `Repartition`（逻辑重分区）→ `RepartitionExec`（文件：`datafusion/physical-plan/src/repartition/mod.rs`）
    - `Limit` → `LocalLimitExec` + `GlobalLimitExec` 组合
    - `Unnest` → `UnnestExec` 等
  - 辅助函数：
    - `create_physical_expr` / `create_physical_sort_exprs`：把逻辑表达式转为物理表达式
    - 聚合相关：`AggregateExec::try_new`，并按 `target_partitions` 和 `repartition_aggregations` 决定是否做分区化终结

## 4. 物理计划优化（单机“分布式”流水线）

- 文件：`datafusion/physical-optimizer/src/optimizer.rs`
- 结构/函数：
  - `PhysicalOptimizer` 与 `PhysicalOptimizer::new()`：内置推荐规则顺序
  - 代表性规则：
    - `JoinSelection`：基于统计选择 Join 实现
    - `EnforceDistribution`：为满足分布要求插入必要的 `RepartitionExec`
    - `EnforceSorting`：为满足排序要求插入必要的局部排序
    - `CombinePartialFinalAggregate`：合并/规约聚合阶段
    - `FilterPushdown`、`ProjectionPushdown`、`LimitPushdown`：进一步下推
    - `CoalesceBatches`、`CoalesceAsyncExecInput`：批合并/异步输入整合，提升算子效率
    - `TopKAggregation`、`LimitedDistinctAggregation` 等特定场景优化
    - `SanityCheckPlan`：最终合法性与约束检查
- 主要功能：在不改变输出的前提下，插入/调整重分区与排序，选择更优 Join/聚合形态，压缩多余投影/过滤，形成可高效并行的执行图。

## 5. “分布式计划”与分发（基于分区与重分区）

分区/分布的抽象
- 文件：`datafusion/physical-expr/src/partitioning.rs`
- 枚举：`Partitioning`（`RoundRobinBatch(n)`、`Hash(exprs, n)`、`UnknownPartitioning(n)`）
  - 主要功能：描述物理算子输出的分区形态；配合 `Distribution` 与等价类检查 `satisfy()`

重分区算子 RepartitionExec
- 文件：`datafusion/physical-plan/src/repartition/mod.rs`
- 结构/关键方法：
  - `RepartitionExec::try_new(input, Partitioning)`：构造重分区节点
  - 内部状态机：`RepartitionExecState`（`NotInitialized` → `InputStreamsInitialized` → `ConsumingInputStreams`）
    - `ensure_input_streams_initialized(...)`：为每个输入分区创建流
    - `consume_input_streams(...)`：启动后台任务从输入流按 RoundRobin/Hash 分发到输出分区的 channel
  - 特点：必要时保序分发（preserve_order），并通过 `SpawnedTask`/自定义 channel 控制背压与内存占用
- 作用：在物理优化插入点或显式 `Repartition` 节点处，把 N 个输入分区数据按策略分发到 M 个输出分区，为后续 Join/Aggregate 等提供要求的分布

### 5.1 与分布式术语（fragment/fragment instance/pipeline/driver）的对应关系

DataFusion 核心是“单进程内的并行执行引擎”，不显式定义分布式系统常用的“Fragment/Stage/Task”等对象，但可以将其概念与 DataFusion 的构件建立如下对应关系：

- Fragment（阶段/子图）
  - 概念映射：物理计划图中，以“数据重分发/交换边界”为切分点的子图。在 DataFusion 中，这些“边界”通常由 `RepartitionExec`（以及可能的 `CoalescePartitionsExec`、`SortPreservingMerge` 等）体现。
  - 现状：DataFusion 核心不会显式创建 Fragment 对象；若你要做跨进程调度，可把每个“重分区边界到下一次重分区边界之间”的连续子图视为一个 Fragment。

- Fragment Instance（阶段实例）
  - 概念映射：同一 Fragment 在不同分区上的并行实例。在 DataFusion 中，一个 `ExecutionPlan` 子图的“输出分区数”就天然定义了可并行的实例数；对每个分区调用一次 `execute(partition)` 就可认为是启动该 Fragment 的一个实例。
  - 现状：没有独立的 FragmentInstance 类型；“实例”的粒度就是“分区号”。

- Pipeline（流水线）
  - 概念映射：在单个分区内，自上游到下游一条“可流式传递”的连续算子链。可由算子的 `pipeline_behavior`/`EmissionType` 推断：遇到需要“全量输入后再输出”的算子（如 `SortExec` 的 `Final` 发射、某些聚合/窗口的终结阶段）即形成流水线断点；`RepartitionExec` 这样的“跨分区数据交换”也天然形成两个流水线的分界。
  - 现状：没有单独的 Pipeline 对象；它是由 `ExecutionPlan` 的拓扑与各算子的 `EmissionType`/`EvaluationType`/`output_partitioning` 隐式决定。

- Driver / Driver Task（驱动任务）
  - 概念映射：某个分区上的一条流水线的执行单元。在 DataFusion 中，可把“对某个 `ExecutionPlan` 调用 `execute(partition)` 返回的 `Stream`”视为一个 driver 的执行主体。部分算子（如 `RepartitionExec`）会内部 spawn 后台任务，但对上层而言，一个分区的一次 `execute` 即代表一个可独立调度的驱动单元。
  - 现状：无显式 Driver 类型；Tokio 任务与内部 `SpawnedTask` 承载了实际执行。

小结：
- 可据“重分区/合并边界”切 Fragment，据“输出分区”界定 FragmentInstance，据“是否需要全输入”的算子划 Pipeline，据“execute(partition)”视为 Driver。
- 这些概念在 DataFusion 核心中都是“隐式的”，用于单机并行；真正的“跨进程”切分与编排需由上层系统实现。

### 5.2 哪些环节在 DataFusion 核心中不存在/需由外部系统补全

- Fragment/FragmentInstance 的显式对象与编号：缺失。需外部系统（例如 Ballista）在 `ExecutionPlan` 上识别重分区边界，生成带 ID 的 fragment/stage 与其实例。
- 全局调度/分发：缺失。DataFusion 仅提供本进程内并发执行（Tokio runtime）；跨节点的任务分配、资源管理、失败重试等需外部调度器。
- 远程 Exchange/Shuffle：缺失。核心的 `RepartitionExec` 使用进程内通道；跨进程的数据交换（网络传输、shuffle 服务）需外部实现。
- Driver 生命周期与线程/核亲和：DataFusion 无显式 driver 管理，实际执行落在 `execute(partition)` 引发的异步流与内部 spawn 的任务上；更细粒度的调度策略需外部接管。

## 6. 执行与调度（driver/pipeline 视角）

执行主入口（按分区“驱动”）
- 文件：`datafusion/physical-plan/src/execution_plan.rs`
- Trait：`ExecutionPlan`
  - 方法：`execute(partition: usize, context: Arc<TaskContext>) -> SendableRecordBatchStream`
  - 语义：一个 `ExecutionPlan` 有 `output_partitioning().partition_count()` 个独立输出分区。对每个分区调用 `execute(..)` 可并行生成数据流，可把“每个分区的一条上游→下游流水线”视为一个 driver
- 收集与执行辅助：
  - `collect(plan, task_ctx).await`：收集所有分区输出到内存
  - `execute_stream(plan, task_ctx)`：单分区/合并后的顺序流
  - `execute_stream_partitioned(plan, task_ctx)`：返回每个分区的流向量

流水线/调度属性（影响 driver 行为）
- 文件：`datafusion/physical-plan/src/execution_plan.rs`
- 核心枚举：
  - `SchedulingType`：`NonCooperative` / `Cooperative`（是否使用 Tokio 任务预算的协作式调度）
  - `EvaluationType`：`Lazy` / `Eager`（是否由算子主动驱动生产批次；如 Repartition/CoalescePartitions 属于 Eager）
  - `EmissionType`：`Incremental/Final/Both`（增量或终结批的发出模式）
- 作用：每个物理算子通过 `PlanProperties` 声明这些属性，配合 `TaskContext` 与内部异步任务，形成单机内的高并发流水线调度

TaskContext 的来源
- 文件：`datafusion/execution/src/lib.rs`、`datafusion/core/src/execution/session_state.rs`
- 函数：`SessionContext::task_ctx()` / `TaskContext::from(&dyn Session)`
  - 主要功能：封装本次执行的函数注册表、运行时环境、会话配置等，用于 `execute(..)` 期间的资源访问

## 7. 端到端调用链（快速小抄）

- 客户端：`SessionContext::sql`（`datafusion/core/.../context/mod.rs`）
  - → `SessionState::create_logical_plan`（`session_state.rs`）
    - → `sql_to_statement`（`session_state.rs`）
    - → `SqlToRel::statement_to_plan` / `query_to_plan`（`datafusion/sql/...`）
  - → `SessionState::optimize`（Analyzer + Optimizer；`datafusion/optimizer/...`）
  - → `SessionState::create_physical_plan`（`session_state.rs`）
    - → `DefaultPhysicalPlanner::create_initial_plan`（`physical_planner.rs`）
      - → `map_logical_node_to_physical`（构造 `ExecutionPlan` 子树）
    - → `optimize_physical_plan`（`datafusion/physical-optimizer/...`）
  - 执行：`collect/execute_stream/execute_stream_partitioned`（`physical-plan/src/execution_plan.rs`）

## 8. 配置项对“分布式”（分区/并行）的影响（举例）

- `SessionConfig::target_partitions()`：决定默认目标分区数，影响 `AggregateExec` 是否进行分区化终结、是否触发 `Repartition`
- `SessionConfig::repartition_aggregations()` / `repartition_window_functions()`：是否允许相应阶段的重分区
- `ConfigOptions.optimizer.*`：控制逻辑/物理优化的启用与参数，如 `filter_null_join_keys`、`max_passes`、`default_filter_selectivity` 等
  - 读取示例：`session_state.config().options()` 或 `session_state.config_options()`

## 9. 主要物理算子文件速览（按常见类别）

- 过滤/投影/排序
  - `FilterExec`：`datafusion/physical-plan/src/filter.rs`
  - `ProjectionExec`：`datafusion/physical-plan/src/projection.rs`
  - `SortExec`：`datafusion/physical-plan/src/sorts/sort.rs`
- 连接
  - `HashJoinExec`：`datafusion/physical-plan/src/joins/hash_join/exec.rs`
  - `SortMergeJoinExec`：`datafusion/physical-plan/src/joins/sort_merge_join.rs`
  - `NestedLoopJoinExec`：`datafusion/physical-plan/src/joins/nested_loop_join.rs`
- 聚合/窗口
  - `AggregateExec`：`datafusion/physical-plan/src/aggregates/mod.rs`
  - `WindowAggExec` / `BoundedWindowAggExec`：`datafusion/physical-plan/src/windows/*`
- 合并/重分区
  - `RepartitionExec`：`datafusion/physical-plan/src/repartition/mod.rs`
  - `CoalesceBatchesExec`：`datafusion/physical-plan/src/coalesce_batches.rs`
  - `CoalescePartitionsExec`（在 `execution_plan.rs` 中被引用，合并所有分区为 1）
- 其它
  - `UnionExec`：`datafusion/physical-plan/src/union.rs`
  - `UnnestExec`：`datafusion/physical-plan/src/unnest.rs`
  - `ExplainExec` / `AnalyzeExec`：`datafusion/physical-plan/src/explain.rs` / `analyze.rs`

---

## 附：关键函数职责小结

以下列出文中提及的关键函数/方法及其职责（每条只保留要点，便于定位源码）：

- `SessionContext::sql`（`core/.../context/mod.rs`）：SQL 入口；调用 state 生成逻辑计划并返回 DataFrame 或执行 DDL。
- `SessionState::create_logical_plan`（`core/.../session_state.rs`）：解析 SQL → DataFusion Statement → `SqlToRel` 生成 `LogicalPlan`。
- `SqlToRel::statement_to_plan` / `query_to_plan`（`sql/...`）：SQL AST → `LogicalPlan`（Select/OrderBy/Limit/With/CTE 等）。
- `SessionState::optimize`（`core/.../session_state.rs`）：先 Analyzer 再 Optimizer，对 `LogicalPlan` 做等价规则优化。
- `Optimizer::new`（`optimizer/src/optimizer.rs`）：内置逻辑优化规则列表与应用顺序。
- `SessionState::create_physical_plan`（`core/.../session_state.rs`）：对逻辑计划进行物理规划 + 物理优化，产出 `Arc<dyn ExecutionPlan>`。
- `DefaultPhysicalPlanner::create_physical_plan`（`core/src/physical_planner.rs`）：`create_initial_plan` 构建初始物理计划，`optimize_physical_plan` 做物理优化。
- `DefaultPhysicalPlanner::map_logical_node_to_physical`：逐类 `LogicalPlan` 节点构造对应 `ExecutionPlan`（Filter/Aggregate/Join/Repartition/Sort/Limit 等）。
- `PhysicalOptimizer::new`（`physical-optimizer/src/optimizer.rs`）：物理优化规则序列（分布/排序约束、下推、Coalesce、Join 选择、TopK 等）。
- `RepartitionExec::try_new`（`physical-plan/src/repartition/mod.rs`）：构造重分区物理算子。
- `RepartitionExecState::{ensure_input_streams_initialized, consume_input_streams}`：初始化/消费输入流并把批次发往各输出分区。
- `ExecutionPlan::execute`（`physical-plan/src/execution_plan.rs`）：对指定分区返回一个可异步拉取的 `RecordBatch` 流（driver 级流水线）。
- `collect/execute_stream/execute_stream_partitioned`：便捷执行/收集接口，启动所有分区的执行。
- `SchedulingType/EvaluationType/EmissionType`（`physical-plan/src/execution_plan.rs`）：调度/驱动/输出模式标注，影响算子在 Tokio 任务中的执行行为。

---

如需把本文作为代码阅读的“导览图”，建议从 `SessionContext::sql`/`SessionState::create_logical_plan` 出发，按照第 7 节的调用链，在各文件中 Trace 至 `map_logical_node_to_physical` 与 `RepartitionExec`，再回看 `PhysicalOptimizer` 的规则如何插入/调整分布与排序，即可完整把握一条 SQL 的端到端生命周期。
