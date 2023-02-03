> ## wordcount
>> ### 基础 wordcount 示例
>> class  : StreamingWordCount
>> package: com.yee.study.bigdata.flink1442.wordcount
>
>> ### 带参数 wordcount 示例
>> class  : StreamingWordCountWithArgs
>> package: com.yee.study.bigdata.flink1442.wordcount
>
>> ### 带webui wordcount 示例
>> class  : StreamingWordCount
>> package: com.yee.study.bigdata.flink1442.wordcount
>
> ## Source
>> ### 通过 socketTextStream 方式来读取数据的示例
>> class  : SourceSocket
>> package: com.yee.study.bigdata.flink1442.source.builtin
>
>> ### 通过 readTextFile 方式来读取 本地文件 的示例
>> class  : SourceLocalFile
>> package: com.yee.study.bigdata.flink1442.source.builtin
>
>> ### 通过 readTextFile 方式来读取 HDFS文件 的示例
>> class  : SourceHDFSFile
>> package: com.yee.study.bigdata.flink1442.source.builtin
>
>> ### 通过 fromCollection 方式来读取 Collection 的示例
>> class  : SourceCollection
>> package: com.yee.study.bigdata.flink1442.source.builtin
>
>> ### 自定义读取mysql的数据源 示例
>> class  : SourceMySQL
>> package: com.yee.study.bigdata.flink1442.source.userdefine
>
>> ### 自定义无并行度的数据源 示例
>> class  : SourceNoParallel
>> package: com.yee.study.bigdata.flink1442.source.userdefine
>
>> ### 自定义带并行度的数据源 示例
>> class  : SourceParallel
>> package: com.yee.study.bigdata.flink1442.source.userdefine
>
> ## Transform
>> ### Map + Filter 示例
>> class  : TransformMapFilter
>> package: com.yee.study.bigdata.flink1442.transform
>
>> ### Union 示例
>> class  : TransformUnion
>> package: com.yee.study.bigdata.flink1442.transform
>
>> ### Connect + CoMap 示例
>> class  : TransformConnectCoMapFilter
>> package: com.yee.study.bigdata.flink1442.transform
>
> ## Sink
>> ### Print + PrintErr 示例
>> class  : SinkPrint
>> package: com.yee.study.bigdata.flink1442.sink.builtin
>
>> ### WriteAsText 示例
>> class  : SinkWrite
>> package: com.yee.study.bigdata.flink1442.sink.builtin
>
>> ### SinkFunction 示例
>> class  : SinkPrintSink
>> package: com.yee.study.bigdata.flink1442.sink.userdefine
>
> ## Dataset
>> ### Distinct 算子
>> class  : DatasetTransformDistinct
>> package: com.yee.study.bigdata.flink1442.dataset
>
> ## Broadcast
>> ### Broadcast 示例
>> class  : BroadcastSample
>> package: com.yee.study.bigdata.flink1442.broadcast
>
> ## Accumulator
>> ### 累加器 示例
>> class  : CounterSample
>> package: com.yee.study.bigdata.flink1442.accumulator
>
> ## Partitioner
>> ### 内置 Partitioner 示例
>> class  : StreamPartitionerBuiltin
>> package: com.yee.study.bigdata.flink1442.partitioner
>
>> ### 自定义 Partitioner 示例
>> class  : StreamPartitionerCustom
>> package: com.yee.study.bigdata.flink1442.partitioner
>
> ## State
>> ### Operator State 示例
>> class  : StateSample
>> package: com.yee.study.bigdata.flink1442.state
>
>> ### ValueState 示例
>> class  : ValueStateSample
>> package: com.yee.study.bigdata.flink1442.state
>
>> ### ListState 示例
>> class  : ListStateSample
>> package: com.yee.study.bigdata.flink1442.state
>
>> ### UnionListState 示例
>> class  : ListStateSample
>> package: com.yee.study.bigdata.flink1442.state
>
>> ### MapState 示例
>> class  : MapStateSample
>> package: com.yee.study.bigdata.flink1442.state
>
>> ### ReducingState 示例
>> class  : ReducingStateSample
>> package: com.yee.study.bigdata.flink1442.state
>
>> ### AggregatingState 示例
>> class  : AggregatingStateSample
>> package: com.yee.study.bigdata.flink1442.state
>
>> ### 使用 State 实现 SQL中Join操作的 示例
>> class  : JoinSample
>> package: com.yee.study.bigdata.flink1442.state
>
> ## Window
>> ### 基本的滑动窗口 示例
>> class  : TimeWindowSample
>> package: com.yee.study.bigdata.flink1442.window
>
>> ### 使用自定义窗口处理函数（ProcessWindowFunction） 示例
>> class  : TimeWindowProcessTimeWithProcessWindowFunctionSample
>> package: com.yee.study.bigdata.flink1442.window
>
>> ### 使用ProcessTime的顺序数据源配合滑动窗口 示例
>> class  : TimeWindowProcessTimeWithOrderedSourceSample
>> package: com.yee.study.bigdata.flink1442.window
>
>> ### 使用ProcessTime的乱序数据源配合滑动窗口 示例
>> class  : TimeWindowProcessTimeWithUnorderedSourceSample
>> package: com.yee.study.bigdata.flink1442.window
>
>> ### 使用EventTime的顺序数据源配合滑动窗口 示例
>> class  : TimeWindowEventTimeWithOrderedSourceSample
>> package: com.yee.study.bigdata.flink1442.window
>
>> ### 使用 EventTime（基于当前时间设置 watermark）配合滑动窗口 示例
>> class  : TimeWindowEventTimeWithWatermarkSample1
>> package: com.yee.study.bigdata.flink1442.window
>
>> ### 使用EventTime（基于最大EventTime动态设置 watermark）配合滑动窗口 示例
>> class  : TimeWindowEventTimeWithWatermarkSample2
>> package: com.yee.study.bigdata.flink1442.window
>
>> ### 使用EventTime（基于最大EventTime动态设置 watermark + allowLateness）配合滑动窗口 示例
>> class  : TimeWindowEventTimeWithWatermarkSample3
>> package: com.yee.study.bigdata.flink1442.window
>
>> ### 使用EventTime（基于最大EventTime动态设置 watermark + OutputTag 处理延迟数据）配合滑动窗口 示例
>> class  : TimeWindowEventTimeWithWatermarkSample4
>> package: com.yee.study.bigdata.flink1442.window
>
>> ### 使用EventTime（基于最大EventTime动态设置 watermark，多并行度）配合滑动窗口 示例
>> class  : TimeWindowEventTimeWithWatermarkSample5
>> package: com.yee.study.bigdata.flink1442.window
>
>> ### CountWindow 使用示例
>> class  : CountWindowSample
>> package: com.yee.study.bigdata.flink1442.window
>
>> ### SessionWindow 使用示例
>> class  : SessionWindowSample
>> package: com.yee.study.bigdata.flink1442.window
>
>> ### 基于 State 和自定义 Function 实现类似 SessionWindow 的示例
>> class  : SessionWindowByProcessFunctionSample
>> package: com.yee.study.bigdata.flink1442.window
> 