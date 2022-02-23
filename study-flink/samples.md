> ## wordcount
>> ### 基础 wordcount 示例
>> class  : StreamingWordCount
>> package: com.yee.study.bigdata.flink.wordcount
>
>> ### 带参数 wordcount 示例
>> class  : StreamingWordCountWithArgs
>> package: com.yee.study.bigdata.flink.wordcount
>
>> ### 带webui wordcount 示例
>> class  : StreamingWordCount
>> package: com.yee.study.bigdata.flink.wordcount
>
> ## Source
>> ### 通过 socketTextStream 方式来读取数据的示例
>> class  : SourceSocket
>> package: com.yee.study.bigdata.flink.source.builtin
>
>> ### 通过 readTextFile 方式来读取 本地文件 的示例
>> class  : SourceLocalFile
>> package: com.yee.study.bigdata.flink.source.builtin
>
>> ### 通过 readTextFile 方式来读取 HDFS文件 的示例
>> class  : SourceHDFSFile
>> package: com.yee.study.bigdata.flink.source.builtin
>
>> ### 通过 fromCollection 方式来读取 Collection 的示例
>> class  : SourceCollection
>> package: com.yee.study.bigdata.flink.source.builtin
>
>> ### 自定义读取mysql的数据源 示例
>> class  : SourceMySQL
>> package: com.yee.study.bigdata.flink.source.userdefine
> 
>> ### 自定义无并行度的数据源 示例
>> class  : SourceNoParallel
>> package: com.yee.study.bigdata.flink.source.userdefine
>
>> ### 自定义带并行度的数据源 示例
>> class  : SourceParallel
>> package: com.yee.study.bigdata.flink.source.userdefine
>
> ## Transform
>> ### Map + Filter 示例
>> class  : TransformMapFilter
>> package: com.yee.study.bigdata.flink.transform
>
>> ### Union 示例
>> class  : TransformUnion
>> package: com.yee.study.bigdata.flink.transform
>
>> ### Connect + CoMap 示例
>> class  : TransformConnectCoMapFilter
>> package: com.yee.study.bigdata.flink.transform
>
> ## Sink
>> ### Print + PrintErr 示例
>> class  : SinkPrint
>> package: com.yee.study.bigdata.flink.sink.builtin
>
>> ### WriteAsText 示例
>> class  : SinkWrite
>> package: com.yee.study.bigdata.flink.sink.builtin
>
>> ### SinkFunction 示例
>> class  : SinkPrintSink
>> package: com.yee.study.bigdata.flink.sink.userdefine
>
> ## Dataset
>> ### Distinct 算子
>> class  : DatasetTransformDistinct
>> package: com.yee.study.bigdata.flink.dataset 
>
> ## Broadcast
>> ### Broadcast 示例
>> class  : BroadcastSample
>> package: com.yee.study.bigdata.flink.broadcast
>
> ## Accumulator
>> ### 累加器 示例
>> class  : CounterSample
>> package: com.yee.study.bigdata.flink.accumulator
> 
> ## Partitioner
>> ### 内置 Partitioner 示例
>> class  : StreamPartitionerBuiltin
>> package: com.yee.study.bigdata.flink.partitioner
> 
>> ### 自定义 Partitioner 示例
>> class  : StreamPartitionerCustom
>> package: com.yee.study.bigdata.flink.partitioner
> 
> ## State
>> ### Operator State 示例
>> class  : StateSample
>> package: com.yee.study.bigdata.flink.state
> 
>> ### ValueState 示例
>> class  : ValueStateSample
>> package: com.yee.study.bigdata.flink.state
> 
>> ### ListState 示例
>> class  : ListStateSample
>> package: com.yee.study.bigdata.flink.state
> 
>> ### UnionListState 示例 
>> class  : ListStateSample
>> package: com.yee.study.bigdata.flink.state
> 
>> ### MapState 示例
>> class  : MapStateSample
>> package: com.yee.study.bigdata.flink.state 
> 
>> ### ReducingState 示例
>> class  : ReducingStateSample
>> package: com.yee.study.bigdata.flink.state  
>
>> ### AggregatingState 示例
>> class  : AggregatingStateSample
>> package: com.yee.study.bigdata.flink.state 
> 
>> ### 使用 State 实现 SQL中Join操作的 示例
>> class  : JoinSample
>> package: com.yee.study.bigdata.flink.state
> 
> ## Window
>> ### 基本的滑动窗口 示例
>> class  : TimeWindowSample
>> package: com.yee.study.bigdata.flink.window
> 
>> ### 使用自定义窗口处理函数（ProcessWindowFunction） 示例
>> class  : TimeWindowWithProcessWindowFunctionSample
>> package: com.yee.study.bigdata.flink.window
> 
>> ### 自定义顺序数据源配合滑动窗口 示例
>> class  : TimeWindowWIthOrderedSourceSample
>> package: com.yee.study.bigdata.flink.window
> 
> >
>> ### 自定义乱序数据源配合滑动窗口 示例
>> class  : TimeWindowWithUnOrderedSourceSample
>> package: com.yee.study.bigdata.flink.window
> 