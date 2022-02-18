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