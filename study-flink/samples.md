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