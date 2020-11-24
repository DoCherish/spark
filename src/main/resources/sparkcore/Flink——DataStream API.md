# Flink——DataStream API

![dave-jWM64nraN_Y-unsplash.jpg](C:\Users\86134\Pictures\picture\dave-jWM64nraN_Y-unsplash.jpg.jpg)

关于Flink程序的开发流程和具体案例请参考：[Flink——从零搭建Flink应用](https://blog.csdn.net/duxu24/article/details/105474238)。

## DataSource

Datasource用于Flink程序读取数据，可通过：`StreamExecutionEnvironment.`进行配置。

### 内置数据源

- 文件数据源：
  - `readTextFile(path)`：直接读取文本文件；
  - `readFile(fileInputFormat, path)`：读取指定类型的文件；
  - `readFile(fileInputFormat, path, watchType, interval, pathFilter)`：可指定读取文件的类型、检测文件变换的时间间隔、文件路径过滤条件等。`watchType`分为两种模式：
    - `PROCESS_CONTINUOUSLY`：一旦检测到文件变化，会将改文件**全部内容**加载到Flink。该模式无法实现`Excatly Once`；
    - `PROCESS_ONCE`：一旦检测到文件变化，只会将**变化的数据**加载到Flink。该模式无法实现`Excatly Once`。
- socket数据源：
  - `socketTextStream(hostname, port)`：从Socket端口传入数据；
- 集合数据源：
  - `fromCollection(Seq)`
  - `fromCollection(Iterator)`
  - `fromElements(elements: _*)`
  - `fromParallelCollection(SplittableIterator)`
  - `generateSequence(from, to)`

### 外部数据源

对于流式计算类型的应用，数据大部分都是从外部第三方系统中获取，为此，Flink通过实现`SourceFunction`定义了丰富的第三方数据连接器（支持自定义数据源）：

- [Apache Kafka](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/kafka.html)
- [Amazon Kinesis Streams](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/kinesis.html)
- [RabbitMQ](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/rabbitmq.html)
- [Apache NiFi](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/nifi.html)
- [Twitter Streaming API](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/twitter.html) 
- [Google PubSub](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/pubsub.html)

## DataStream Transformations

|       `Operator`        |                      `Transformation`                      | `Example`                                                    |
| :---------------------: | :--------------------------------------------------------: | :----------------------------------------------------------- |
|           map           |                  DataStream → DataStream                   | dataStream.map { x => x * 2 }                                |
|         flatMap         |                  DataStream → DataStream                   | dataStream.flatMap { str => str.split(" ") }                 |
|         filter          |                  DataStream → DataStream                   | dataStream.filter { _ != 0 }                                 |
|          keyBy          |                  DataStream → KeyedStream                  | dataStream.keyBy("someKey") // Key by field "someKey" <br />dataStream.keyBy(0) // Key by the first element of a Tuple |
|         reduce          |                  KeyedStream → DataStream                  | keyedStream.reduce { _ + _ }                                 |
|          fold           |                  KeyedStream → DataStream                  | val result: DataStream[String] = keyedStream.fold("start")((str, i) => { str + "-" + i }) |
|      aggregations       |                  KeyedStream → DataStream                  | keyedStream.sum(0) <br />keyedStream.sum("key") <br />keyedStream.min(0) <br />keyedStream.min("key") <br />keyedStream.max(0) <br />keyedStream.max("key") <br />keyedStream.minBy(0) <br />keyedStream.minBy("key") <br />keyedStream.maxBy(0) <br />keyedStream.maxBy("key") |
|         window          |                KeyedStream → WindowedStream                | dataStream.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(5))) // Last 5 seconds of data |
|        windowAll        |               DataStream → AllWindowedStream               | dataStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5))) // Last 5 seconds of data |
|      Window Apply       | WindowedStream → DataStream AllWindowedStream → DataStream | windowedStream.apply { WindowFunction }  <br />// applying an AllWindowFunction on non-keyed window stream <br />allWindowedStream.apply { AllWindowFunction } |
|      Window Reduce      |                WindowedStream → DataStream                 | windowedStream.reduce { _ + _ }                              |
|       Window Fold       |                WindowedStream → DataStream                 | val result: DataStream[String] =  windowedStream.fold("start", (str, i) => { str + "-" + i }) |
| Aggregations on windows |                WindowedStream → DataStream                 | windowedStream.sum(0) <br />windowedStream.sum("key") <br />windowedStream.min(0) <br />windowedStream.min("key") <br />windowedStream.max(0) <br />windowedStream.max("key") <br />windowedStream.minBy(0) <br />windowedStream.minBy("key") <br />windowedStream.maxBy(0) <br />windowedStream.maxBy("key") |
|          union          |                  DataStream* → DataStream                  | dataStream.union(otherStream1, otherStream2, ...)            |
|       Window Join       |             DataStream,DataStream → DataStream             | dataStream.join(otherStream)     <br />.where(<key selector>).equalTo(<key selector>)<br />.window(TumblingEventTimeWindows.of(Time.seconds(3)))     <br />.apply { ... } |
|     Window CoGroup      |             DataStream,DataStream → DataStream             | dataStream.coGroup(otherStream)     <br />.where(0).equalTo(1)  <br />.window(TumblingEventTimeWindows.of(Time.seconds(3)))     <br />.apply {} |
|         connect         |          DataStream,DataStream → ConnectedStreams          | someStream : DataStream[Int] = ... <br />otherStream : DataStream[String] = ...  <br />val connectedStreams = someStream.connect(otherStream) |
|    CoMap, CoFlatMap     |               ConnectedStreams → DataStream                | connectedStreams.map(     (_ : Int) => true,     (_ : String) => false ) connectedStreams.flatMap(     (_ : Int) => true,     (_ : String) => false ) |
|          split          |                  DataStream → SplitStream                  | val split = someDataStream.split(   (num: Int) =>     (num % 2) match {       case 0 => List("even")       case 1 => List("odd")     } ) |
|         select          |                  SplitStream → DataStream                  | val even = split select "even" val odd = split select "odd" val all = split.select("even","odd") |
|         iterate         |         DataStream → IterativeStream → DataStream          | initialStream.iterate {   iteration => {     val iterationBody = iteration.map {/*do something*/}     (iterationBody.filter(_ > 0), iterationBody.filter(_ <= 0))   } } |
|   Extract Timestamps    |                  DataStream → DataStream                   | stream.assignTimestamps { timestampExtractor }               |

## DataSink

经过各种数据转换操作之后，形成最终结果数据集。通常情况下，需要将结果输出在外部存储介质或者传输到下游的消息中间件内，在Flink中将DataStream数据输出到外部系统的过程被定义为DataSink操作。可通过：`StreamExecutionEnvironment.`进行配置。

### 内置数据源

- `writeAsText()` / `TextOutputFormat`
- `writeAsCsv(...)` / `CsvOutputFormat`
- `print()` / `printToErr()`
- `writeUsingOutputFormat()` / `FileOutputFormat`
- `writeToSocket`

### 外部数据源

- [Apache Kafka](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/kafka.html)
- [Apache Cassandra](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/cassandra.html)
- [Amazon Kinesis Streams](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/kinesis.html)
- [Elasticsearch](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/elasticsearch.html)
- [Hadoop FileSystem](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/filesystem_sink.html)
- [RabbitMQ](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/rabbitmq.html)
- [Apache NiFi](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/nifi.html)
- [Google PubSub](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/pubsub.html)

