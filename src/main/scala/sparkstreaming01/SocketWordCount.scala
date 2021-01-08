package sparkstreaming01

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author Do
 * @Date 2020/7/26 21:01
 *
 * sparkStreaming接受socket数据实现单词计数程序
 */
object SocketWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // todo: 1、创建SparkConf对象  注意这里最少给两个线程也就是local[2]  一个线程没法执行
    val sparkConf: SparkConf = new SparkConf().setAppName("TcpWordCount").setMaster("local[2]")

    // todo: 2、创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf,Seconds(1))

    //todo: 3、接收socket数据
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01",9999)
    // val textFileStream: DStream[String] = ssc.textFileStream("hdfs://node01:8020/data")  // 监控hdfs目录

    //todo: 4、对数据进行处理
    val result: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    //todo: 5、打印结果
    result.print()

    //todo: 6、开启流式计算
    ssc.start()
    ssc.awaitTermination()

  }

}
