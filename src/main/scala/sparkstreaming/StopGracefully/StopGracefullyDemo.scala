package sparkstreaming.StopGracefully

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.StreamingContextUtil._

/**
 * @Author Do
 * @Date 2021/1/14 16:24
 * 借助hdfs优雅关闭流程序
 * 需要关闭流程序时，在hdfs上创建目录："/stopStreaming",即可关闭。
 *
 */
object StopGracefullyDemo {
  private val chechPointPath: String = "src/main/resources/ckp"

  def creatingFunc(): StreamingContext = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf: SparkConf = new SparkConf().setAppName("StopGracefullyDemo").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint(chechPointPath)

    val rst: DStream[(String, Int)] = createSocketSatream(ssc)
      .flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_ + _)

    rst.print()

    ssc
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // 若application首次启动，将创建一个新的StreamingContext实例；
    // 若application从失败中重启，将从checkpoiint目录中导入checkpoint数据重新创建StreamingContext实例。
    val ssc: StreamingContext = StreamingContext.getOrCreate(chechPointPath, creatingFunc)

    // 创建一个线程，传入StreaingContext线程类。
    // 该类内部每隔5s监听一下hdfs上："/stopStreaming"文件夹是否存在，若存在，则优雅关闭程序。
    new Thread(new MonitorStop(ssc)).start()

    ssc.start()
    ssc.awaitTermination()

  }

}
