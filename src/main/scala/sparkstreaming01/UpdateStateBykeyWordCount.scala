package sparkstreaming01

import jdk.nashorn.internal.runtime.options.LoggingOption.LoggerInfo
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author Do
 * @Date 2021/1/6 15:53
 * 使用updateStateByKey，用于记录历史数据，跨批次维护状态：
 *     1 定义状态，状态可以是一个任意的数据类型。
 *     2 定义状态更新函数，用此函数阐明如何使用之前的状态和来自输入流的新值对状态进行更新。
 *     3 配置checkpoint，保存状态。
 *
 * sparkStreaming接受socket数据，使用updateStateByKey，实现所有批次的单词次数累加
 */
object UpdateStateBykeyWordCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateBykeyWordCount")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("src/main/resources/ckp")

    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01",9999)

    val rst: DStream[(String, Int)] = socketTextStream
      .flatMap(_.split(" "))
      .map((_, 1))
      .updateStateByKey(MyUpdateStateFunc)

    rst.print()

    ssc.start()
    ssc.awaitTermination()

  }

  /**
   *
   * @param currentValue  当前批次中每一个单词出现的所有的1
   * @param historyValues 之前批次中每个单词出现的总次数,Option类型表示存在或者不存在,Some表示存在有值，None表示没有
   * @return
   */
  def MyUpdateStateFunc(currentValue:Seq[Int], historyValues:Option[Int]): Option[Int] = {
    val newValue: Int = currentValue.sum + historyValues.getOrElse(0)
    Some(newValue)

  }

}
