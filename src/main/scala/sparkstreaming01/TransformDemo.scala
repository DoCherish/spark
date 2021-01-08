package sparkstreaming01

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @Author Do
 * @Date 2021/1/6 17:32
 * transform算子：一种转换算子，应用在DStream上，可以用于执行任意的RDD到RDD的转换操作。
 * 使用场景：
 * 获取一个批次（间隔Ts）中的前N名。如:
 *     每隔20秒，获取一个批次中单词出现次数最多的前3
 *     每隔1分钟，获取一个批次中点击次数排前10的页面
 * 黑名单过滤：
 *      从接收到的用户行为数据中，过滤掉黑名单中的用户。
 */
object TransformDemo {

  def main(args: Array[String]): Unit = {

    wordCountTop3Demo()

    blackListDemo()
  }


  def wordCountTop3Demo(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf: SparkConf = new SparkConf().setAppName("TransformWordCount").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(20))

    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)

    val result: DStream[(String, Int)] = socketTextStream.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)

    result.transform(rdd => {
      val sortedRDD: RDD[(String, Int)] = rdd.sortBy(_._2, false)
      val top3: Array[(String, Int)] = sortedRDD.take(3)
      println("------------top3----------start")
      top3.foreach(println)
      println("------------top3----------end")

      sortedRDD
    }).print()

    //------------top3----------start
    //(22,4)
    //(11,4)
    //(33,3)
    //------------top3----------end
    //-------------------------------------------
    //Time: 1609927680000 ms
    //-------------------------------------------
    //(22,4)
    //(11,4)
    //(33,3)
    //(44,2)
    //(55,1)

    ssc.start()
    ssc.awaitTermination()
  }

  def blackListDemo(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf: SparkConf = new SparkConf().setAppName("blackListDemo").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    //定义一个黑名单
    val blackList: List[(String, Boolean)] = List(("小明", true), ("小张", true))
    val blRDD: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(blackList)
    println("黑名单：")
    blRDD.foreach(println)

    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)
    val userLogs: DStream[(String, String)] = socketTextStream.map(line => {
      (line.split(",")(0), line)
    })

    val rstRDD: DStream[String] = userLogs.transform(line => {
      line.leftOuterJoin(blRDD)
        .filter(item => {
          item._2._2.getOrElse(false) != true
        })
        .map(tuple => {
          tuple._2._1
        })
    })

    rstRDD.print()
    //输入：
    //(小张,小张,18,m)
    //(小王,小王,20,f)
    //(小李,小李,16,m)
    //(小赵,小赵,30,f)
    //(小明,小明,25,f)

    //输出：
    //(小王,小王,20,f)
    //(小李,小李,16,m)
    //(小赵,小赵,30,f)


    ssc.start()
    ssc.awaitTermination()

  }


}
