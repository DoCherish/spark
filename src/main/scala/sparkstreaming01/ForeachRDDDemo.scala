package sparkstreaming01

import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author Do
 * @Date 2021/1/8 19:04
 *
 * DStream foreachRDD(func)：通用的输出操作，将函数func应用于DStream中的每个RDD。
 * 常见的用例之一是把数据写到诸如MySQL的外部数据库中：
 *     1 连接不能写在driver端（序列化问题）。
 *     2 如果写在foreach，则每个RDD中的每一条数据都会创建一个连接，得不偿失
 *     3 建议：foreachRDD后使用foreachPartition，一个分区创建一个连接，批量提交分区数据。
 */
object ForeachRDDDemo {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf: SparkConf = new SparkConf().setAppName("WordCountForeachRDD").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01",9999)

    val rstDStream: DStream[(String, Int)] = socketTextStream.flatMap(lines => {
      lines.split(",")
    })
      .map((_, 1))
      .window(Seconds(10), Seconds(5)) // 每隔5s统计窗口内的数据，窗口长度10秒，滑动时间5秒
      .reduceByKey(_ + _)

    // 将结果保存到mysql
    rstDStream.foreachRDD(rdd => {
      rdd.foreachPartition(eachPar => {
        val conn = DriverManager.getConnection("jdbc:mysql://node03:3306/test", "root", "123456")
        val statement = conn.prepareStatement(s"insert into wordcount(word, count) values (?, ?)")

        conn.setAutoCommit(false)
        eachPar.foreach(record => {
          statement.setString(1, record._1)
          statement.setInt(2, record._2)
          //添加到一个批次
          statement.addBatch()
        })
        // 批量提交该分区所有数据
        statement.executeBatch()
        conn.commit()
        // 关闭资源
        statement.close()
        conn.close()

      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
