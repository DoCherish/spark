package sparkstreaming.kafka.exactlyonce

import com.typesafe.scalalogging.LazyLogging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import constant.Constants._
import org.apache.kafka.clients.consumer.ConsumerRecord
import utils.utils._
import constant.Constants

import scala.collection.mutable
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, HasOffsetRanges, KafkaUtils, LocationStrategies, LocationStrategy, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.config.DBs
import scalikejdbc.{ConnectionPool, DB, _}

/**
 * @Author Do
 * @Date 2021/1/25 20:35
 * SparkStreaming消费kafka，实现Exactly-Once
 *
 * SparkStreaming保证Exactly-Once:
 *     1 Source支持Replay Input:Kafka
 *     2 流计算引擎本身处理能保证Exactly-Once Process:Spark Streaming
 *     3 sink支持幂等或事务更新 Output:Mysql
 *
 * 保证：
 *     1 偏移量自己管理，即enable.auto.commit=false,这里保存在Mysql中
 *     2 使用createDirectStream
 *     3 事务输出: 结果存储与Offset提交在Driver端同一Mysql事务中
 */
object SparkStreamingEOSKafkaMysqlAtomic extends LazyLogging {
  private val sql: String =
    """
      |select
      |   --每分钟
      |   eventTimeMinute,
      |   --每种语言
      |   language,
      |   -- 次数
      |   count(1) pv,
      |   -- 人数
      |   count(distinct(userID)) uv
      |from(
      |   select *, substr(eventTime,0,16) eventTimeMinute from tmpTable
      |) as tmp group by eventTimeMinute,language
          """.stripMargin

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.replace("$", ""))
      .setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //  DBs.setup()
    // 在Driver端创建数据库连接池
    Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(mysqlUrl, mysqlUser, mysqlPsw)

    val topics: Array[String] = Array("test")
    val groupId: String = "spark_exactly_once"
    val kafkaParams: mutable.Map[String, Object] = getKafkaParams(groupId, "earliest")
    // 读取mysql，获取topic partition offset
    val currentOffsets: Map[TopicPartition, Long] = getCurrentOffsets(topics, groupId)
    // 根据读取结果，确定consumerStrategies
    val consumerStrategy: ConsumerStrategy[String, String] = getConsumerStrategies(currentOffsets, kafkaParams, topics)
    // 消费kafka
    println("开始消费kafka数据！")
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      consumerStrategy
    )
    // 数据处理过程
    kafkaStream.foreachRDD((rdd:RDD[ConsumerRecord[String, String]]) => {
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      println("打印每个rdd中offset信息：")
      offsetRanges.foreach((offsetRange: OffsetRange) => {
        logger.info(
          s"topic: ${offsetRange.topic}" + "\t"
          + s"partition: ${offsetRange.partition}" + "\t"
          + s"fromOffset: ${offsetRange.fromOffset}" + "\t"
          + s"untilOffset: ${offsetRange.untilOffset}"
        )
      })

      // 将结果收集到Driver端
      val sparkSession: SparkSession = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import sparkSession.implicits._
      val dataFrame: DataFrame = sparkSession.read.json(rdd.map(_.value()).toDS)
      dataFrame.show(false)
      // 将df注册成临时表
      dataFrame.createOrReplaceTempView("tmpTable")
      val rst: Array[Row] = sparkSession.sql(
        """
          |select
          | id,
          | name,
          | gender,
          | age
          |from tmpTable
          |""".stripMargin
      ).collect()

      // 在Driver端存储数据、提交offset
      // 结果存储与offset提交，在同一事务中原子执行
      // 这里将offset保存到mysql
      println("消费kafka数据落盘mysql")
      DB.localTx(implicit session => {  // 事务提交
        rst.foreach((row: Row) => {
          sql"""
            insert into sparkStreaming_data (id, name, gender, age)
            value (
                ${row.getAs[String]("id")},
                ${row.getAs[String]("name")},
                ${row.getAs[String]("gender")},
                ${row.getAs[String]("age")}
                )
            on duplicate key update name = name, gender = gender, age = age
          """.update.apply()
        })

        // offset提交
        println("开始提交offset到mysql")
        offsetRanges.foreach((offsetRange: OffsetRange) => {
//          val affectedRows: Int =
//            sql"""
//          update kafka_topic_offset set offset = ${offsetRange.untilOffset}
//          where
//            topic = ${topics(0)}
//            and group_id = ${groupId}
//            and partitions = ${offsetRange.partition}
//            and offset = ${offsetRange.fromOffset}
//          """.update.apply()


          val affectedRows: Int =
            sql"""
          insert into kafka_topic_offset(topic, group_id, partitions, offset)
          value (${topics(0)}, ${groupId}, ${offsetRange.partition}, ${offsetRange.fromOffset})
          """.update.apply()

          if (affectedRows != 1) {
            throw new Exception(s"""Commit Kafka Topic: ${topics} Offset Faild!""")
          }
        })

      })

    })

    ssc.start()
    ssc.awaitTermination()

  }

  /**
   * 根据读取mysql的结果，确定消费策略——ConsumerStrategies
   * @param offsets
   * @param kafkaParams
   * @param topics
   * @return
   */
  def getConsumerStrategies(offsets: Map[TopicPartition, Long],
                            kafkaParams: mutable.Map[String, Object],
                            topics: Array[String]
                           ): ConsumerStrategy[String, String] = {
    println("确定ConsumerStrategies")
    if (offsets.isEmpty) {
      ConsumerStrategies.Subscribe(topics, kafkaParams)
    } else {
      ConsumerStrategies.Assign[String, String](offsets.keys, kafkaParams: mutable.Map[String, Object], offsets)
    }

  }

  def getCurrentOffsets(topics: Array[String], groupId: String): Map[TopicPartition, Long] = {
    DB.readOnly(implicit session => {
      sql"select partitions, offset from kafka_topic_offset where topic = ${topics(0)} and group_id = ${groupId}"
        .map(item => {
          val partition: Int = item.get[Int]("partition")
          val offset: Long = item.get[Long]("offset")
          new TopicPartition(topics(0), partition) -> offset // (TopicPartition, Int)
        })
        .list()
        .apply()
        .toMap
    })
  }

}
