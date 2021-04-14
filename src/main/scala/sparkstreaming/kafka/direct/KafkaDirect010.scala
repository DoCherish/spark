package sparkstreaming.kafka.direct

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.utils.getKafkaParams

import scala.collection.mutable

/**
 * @Author Do
 * @Date 2021/1/16 14:39
 * 基于kafka010版本，使用SparkStreaming的createDirectStream方式消费kafka数据
 */
object KafkaDirect010 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf: SparkConf = new SparkConf().setAppName("KafkaDirect010").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    val topics: Set[String] = Set("spark_test_01")
    val kafkaParams: mutable.Map[String, Object] = getKafkaParams("KafkaDirect010", "eraliest")
    // ssc: StreamingContext,
    // locationStrategy: LocationStrategy,
    // consumerStrategy: ConsumerStrategy[K, V]
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream( // 使用direct接受kafka数据
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    // 要获取到消息消费的offset，这里需要拿到最开始的这个Dstream进行操作
    // 该DStream经过转换后形成的新的DStream不会保存offset
    kafkaStream.foreachRDD(rdd => {
      val value: RDD[String] = rdd.map(_.value())
      value.foreach(line => {
        println(line)
      })
      // 提交offset到kafka中
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    })


    ssc.start()
    ssc.awaitTermination()

  }

}
