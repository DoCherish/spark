package sparkstreaming.kafka.direct

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
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
 *
 *
 */
object ConsumerSpecifiedOffset {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf: SparkConf = new SparkConf().setAppName("KafkaDirect010").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val topics: Set[String] = Set("spark_test_01")
    val groupId: String = "KafkaDirect010_02"
    val kafkaParams: mutable.Map[String, Object] = getKafkaParams(groupId, "earliest")

    val currentOffsets: mutable.HashMap[TopicPartition, Long] = new mutable.HashMap[TopicPartition, Long]()

    currentOffsets.put(new TopicPartition("spark_test_01", 0), 111)
    currentOffsets.put(new TopicPartition("spark_test_01", 1), 222)
    currentOffsets.put(new TopicPartition("spark_test_01", 2), 333)

    val locationStrategies: LocationStrategy = LocationStrategies.PreferConsistent
    // topics: Iterable[jl.String],
    // kafkaParams: collection.Map[String, Object],
    // offsets: collection.Map[TopicPartition, Long]
    val consumerStrategies: ConsumerStrategy[String, String] = ConsumerStrategies
      .Subscribe[String, String](
        topics,
        kafkaParams,
        currentOffsets)
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      locationStrategies,
      consumerStrategies
    )

    kafkaStream.foreachRDD(rdd => {
      val value: RDD[String] = rdd.map(_.value())
      value.foreach(line => {
        println(line)
      })
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    })

    ssc.start()
    ssc.awaitTermination()

  }

}
