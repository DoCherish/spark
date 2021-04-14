package utils

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import constant.Constants._
import scala.collection.mutable.Map
/**
 * @Author Do
 * @Date 2020/12/5 13:23
 */
package object utils {

  /**
   * 获取命令行参数
   * 格式：env=test,type=B
   *
   * @param args
   * @return
   */
  def getParams(args: Array[String]): Map[String, String] = {
  val map: Map[String, String] = Map()
  if (args.length != 1) {
  throw new Exception("args length is not 1!")
} else {
  args(0).split(",").foreach(item => {
  val itemSplit: Array[String] = item.split("=")
  val key: String = itemSplit(0).trim
  val values: String = itemSplit(1).trim
  map += (key -> values)
})
  map
}


}

  def getKafkaParams(groupId: String, reSet:String): Map[String, Object] = {

  Map(
  ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> BOOTSTRAP_SERVERS,
  ConsumerConfig.GROUP_ID_CONFIG -> groupId,
  ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
  ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
  ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
  ConsumerConfig.AUTO_OFFSET_RESET_CONFIG ->reSet
  )
}

}
