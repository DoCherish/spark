package sparkcore

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.SparkCoreUtil.getSc
import constant.Constants._

/**
 * @Author Do
 * @Date 2020/11/24 13:37
 */
object SparkBroadCast {
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = getSc("SparkBroadCast", "local[2]", "WARN")

    val productRDD: RDD[String] = sc.textFile(pdtsPath)
    val mapProduct: collection.Map[String, String] = productRDD.map((x => (x.split(",")(0), x))).collectAsMap()
    val broadcast: Broadcast[collection.Map[String, String]] = sc.broadcast(mapProduct)

    val ordersRDD: RDD[String] = sc.textFile(ordersPath)

    val productAndOrdersMsg: RDD[String] = ordersRDD.mapPartitions(eachPar => {
      val broadcastValue: collection.Map[String, String] = broadcast.value

      val rst: Iterator[String] = eachPar.map(eachLine => {
        val key: String = eachLine.split(",")(2)
        val productMsg: String = broadcastValue.getOrElse(key, "")
        "pdtsMsg:" + eachLine + "\t" + "ordersMsg:" + productMsg
      })
      rst
    })

    println(productAndOrdersMsg.collect().toBuffer)
    // 输出：
    // ArrayBuffer(pdtsMsg:1001,20150710,p0001,2	ordersMsg:p0001,xiaomi,1000,2, pdtsMsg:1002,20150710,p0002,3	ordersMsg:p0002,apple,1000,3, pdtsMsg:1002,20150710,p0003,3	ordersMsg:p0003,samsung,1000,4)

    sc.stop()
  }

}
