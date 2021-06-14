package sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import utils.SparkContextUtil._

/**
 * @Author Do
 * @Date 2020/11/18 10:56
 * 统计PV
 */
object PvCount {

  def main(args: Array[String]): Unit = {
    val sc: SparkContext = getScLocal("Pv")

    val dataRDD: RDD[String] = sc.textFile("src/main/resources/sparkcore/access.log")
    val pv: Long = dataRDD.count()

    println(s"pv: $pv") // pv: 14619

    sc.stop()
  }

}
