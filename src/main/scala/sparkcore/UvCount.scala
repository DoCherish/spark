package sparkcore

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.SparkContextUtil._

/**
 * @Author Do
 * @Date 2020/11/18 11:33
 * 统计UV
 */
object UvCount {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = getSc("Uv", "local[2]", "WARN")

    val dataRDD: RDD[String] = sc.textFile("src/main/resources/sparkcore/access.log")
    val urls: RDD[String] = dataRDD.map(_.split(" ")(0))
    val uv: Long = urls.distinct().count()

    println(s"uv: $uv") // uv: 1050

    sc.stop()

  }

}
