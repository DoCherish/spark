package utils

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author Do
 * @Date 2020/11/18 11:29
 */
object SparkContextUtil {

  def getScLocal(appName: String): SparkContext = {
    val sparkConf: SparkConf = new SparkConf().setAppName(appName).setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    sc
  }

}
