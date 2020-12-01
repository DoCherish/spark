package utils

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author Do
 * @Date 2020/11/18 11:29
 */
object SparkContextUtil {

  def getSc(appName: String, master: String, logLevel: String): SparkContext = {
    val sparkConf: SparkConf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel(logLevel)

    sc
  }

}
