package sparkcore

import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import utils.SparkContextUtil.getSc
import constant.Constants._

/**
 * @Author Do
 * @Date 2020/11/26 13:25
 *
 * 针对各个网站来的不同的url，如果域名相同，那么就认为是同一组数据，发送到同一个分区里面去，并且输出到同一个文件里面去进行保存
 */
object SparkPartitioner {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = getSc("SparkBroadCast", "local[2]", "WARN")
    val dataRDD: RDD[String] = sc.textFile(webDataPath, 4)
    val rstRDD: RDD[(String, String)] = dataRDD.map(data => {
      val strings: Array[String] = data.split("@zolen@")
      if (strings.length >= 16) {
        (strings(16), data)
      } else {
        ("http://www.baidu.com", data)
      }
    })

    rstRDD.partitionBy(new MyPartitioner(8)).saveAsTextFile(outParPath)

    sc.stop()

  }

  // 自定义分区器
  class MyPartitioner(numPars: Int) extends Partitioner {
    override def numPartitions: Int = {numPars}

    override def getPartition(key: Any): Int = {
      if (key.toString.startsWith("http")) {
        val domain: String = new java.net.URL(key.toString).getHost
        (domain.hashCode & Integer.MAX_VALUE) % numPars
      } else {
        0
      }
    }
  }

}
