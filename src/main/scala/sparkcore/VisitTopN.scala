package sparkcore

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.SparkCoreUtil.getSc

/**
 * @Author Do
 * @Date 2020/11/18 12:06
 * 统计访问最多前五位的URL
 */
object VisitTopN {

  def main(args: Array[String]): Unit = {
    val sc: SparkContext = getSc("VisitTopN", "local[2]", "WARN")

    val dataRDD: RDD[String] = sc.textFile("src/main/resources/sparkcore/access.log")

    val top5: Array[(String, Int)] = dataRDD.filter(_.split(" ").length > 10)
      .map(_.split(" ")(10))
      .filter(_.startsWith("\"http"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(5)

    var n = 1
    println(s"top5:")
    top5.foreach(item => {
      println(s"排名第${n}的url: ${item._1}，访问次数：${item._2} ")
      n += 1
    })

    //top5:
    //排名第1的url: "http://blog.fens.me/category/hadoop-action/"，访问次数：547
    //排名第2的url: "http://blog.fens.me/"，访问次数：377
    //排名第3的url: "http://blog.fens.me/wp-admin/post.php?post=2445&action=edit&message=10"，访问次数：360
    //排名第4的url: "http://blog.fens.me/r-json-rjson/"，访问次数：274
    //排名第5的url: "http://blog.fens.me/angularjs-webstorm-ide/"，访问次数：271

    sc.stop()
  }

}
