package sparkcore

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.SparkContextUtil.getScLocal

/**
 * @Author Do
 * @Date 2020/12/7 15:52
 *
 * 解决数据倾斜的方法：将reduce join转为map join
 *
 * 适用场景：
 * 在对RDD使用join类操作，或者是在Spark SQL中使用join语句时，且join操作中的一个RDD或表的数据量比较小，一个大表和一个小表join。
 *
 * 思想：
 * 不使用join算子进行连接操作，而使用Broadcast变量与map类算子实现join操作，进而完全规避掉shuffle，彻底避免数据倾斜的发生和出现。
 *
 * 将较小RDD中的数据直接通过collect算子拉取到Driver端的内存中来，然后对其创建一个Broadcast变量，
 * 接着对另一个RDD执行map类算子，在算子函数内，从Broadcast变量中获取较小RDD的全量数据，与当前RDD的每一条数据按照连接key进行比对，
 * 如果连接key相同的话，那么就将两个RDD的数据用你需要的方式连接起来。
 *
 * 普通的join是会走shuffle过程的，而一旦shuffle，就相当于会将相同key的数据拉取到一个shuffle read task中再进行join，
 * 此时就是reduce join。但是如果一个RDD是比较小的，则可以采用广播小RDD全量数据+map算子来实现与join同样的效果，
 * 也就是map join，此时就不会发生shuffle操作，也就不会发生数据倾斜。
 */
object MapJoin {
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = getScLocal("TwoStageAgg", "local[2]", "WARN")

    val lista: Array[(String, String)] = Array(
      // pid,amount
      Tuple2("01", "1"),
      Tuple2("02", "2"),
      Tuple2("03", "3"),
      Tuple2("01", "4"),
      Tuple2("02", "5"),
      Tuple2("03", "6")
    )
    //数据量小一点
    val listb: Array[(String, String)] = Array(
      // pid,pname
      Tuple2("01", "小米"),
      Tuple2("02", "华为"),
      Tuple2("03", "苹果")
    )
    val listaRDD: RDD[(String, String)] = sc.parallelize(lista)
    val listbRDD: RDD[(String, String)] = sc.parallelize(listb)
    // 若使用reduce join
    val rstRDD1: RDD[(String, (String, String))] = listaRDD.join(listbRDD)

    // 使用map join
    val listbBoradcast: Broadcast[Array[(String, String)]] = sc.broadcast(listbRDD.collect())
    val rstRDD2: RDD[(String, (String, String))] = listaRDD.map(x => {
      val key: String = x._1
      val value: String = x._2
      val map: Map[String, String] = listbBoradcast.value.toMap
      println((key, (map.getOrElse(key, ""), value)))
      (key, (map.getOrElse(key, ""), value))
    })

    rstRDD2.collect()

    sc.stop()

  }

}
