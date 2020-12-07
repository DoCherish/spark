package sparkcore

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.SparkContextUtil._

/**
 * @Author Do
 * @Date 2020/12/4 13:03
 *
 * 解决数据倾斜的方法：两阶段聚合（局部聚合+全局聚合）
 *
 * 思想：
 * 将原本相同的key通过附加随机前缀的方式，变成多个不同的key。
 * 让原本被一个task处理的数据分散到多个task上去做局部聚合，进而解决单个task处理数据量过多的问题。
 * 接着去除掉随机前缀，再次进行全局聚合，就可以得到最终的结果。
 *
 * 步骤：
 * 1.原始key加随机值：Random.nextInt() + key
 * 2.对数据进行聚合
 * 3.去随机值还原key：key
 * 4.再对数据进行聚合
 */
object TwoStageAgg {
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = getSc("TwoStageAgg", "local[2]", "WARN")

    val array = Array("love", "love", "love", "love", "love", "love", "love", "you", "i")

    val rdd: RDD[String] = sc.parallelize(array, 8)

    val value: RDD[(String, Int)] = rdd.map(x => {
      // 增加随机前缀
      val prefix: Int = (new util.Random).nextInt(3)
      (prefix.toString + "_" + x, 1)
    })
      // 局部聚合
      .reduceByKey(_ + _)
      // 还原key
      .map(x => {
        val x1: String = x._1.split("_")(1)
        val x2: Int = x._2
        (x1, x2)
      })
      // 全局聚合
      .reduceByKey(_ + _)

    value.foreach(x => println(x.toString()))

    sc.stop()

  }

}
