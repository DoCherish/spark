package sparksql_day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author Do
 * @Date 2020/7/16 21:56
 */
object SparkSDL {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkDSL")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("WARN")

    val rdd1 = sc.textFile("file:///D:\\Work\\Code\\spark\\src\\main\\resources\\person.txt").map(x=>x.split(" "))
    val personRDD: RDD[Person] = rdd1.map(x => Person(x(0), x(1), x(2).toInt))

    import sparkSession.implicits._  // 隐式转换
    val personDF: DataFrame = personRDD.toDF

    //打印schema信息
    personDF.printSchema
    //展示数据
    personDF.show

    sparkSession.stop()
    sc.stop()

  }
  case class Person(id:String,name:String,age:Int)
}
