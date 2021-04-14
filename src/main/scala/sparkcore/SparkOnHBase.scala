package sparkcore

import org.apache.hadoop.conf.Configuration
import utils.HbaseUtil.createHbaseConf
import utils.SparkContextUtil.getScLocal
import constant.Constants._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @Author Do
 * @Date 2020/11/28 13:54
 */
object SparkOnHBase {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = getScLocal("SparkOnHBase", "local[2]", "WARN")

    val conf: Configuration = createHbaseConf()
    conf.set(TableInputFormat.INPUT_TABLE, sparkHbase)

    val hbaseContext: HBaseContext = new HBaseContext(sc, conf)
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = hbaseContext.hbaseRDD(TableName.valueOf(sparkHbase), new Scan())

    hbaseRDD.foreach(item => {
      val rowKey: String = item._1.get().toString
      val rst: Result = item._2

      val age: Array[Byte] = rst.getValue("info".getBytes(), "age".getBytes())
      val name: Array[Byte] = rst.getValue("info".getBytes(), "name".getBytes())
      println(rowKey + "\t" + age + "\t" + name)

    })

    sc.stop()

  }

}
