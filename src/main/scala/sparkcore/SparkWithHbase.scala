package sparkcore

import org.apache.spark.SparkContext
import utils.SparkContextUtil.getScLocal
import utils.HbaseUtil._
import constant.Constants._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

/**
 * @Author Do
 * @Date 2020/11/26 16:59
 */
object SparkWithHbase {

  def main(args: Array[String]): Unit = {
    val sc: SparkContext = getScLocal("SparkBroadCast", "local[2]", "WARN")

    val conf: Configuration = createHbaseConf()

    val rstRDD: RDD[(ImmutableBytesWritable, Result)] =
      sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    rstRDD.foreach(item => {
      val rst: Result = item._2
      val rowKey: String = Bytes.toString(rst.getRow)
      val age: Array[Byte] = rst.getValue("info".getBytes(), "age".getBytes())
      val name: Array[Byte] = rst.getValue("info".getBytes(), "name".getBytes())
      println(rowKey + "\t" + age + "\t" + name)
    })

    sc.stop()

  }

}
