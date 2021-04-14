package sparkcore

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import utils.SparkContextUtil.getScLocal

/**
 * @Author Do
 * @Date 2020/12/11 17:15
 */
object createEmtyDF {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = getScLocal("TwoStageAgg", "local[2]", "WARN")

    val spark: SparkSession = SparkSession.builder().appName("test").getOrCreate()

    val schema: StructType = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true),
        StructField("birth", StringType, true)
      )
    )

    val emptyDF: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    emptyDF.show()

    //+---+----+---+-----+
    //| id|name|age|birth|
    //+---+----+---+-----+
    //+---+----+---+-----+

    spark.stop()


  }

}
