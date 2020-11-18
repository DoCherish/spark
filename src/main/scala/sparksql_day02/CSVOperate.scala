package sparksql_day02

import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @Author Do
 * @Date 2020/7/19 22:55
 */
object CSVOperate {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("sparkCSV")

    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    val frame: DataFrame = session
      .read
      .format("csv")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("header", "true")
      .option("multiLine", true)
      .load("file:///D:\\Work\\Code\\spark\\src\\main\\resources\\SparkSQL02")

    frame.createOrReplaceTempView("job_detail")
    // session.sql("select job_name,job_url,job_location,job_salary,job_company,job_experience,job_class,job_given,job_detail,company_type,company_person,search_key,city from job_detail where job_company = '北京无极慧通科技有限公司'  ").show(80)
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "123456")

   frame.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.237.120:3306/mydb?useSSL=false&useUnicode=true&characterEncoding=UTF-8", "mydb.jobdetail_copy", prop)

  }

}
