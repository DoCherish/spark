package sparkcore

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{JdbcRDD, RDD}
import utils.SparkCoreUtil.getSc
import constant.Constants._

/**
 * @Author Do
 * @Date 2020/11/18 16:01
 *
 *       读取mysql数据库当中的招聘数据，然后进行数据统计分析
 *       1> 求取每个搜索关键字下的职位数量，并将结果入库mysql，注意：实现高效入库
 */
object JdbcOperate {

  private val sql1: String = "SELECT * FROM jobdetail_copy WHERE job_id >= ? AND job_id <= ?"
  private val sql2: String = "insert into job_count(search_name, job_num) values (?, ?)"

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = getSc("JdbcOperate", "local[2]", "WARN")

    val getConn: () => Connection = () => DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPsw)

    val jdbcRDD: JdbcRDD[jobDetail] = getJdbcRDD(sc, getConn, sql1, 1, 100, 1)

    val rstRDD: RDD[(String, Int)] = jdbcRDD
      .filter(!_.search_key.isEmpty)
      .groupBy(_.search_key)
      .map(x => (x._1, x._2.size))
      .sortBy(_._2, false)
    // 分区内排序。若需要全局排序：方法1 设置JdbcRDD的numPartitions=1，方法2 .collect

    // 打印RDD的组成
    println(rstRDD.toDebugString)
    rstRDD.foreach(println(_))

    // 将统计结果写回mysql
    rstRDD.foreachPartition(eachPar => {
      // 一个分区创建一个数据库连接
      val conn: Connection = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPsw)
      conn.setAutoCommit(false)
      val statement: PreparedStatement = conn.prepareStatement(sql2)

      eachPar.foreach(record => {
        statement.setString(1, record._1)
        statement.setInt(2, record._2)
        statement.addBatch()   // 添加到一个批次当中
      })

      statement.executeBatch()  //批量提交该分区所有数据

      conn.commit()
      statement.close()

    })


    sc.stop()

  }

  case class jobDetail(job_id: String, job_name: String, job_url: String, job_location: String, job_salary: String,
                       job_company: String, job_experience: String, job_class: String, job_given: String,
                       job_detail: String, company_type: String, company_person: String, search_key: String, city: String)

  def getJdbcRDD(sc: SparkContext,
                 conn: () => Connection,
                 sql: String,
                 lowerBound: Int,
                 upperBound: Int,
                 numPartitions: Int): JdbcRDD[jobDetail] = {

    new JdbcRDD[jobDetail](
      sc,
      conn,
      sql,
      lowerBound,
      upperBound,
      numPartitions,
      rstSet => {
        val job_id:String = rstSet.getString(1)
        val job_name: String = rstSet.getString(2)
        val job_url: String = rstSet.getString(3)
        val job_location: String = rstSet.getString(4)
        val job_salary:String = rstSet.getString(5)
        val job_company: String = rstSet.getString(6)
        val job_experience:String = rstSet.getString(7)
        val job_class: String = rstSet.getString(8)
        val job_given:String = rstSet.getString(9)
        val job_detail: String = rstSet.getString(10)
        val company_type:String = rstSet.getString(11)
        val company_person: String = rstSet.getString(12)
        val search_key:String = rstSet.getString(13)
        val city: String = rstSet.getString(14)

        jobDetail(job_id, job_name, job_url, job_location, job_salary, job_company, job_experience, job_class, job_given, job_detail, company_type, company_person, search_key, city)
      }
    )

  }

}
