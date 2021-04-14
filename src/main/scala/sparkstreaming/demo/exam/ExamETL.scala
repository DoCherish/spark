package sparkstreaming.demo.exam

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.catalyst.optimizer.OptimizeIn
import org.apache.commons.codec.digest._
import org.apache.kafka.common.TopicPartition
import scalikejdbc.{DB, SQL}

import scala.util.{Failure, Success, Try}

/**
 * @Author Do
 * @Date 2021/4/10 13:38
 */
object ExamETL {

  case class ExtProp(
                    var attemptId: String = "",
                    var testName: String = "",
                    var score: Double = 0D,
                    var testTime: Double = 0D,
                    var status: String = ""
                    )

  case class ExamJson(
                     var trainId: String = "",
                     var empId: String = "",
                     var resource_id: String = "", // 即考试id,取 resource_id
                     var examIdList:Array[String] = Array(""),
                     var resourceName: String = "",
                     var userId: String = "",
                     var upLoadDate: Long = 0L, // 上报时间
                     var empName: String = "",
                     var enterpriseId: String = "",

                     var extProp: ExtProp = ExtProp(),

                     var attemptId: String = "",
                     var testName: String = "",
                     var score: Double = 0D,
                     var testTime: Double = 0D,
                     var status: String = ""

                     ){
    // 用于统计一个人的所有考试数据
    def personRK: String = DigestUtils.md5Hex(enterpriseId + "_" + trainId + "_" + userId)

    def initExamJson(): ExamJson ={
      attemptId = Option(extProp.attemptId).getOrElse("")
      testName = Option(extProp.testName).getOrElse("")
      score = Option(extProp.score).getOrElse(0D)
      testTime = Option(extProp.testTime).getOrElse(0D)
      status = Option(extProp.status).getOrElse("")


      println("考试数据初始化完毕！")
      this
    }

  }

  def intoExamJson(str: String): Seq[ExamJson] = {
    import scala.collection.JavaConverters._

    val jsons: Seq[ExamJson] = Try(JSON.parseArray(str, classOf[ExamJson])) match {
      case Success(s) => s.asScala.toList
      case Failure(e) => Seq(ExamJson())
    }

    jsons.map(_.initExamJson())

  }


  def getCurrentOffsets(topics: Array[String], groupId: String): Map[TopicPartition, Long] = {
    import scalikejdbc._

    DB.readOnly(implicit session => {
      sql"select partitions, offset from kafka_topic_offset where topic = ${topics(0)} and group_id = ${groupId}"
        .map(item => {
          val partition: Int = item.get[Int]("partition")
          val offset: Long = item.get[Long]("offset")
          new TopicPartition(topics(0), partition) -> offset // (TopicPartition, Int)
        })
        .list()
        .apply()
        .toMap
    })
  }

  case class examRst(id: String, enterprise_id: String, train_id: String, user_id: String,
                     test_submit_times: Long, test_score_set: String, total_points: Double, stat_date: String) {
    def personRK: String = DigestUtils.md5Hex(enterprise_id + "_" + train_id + "_" + user_id)
  }

  /**
   * mysql表exam字段：
   * id   主键 一人维度
   * enterprise_id
   * train_id
   * user_id
   * test_submit_times 考试提交次数（attemptId去重）
   * test_score_set    考试分数列表
   * total_points      考试总分
   * stat_date         统计日期
   *
   * @param strList
   * @return
   */
  def getPersonRKFromMysql(strList: String): Seq[examRst] = {
    DB.readOnly(implicit session => {
      SQL(
        "select * from exam where id in (" + strList + ")"
      )
        // .bind("6fa766afced1f5420327d7ac4fcf2848")
        .map(item => {
          val id: String = item.get[String]("id")
          val enterprise_id: String = item.get[String]("enterprise_id")
          val train_id: String = item.get[String]("train_id")
          val user_id: String = item.get[String]("user_id")
          val test_submit_times: Long = item.get[Long]("test_submit_times")
          val test_score_set: String = item.get[String]("test_score_set")
          val total_points: Double = item.get[Double]("total_points")
          val stat_date: String = item.get[String]("stat_date")

          examRst(id, enterprise_id, train_id, user_id, test_submit_times, test_score_set, total_points, stat_date)

        })
        .list()
        .apply()
    })
  }


  /**
   *
   * @param lst
   * @return
   */
  def ListConvert2Str(lst: Seq[String]): String = {
    //    lst.mkString(",")
    lst.map("\'" + _ + "\'").mkString(",")
  }



}
