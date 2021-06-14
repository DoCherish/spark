package sparkstreaming.demo.exam

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.catalyst.optimizer.OptimizeIn
import org.apache.commons.codec.digest._
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.common.TopicPartition
import scalikejdbc.{DB, SQL}

import scala.beans.BeanProperty
import scala.collection.immutable
import scala.util.{Failure, Success, Try}

/**
 * @Author Do
 * @Date 2021/4/10 13:38
 */
object ExamETL {
  // mysql字段
  case class examRst(
                      var id: String,               // 主键
                      var enterprise_id: String,    // 企业id
                      var train_id: String,         // 培训项目id
                      var user_id: String,          // 用户id
                      var stat_date: String,        // 统计日期
                      var examIdList: Array[String],// 考试id列表

                      var total_points: Double,     // 最高分
                      var test_score_set: String,   // 考试集合对象 List[testScoreJson]
                      var test_submit_times: Long   // 考试提交次数
                    ) {
    def personRK: String = DigestUtils.md5Hex(enterprise_id + "_" + train_id + "_" + user_id)
  }

  case class testScoreJson(
                          @BeanProperty resourceId: String = "",
                          @BeanProperty testName: String = "",
                          @BeanProperty var status: String = "",
                          @BeanProperty var score: Double = 0D,
                          @BeanProperty var uploadDate: Long = 0L,
                          @BeanProperty attemptId: String = ""
                          )

  case class ExtProp(
                    var attemptId: String = "",
                    var testName: String = "",
                    var score: Double = 0D,
                    var testTime: Double = 0D,
                    var status: String = ""
                    )

  case class ExamJson(
                     var trainId: String = "",                  // 培训项目id
                     var enterpriseId: String = "",             // 企业id
                     var empId: String = "",                    // 员工id
                     var empName: String = "",                  // 员工姓名
                     var userId: String = "",                   // 用户id
                     var resource_id: String = "",              // 考试id,取 resource_id
                     var resourceName: String = "",             // 资源名称
                     var examIdList:Array[String] = Array(""),  // 考试id数组
                     var upLoadDate: Long = 0L,                 // 上报时间

                     var extProp: ExtProp = ExtProp(),          // 自定义字段
                     // 从extProp中取以下字段
                     var attemptId: String = "",                // 提交id
                     var testName: String = "",                 // 考试名称
                     var score: Double = 0D,                    // 考试分数
                     var testTime: Double = 0D,                 // 考试耗时
                     var status: String = ""                    // 考试状态

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


  /**
   *
   * @param topics
   * @param groupId
   * @return
   */
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
      SQL("select * from exam where id in (" + strList + ")")
        // .bind()
        .map(item => { // 对结果进行解析
          val id: String = item.get[String]("id")
          val enterprise_id: String = item.get[String]("enterprise_id")
          val train_id: String = item.get[String]("train_id")
          val user_id: String = item.get[String]("user_id")
          val test_submit_times: Long = item.get[Long]("test_submit_times")
          val test_score_set: String = item.get[String]("test_score_set")
          val total_points: Double = item.get[Double]("total_points")
          val stat_date: String = item.get[String]("stat_date")

          examRst(id, enterprise_id, train_id, user_id, stat_date,  Array("d"), total_points, test_score_set, test_submit_times)

        })
        .list()
        .apply()
    })
  }

  def ListConvert2Str(lst: Seq[String]): String = {
    //    lst.mkString(",")
    lst.map("\'" + _ + "\'").mkString(",")
  }

//  def rmvDuplicate(testScoreJsons: Seq[testScoreJson], head: ExamJson): immutable.Iterable[testScoreJson] = {
//    testScoreJsons
//      .filter(j => StringUtils.isNotBlank(j.attemptId) && StringUtils.isNotBlank(j.resourceId))
//      .map(j => ((j.resourceId + j.attemptId), j))
//      .groupBy(_._1)
//      .map(j => {
//        j._2.reduce((a, b) => {
//          if (a._2.uploadDate > b._2.uploadDate) {
//            a._2
//          } else {
//            b._2
//          }
//
//        })
//      })
//      .filter(head.examIdList.contains(_))
//
//  }


}
