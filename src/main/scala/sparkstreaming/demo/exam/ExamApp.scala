package sparkstreaming.demo.exam

import constant.Constants._
import org.apache.spark.SparkContext
import scalikejdbc.{ConnectionPool, DB, _}
import sparkstreaming.demo.exam.ExamETL._
import utils.utils._
import utils.SparkContextUtil.getScLocal
import constant.Constants._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.StreamingContextUtil._

/**
 * @Author Do
 * @Date 2021/4/10 15:11
 */
object ExamApp {

  def main(args: Array[String]): Unit = {

    //  DBs.setup()
    // 在Driver端创建数据库连接池
    Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(mysqlUrl, mysqlUser, mysqlPsw)

    //    val strList: String = ListConvert2Str(List("5bbe0f922ec1740ca9fba9cc3a716641", "6fa766afced1f5420327d7ac4fcf2848"))
    //    println(strList)
    //
    //    val rsts: Seq[examRst] = getPersonRK(strList)
    //    rsts.foreach(println(_))

//    val objects: Seq[ExamJson] = intoExamJson("[{\"trainId\":\"px111\",\"empId\":\"duxu\",\"empName\":\"杜续\",\"resourceName\":\"课程1\",\"extProp\":{\"score\":90,\"attemptId\":\"attemptId001\",\"testName\":\"testName001\",\"testTime\":0.3,\"status\":\"Y\"}}]")
//    println(objects)
//    println(objects(0).personRowKeyStr)

    val sc: SparkContext = getScLocal("Exam")
    val ssc = new StreamingContext(sc, Seconds(5))

    // todo 1-清晰合并当前批次数据
    val examDStream: DStream[(String, List[ExamJson])] = createSocketStream(ssc)
      .filter(StringUtils.isNotEmpty(_))
      .map(intoExamJson(_))
      .flatMap(v => v)
      // 按人聚合
      .map(v => (v.personRK, List(v)))
      .reduceByKey(_ ++ _)

    // todo 2-写入mysql前，按分区进行聚合处理
    examDStream.mapPartitions((eachParts: Iterator[(String, List[ExamJson])]) => {
      val examList: List[(String, List[ExamJson])] = eachParts.toList
      val examMap: Map[String, List[ExamJson]] = examList.toMap
      // 本批次主键
      val personRKList: List[String] = examList.map(_._1)
      val strList: String = ListConvert2Str(personRKList)
      // mysql中已存在的人员学习记录
      val existList: Seq[examRst] = getPersonRKFromMysql(strList)
      // mysql中已存在的主键
      val existRKList: List[String] = existList.map(v => v.personRK).toList
      // mysql中不存在的主键
      val notExistRKList: List[String] = personRKList.diff(existRKList)

      // 对当前数据进行处理，分两种情况：mysql中有无历史数据
      // ---有历史数据
      if (!existList.isEmpty) {
        existList.map((ele: examRst) => {
          // 过滤掉“超时提交成绩无效”的数据
          val onePersonList: List[ExamJson] = examMap(ele.personRK).filter(_.status != 'F')
          onePersonList


        })
      }

      eachParts
    })


  }



}
