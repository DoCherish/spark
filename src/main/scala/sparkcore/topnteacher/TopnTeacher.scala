package sparkcore.topnteacher

import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import utils.SparkContextUtil.getScLocal

import scala.collection.mutable

/**
 * @Author Do
 * @Date 2021/6/12 13:57
 */
object TopnTeacher {

  val topN = 1

  def main(args: Array[String]): Unit = {
    val sc: SparkContext = getScLocal(this.getClass.getSimpleName)
    val lines: RDD[String] = sc.textFile("src/main/resources/sparkcore/teacher.log")

    val subjectTeacherAddOne: RDD[((String, String), Int)] = lines.map(line => {
      val strings: Array[String] = line.split("/")
      val subject: String = strings(2).split("[.]")(0)
      val teacher: String = strings(3)

      ((subject, teacher), 1)
    })

    val rst: RDD[((String, String, Int), Null)] = method6(subjectTeacherAddOne)

    println(rst.collect().toBuffer)

    sc.stop()

  }

  // 全局排序（未实现组内排序）
  def method1: RDD[((String, String), Int)] => Array[((String, String), Int)] =
    (subjectTeacherAddOne: RDD[((String, String), Int)]) => {
      subjectTeacherAddOne
        .reduceByKey(_ + _)
        .sortBy(_._2, false)
        .take(topN)
    }

  // 方法二：将一个组的数据，全部加载到内存
  // 风险：若是组内数据量太大，可能会出现内存溢出
  def method2(subjectTeacherAddOne: RDD[((String, String), Int)]): RDD[(String, Iterable[((String, String), Int)])] = {
    subjectTeacherAddOne
      .reduceByKey(_ + _) // 分区内聚合
      .groupBy(_._1._1) // 按照学科分组
      .mapValues(iter => {
        iter
          .toList // 将迭代器数据全部放到list，然后再排序
          .sortBy(-_._2)
          .take(topN)
      })

  }

  // 方法三：过滤出不同科目，分别做处理
  def method3(subjectTeacherAddOne: RDD[((String, String), Int)]) = {
    val reduced: RDD[((String, String), Int)] = subjectTeacherAddOne.reduceByKey(_ + _)
    reduced.cache()
    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()
    for (sub <- subjects) {
      val rst: Array[((String, String), Int)] = reduced
        .filter(_._1._1.equals(sub))
        .sortBy(-_._2)  // 调用rdd的sortBy（使用rangePartitioner进行排序，内存加磁盘）
        .collect()      // 多次触发action操作
        .take(topN)
      println(rst.toBuffer)
    }
    reduced.unpersist()
  }

  // 方法四：使用自定义分区器，每个分区只存储一个科目的数据
  // 一个科目数据量很大时，toList会OOM
  def method4(subjectTeacherAddOne: RDD[((String, String), Int)]):  RDD[((String, String), Int)] = {
    val subjects: Array[String] = subjectTeacherAddOne.map(_._1._1).distinct().collect()
    val reduced: RDD[((String, String), Int)] = subjectTeacherAddOne
      .reduceByKey(new SubjectPartitioner(subjects), _ + _)

    val rst: RDD[((String, String), Int)] = reduced
      .mapPartitions((iter: Iterator[((String, String), Int)]) => {
        iter
          .toList
          .sortBy(-_._2)
          .take(topN)
          .iterator
      })

    rst
  }

  class SubjectPartitioner(val subjects: Array[String]) extends Partitioner{
    // 构造器
    // 初始化分区器的分区规则：每个科目对应一个分区
    private val rules = new mutable.HashMap[String, Int]()
    private var id = 0
    for (sub <- subjects) {
      rules.put(sub, id)
      id += 1
    }

    override def numPartitions: Int = subjects.length
    // 该方法会在executor中的task被调用
    override def getPartition(key: Any): Int = {
      val subject: String = key.asInstanceOf[(String, String)]._1
      rules(subject)
    }
  }

  def method5(subjectTeacherAddOne: RDD[((String, String), Int)]) = {
    val subjects: Array[String] = subjectTeacherAddOne.map(_._1._1).distinct().collect()
    val reduced: RDD[((String, String), Int)] = subjectTeacherAddOne
      .reduceByKey(new SubjectPartitioner(subjects), _ + _)

    reduced.mapPartitions(iter => {
      implicit val sortRules = Ordering[Int].on[((String, String), Int)](-_._2)
      // 定义一个key排序的集合TreeSet
      val sorter: mutable.TreeSet[((String, String), Int)] = new mutable.TreeSet[((String, String), Int)]()
      iter.foreach(i => {
        sorter += i
        if (sorter.size > 2) {
          val last: ((String, String), Int) = sorter.last
          sorter -= last
        }
      })

      sorter.iterator

    })
  }

  // 调用算子：repartitionAndSortWithinPartitions
  def method6(subjectTeacherAddOne: RDD[((String, String), Int)]) = {
    val subjects: Array[String] = subjectTeacherAddOne.map(_._1._1).distinct().collect()
    val reduced: RDD[((String, String), Int)] = subjectTeacherAddOne.reduceByKey(_ + _)
    val partitioner: SubjectPartitionerV2 = new SubjectPartitionerV2(subjects)
    // 在原数据上构建新的key
    val newRDD: RDD[((String, String, Int), Null)] = reduced.map(t => ((t._1._1, t._1._2, t._2), null))
    // 隐式转换：新的排序规则
    implicit val sortRules = Ordering[Int].on[(String, String, Int)](-_._3)

    val rst: RDD[((String, String, Int), Null)] = newRDD.repartitionAndSortWithinPartitions(partitioner)
    rst
  }

  class SubjectPartitionerV2(subjects: Array[String]) extends Partitioner {
    private val sorter = new mutable.HashMap[String, Int]()
    var index: Int = 0
    for (i <- subjects) {
      sorter(i) = index
      index += 1
    }

    override def numPartitions: Int = subjects.length

    override def getPartition(key: Any): Int = {
      val tuple: (String, String, Int) = key.asInstanceOf[(String, String, Int)]
      val subject: String = tuple._1
      sorter(subject)
    }
  }



}
