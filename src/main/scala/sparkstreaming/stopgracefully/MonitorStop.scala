package sparkstreaming.stopgracefully

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.{StreamingContext, StreamingContextState}

/**
 * @Author Do
 * @Date 2021/1/14 18:04
 * 该类内部每隔5s监听一下hdfs上："/stopStreaming"文件夹是否存在，若存在，则优雅关闭程序。
 *
 * 1 stopSparkContext: if true, stops the associated SparkContext. The underlying SparkContext
 * will be stopped regardless of whether this StreamingContext has been started.
 * 2 stopSparkContext：if true, stops gracefully by waiting for the processing of all received data to be completed
 *
 */
class MonitorStop(ssc: StreamingContext) extends Runnable {
  override def run(): Unit = {
    // 获取hdfs分布式文件系统
    val fs: FileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration())

    while (true) {
      try {
        // 主线程每隔5s执行一次
        Thread.sleep(5000)
      } catch {
        case e: InterruptedException => e.printStackTrace()
      }
      // 获取StreamingContext程序的运行状态
      val state: StreamingContextState = ssc.getState()
      println("state: " + state)
      // 监听目录是否存在
      val isExists: Boolean = fs.exists(new Path("hdfs://node01:8020/stopStreaming"))
      println("isExists: " + isExists)
      if (isExists) {
        // 若存在，且运行状态为active（The context has been started, and been not stopped.），则优雅关闭程序
        if (state == StreamingContextState.ACTIVE) {
          ssc.stop(stopSparkContext = true, stopGracefully = true)

          System.exit(0)
        }
      }

    }

  }
}
