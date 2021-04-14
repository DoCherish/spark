package utils

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
 * @Author Do
 * @Date 2021/1/14 20:47
 */
object StreamingContextUtil {

  def createSocketStream(ssc: StreamingContext): ReceiverInputDStream[String] = {
    ssc.socketTextStream("node01", 9999)
  }


}
