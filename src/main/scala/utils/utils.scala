package utils

import scala.collection.mutable.Map
/**
 * @Author Do
 * @Date 2020/12/5 13:23
 */
package object utils {

  /**
   * 获取命令行参数
   * 格式：env=test,type=B
   *
   * @param args
   * @return
   */
  def getParams(args: Array[String]): Map[String, String] = {
    val map: Map[String, String] = Map()
    if (args.length != 1) {
      throw new Exception("args length is not 1!")
    } else {
      args(0).split(",").foreach(item => {
        val itemSplit: Array[String] = item.split("=")
        val key: String = itemSplit(0).trim
        val values: String = itemSplit(1).trim
        map += (key -> values)
      })
      map
    }


  }

}
