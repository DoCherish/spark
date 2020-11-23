package constant

import com.typesafe.config.{Config, ConfigFactory}

/**
 * @Author Do
 * @Date 2020/11/18 16:40
 */
object Constants {

  // 默认加载配置文件的顺序是：application.conf --> application.json --> application.properties
  lazy val config: Config = ConfigFactory.load()

  lazy val mysqlUrl: String = config.getString("mysql.url")
  lazy val mysqlUser: String = config.getString("mysql.user")
  lazy val mysqlPsw: String = config.getString("mysql.password")

  



}
