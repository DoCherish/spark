package constant

import com.typesafe.config.{Config, ConfigFactory}

/**
 * @Author Do
 * @Date 2020/11/18 16:40
 */
object Constants {

  lazy val config: Config = ConfigFactory.load()

  lazy val mysqlUrl: String = config.getString("mysql.url")
  lazy val mysqlUser: String = config.getString("mysql.user")
  lazy val mysqlPsw: String = config.getString("mysql.password")



}
