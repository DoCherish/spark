package utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import constant.Constants._
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

/**
 * @Author Do
 * @Date 2020/11/26 17:05
 */
object HbaseUtil {

  def createHbaseConf(): Configuration = {
    val conf: Configuration = HBaseConfiguration.create()
    conf.set(HBASE_ZOOKEEPER_QUORUM, zkQuorum)
    conf.set(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, zkPort)

    conf
  }

  def createTableIfNotExist(name: String, families: String*): Unit = {
    val conf: Configuration = createHbaseConf()
    val conn: Connection = ConnectionFactory.createConnection(conf)
    val admin: Admin = conn.getAdmin
    val tableName: TableName = TableName.valueOf(name)
    val tableDesc: HTableDescriptor = new HTableDescriptor(tableName)

    families.foreach(f => {
      tableDesc.addFamily(new HColumnDescriptor(f.getBytes()))
    })

    if (!admin.tableExists(tableName)) {
      admin.createTable(tableDesc)
      println(s"${tableName}已经成功创建！")
    } else {
      println(s"${tableName}已存在！")
    }

  }

  def getTable(conf: Configuration, name: String): HTable = {
    new HTable(conf, name)
  }

  def putData(tableName: String, rowKey: String, columnFamily: String, column: String, data: String): Unit = {
    val conf: Configuration = createHbaseConf()
    val table: HTable = getTable(conf, tableName)

    val put: Put = new Put(rowKey.getBytes())

    put.add(columnFamily.getBytes(), column.getBytes(), data.getBytes())
    table.put(put)
  }

}
