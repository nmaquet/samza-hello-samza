package smurf.kv

import java.util
import java.sql
import scala.collection.JavaConverters._

import org.apache.samza.storage.kv.{KeyValueIterator, Entry, KeyValueStore}
import org.apache.samza.util.Logging

class MySQLKeyValueStore(
    url: String,
    user: String,
    password: String,
    database: String,
    table: String) extends KeyValueStore[Array[Byte], Array[Byte]] with Logging {

  Class.forName("com.mysql.jdbc.Driver")

  val conn = sql.DriverManager.getConnection(s"jdbc:mysql://$url/$database", user, password)

  override def range(k: Array[Byte], k1: Array[Byte]): KeyValueIterator[Array[Byte], Array[Byte]] = ???

  override def get(k: Array[Byte]): Array[Byte] = {
    ???
  }

  override def put(k: Array[Byte], v: Array[Byte]): Unit = {
    val statement = conn.prepareStatement(s"INSERT INTO `$table` VALUES (?, ?)")
    statement.setBytes(1, k)
    statement.setBytes(2, v)
    statement.executeUpdate()
  }

  override def all(): KeyValueIterator[Array[Byte], Array[Byte]] = ???

  override def flush(): Unit = {
    // no-op
  }

  override def delete(k: Array[Byte]): Unit = {
    val statement = conn.prepareStatement(s"DELETE FROM `$table` WHERE `key` = ?")
    statement.setBytes(1, k)
    statement.executeUpdate()
  }

  override def close(): Unit = {
    conn.close()
  }

  override def putAll(list: util.List[Entry[Array[Byte], Array[Byte]]]): Unit = {
    // TODO: optimize this
    list.asScala.foreach((e) => put(e.getKey, e.getValue))
  }
}
