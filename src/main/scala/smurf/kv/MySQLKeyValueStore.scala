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

  override def range(from: Array[Byte], to: Array[Byte]): KeyValueIterator[Array[Byte], Array[Byte]] = {
    val statement = conn.prepareStatement(s"SELECT `key`, `value` FROM `$table` WHERE `key` >= ? AND `key` < ?")
    statement.setBytes(1, from)
    statement.setBytes(2, to)
    val rs = statement.executeQuery()

    new KeyValueIterator[Array[Byte], Array[Byte]] {
      override def close(): Unit = {
        rs.close()
        statement.close()
      }

      override def next(): Entry[Array[Byte], Array[Byte]] = new Entry[Array[Byte], Array[Byte]](rs.getBytes(1), rs.getBytes(2))

      override def hasNext: Boolean = rs.next()
    }
  }

  override def get(k: Array[Byte]): Array[Byte] = {
    val statement = conn.prepareStatement(s"SELECT `value` FROM `$table` WHERE `key` = ?")
    statement.setBytes(1, k)
    val rs = statement.executeQuery()
    rs.getBytes(1)
  }

  override def put(k: Array[Byte], v: Array[Byte]): Unit = {
    val statement = conn.prepareStatement(s"INSERT INTO `$table` VALUES (?, ?)")
    statement.setBytes(1, k)
    statement.setBytes(2, v)
    statement.executeUpdate()
  }

  override def all(): KeyValueIterator[Array[Byte], Array[Byte]] = {
    val statement = conn.prepareStatement(s"SELECT `key`, `value` FROM `$table`")
    val rs = statement.executeQuery()

    new KeyValueIterator[Array[Byte], Array[Byte]] {
      override def close(): Unit = {
        rs.close()
        statement.close()
      }

      override def next(): Entry[Array[Byte], Array[Byte]] = new Entry[Array[Byte], Array[Byte]](rs.getBytes(1), rs.getBytes(2))

      override def hasNext: Boolean = rs.next()
    }
  }

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
