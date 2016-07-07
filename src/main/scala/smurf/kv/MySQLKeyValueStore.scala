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

  info("Instantiating connection to MySQL...")

  Class.forName("com.mysql.jdbc.Driver")

  val conn = sql.DriverManager.getConnection(s"jdbc:mysql://$url/$database", user, password)

  conn.setAutoCommit(false) // only use MyISAM tables !

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
    if (rs.next()) {
      val result = rs.getBytes(1)
      statement.close()
      result
    } else {
      statement.close()
      null
    }
  }

  override def put(k: Array[Byte], v: Array[Byte]): Unit = {
    val statement = conn.prepareStatement(s"INSERT INTO `$table` VALUES (?, ?) ON DUPLICATE KEY UPDATE `value` = ?")
    statement.setBytes(1, k)
    statement.setBytes(2, v)
    statement.setBytes(3, v)
    statement.executeUpdate()
    statement.close()
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
    statement.close()
  }

  override def close(): Unit = {
    conn.close()
  }

  var counter = 0

  override def putAll(list: util.List[Entry[Array[Byte], Array[Byte]]]): Unit = {
    if (counter < 10) {
      counter += 1
      info(s"putAll of ${list.size()} elements")
    }
    val statement = conn.prepareStatement(s"INSERT INTO `$table` VALUES (?, ?) ON DUPLICATE KEY UPDATE `value` = ?")
    list.asScala.foreach((e) => {
      statement.setBytes(1, e.getKey)
      statement.setBytes(2, e.getValue)
      statement.setBytes(3, e.getValue)
      statement.addBatch()
    })
    statement.executeBatch()
    statement.close()
  }
}
