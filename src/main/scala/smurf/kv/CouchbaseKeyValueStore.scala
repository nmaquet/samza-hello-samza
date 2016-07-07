package smurf.kv

import java.util

import scala.collection.JavaConverters._
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query._
import org.apache.samza.storage.kv.{Entry, KeyValueIterator, KeyValueStore}
import org.apache.samza.util.Logging
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query.dsl.Expression._

class CouchbaseKeyValueStore(url: String, bucketName: String) extends KeyValueStore[Array[Byte], Array[Byte]] with Logging {
  val cluster = CouchbaseCluster.create(url)
  val bucket = cluster.openBucket(bucketName)

  override def range(from: Array[Byte], to: Array[Byte]): KeyValueIterator[Array[Byte], Array[Byte]] = {

    val statement = select("key", "value").from(i(bucketName)).where(
      x("key").gte(x("$startKey")).and(x("key").lt(x("$endKey"))))
    val placeholderValues = JsonObject.create().put("startKey", new String(from)).put("endKey", new String(to))
    val query = N1qlQuery.parameterized(statement, placeholderValues)
    val iter = bucket.query(query).iterator()

    new KeyValueIterator[Array[Byte], Array[Byte]] {
      override def close(): Unit = {
        /* no op */
      }

      override def next(): Entry[Array[Byte], Array[Byte]] = {
        val doc = iter.next().value()
        val key = doc.getString("key")
        val value = doc.getString("value")
        // doc.
        new Entry(key.getBytes("UTF-8"), value.getBytes("UTF-8"))
      }
      override def hasNext: Boolean = iter.hasNext
    }
  }

  override def get(k: Array[Byte]): Array[Byte] = {
    val v = bucket.get(new String(k))
    v.content().getString("value").getBytes("UTF-8")
  }

  override def put(k: Array[Byte], v: Array[Byte]): Unit = {
    val content = JsonObject.create().put(new String(k), new String(v))
    bucket.upsert(JsonDocument.create(new String(k), content))
  }

  override def all(): KeyValueIterator[Array[Byte], Array[Byte]] = {
    val statement = select("key", "value").from(i(bucketName))
    val query = N1qlQuery.simple(statement)
    val iter = bucket.query(query).iterator()

    new KeyValueIterator[Array[Byte], Array[Byte]] {
      override def close(): Unit = {
        /* no op */
      }

      override def next(): Entry[Array[Byte], Array[Byte]] = {
        val doc = iter.next().value()
        val key = doc.getString("key")
        val value = doc.getString("value")
        // doc.
        new Entry(key.getBytes("UTF-8"), value.getBytes("UTF-8"))
      }
      override def hasNext: Boolean = iter.hasNext
    }
  }

  override def flush(): Unit = {
    /* no op */
  }

  override def delete(k: Array[Byte]): Unit = {
    bucket.remove(new String(k))
  }

  override def close(): Unit = {
    bucket.close()
    cluster.disconnect()
  }

  override def putAll(list: util.List[Entry[Array[Byte], Array[Byte]]]): Unit = {
    list.asScala.foreach(x => put(x.getKey, x.getValue))
  }
}
