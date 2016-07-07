package smurf.kv

import java.{util}

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.api.commands.kv.{DeleteValue, FetchValue, StoreValue}
import com.basho.riak.client.core.query.{Location, Namespace, RiakObject}
import com.basho.riak.client.core.util.BinaryValue
import com.basho.riak.client.core.{RiakCluster, RiakNode}
import org.apache.samza.storage.kv.{Entry, KeyValueIterator, KeyValueStore}
import org.apache.samza.util.Logging

import scala.collection.JavaConverters._

class RiakKeyValueStore(
  url: String,
  port: Int,
  bucketName: String
) extends KeyValueStore[Array[Byte], Array[Byte]] with Logging {
  val node = new RiakNode.Builder()
    .withRemoteAddress(url)
    .withRemotePort(port)
    .build()

  val cluster = new RiakCluster.Builder(node).build()
  cluster.start()
  val client = new RiakClient(cluster)
  val bucket = new Namespace("profile")

  override def range(k: Array[Byte], k1: Array[Byte]): KeyValueIterator[Array[Byte], Array[Byte]] = {
    ???
  }

  override def get(k: Array[Byte]): Array[Byte] = {
    val location = new Location(bucket, BinaryValue.create(k))
    val fetchOp = new FetchValue.Builder(location).build()
    val fetchedObject = client.execute(fetchOp).getValue(classOf[RiakObject])
    fetchedObject.getValue.getValue
  }

  override def put(k: Array[Byte], v: Array[Byte]): Unit = {

    val valueObj = new RiakObject()
      .setContentType("text/plain")
      .setValue(BinaryValue.create(v))

    val location = new Location(bucket, BinaryValue.create(k))

    val storeOp = new StoreValue.Builder(valueObj)
      .withLocation(location)
      .build()
    val response = client.execute(storeOp)
    println(response.getValues)
    println(response)
  }

  override def all(): KeyValueIterator[Array[Byte], Array[Byte]] = ???

  override def flush(): Unit = {
    // no op
  }

  override def delete(k: Array[Byte]): Unit = {
    val location = new Location(bucket, BinaryValue.create(k))
    val deleteOp = new DeleteValue.Builder(location).build()
    client.execute(deleteOp)
  }

  override def close(): Unit = {
    cluster.shutdown()
  }

  override def putAll(list: util.List[Entry[Array[Byte], Array[Byte]]]): Unit = {
    // TODO: optimize this
    list.asScala.foreach((e) => put(e.getKey, e.getValue))
  }
}


