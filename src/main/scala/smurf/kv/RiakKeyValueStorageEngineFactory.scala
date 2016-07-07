package smurf.kv

import java.io.File

import org.apache.samza.container.SamzaContainerContext
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.storage.kv.{BaseKeyValueStorageEngineFactory, _}
import org.apache.samza.system.SystemStreamPartition

class RiakKeyValueStorageEngineFactory [K, V] extends BaseKeyValueStorageEngineFactory[K, V]
{

  override def getKVStore(storeName: String,
                          storeDir: File,
                          registry: MetricsRegistry,
                          changeLogSystemStreamPartition: SystemStreamPartition,
                          containerContext: SamzaContainerContext): KeyValueStore[Array[Byte], Array[Byte]] = {
    val storageConfig = containerContext.config.subset("stores." + storeName + ".", true)
    val url = storageConfig.get("riak.url", "localhost")
    val port = storageConfig.getInt("riak.port", 8087)
    val bucketName = storageConfig.get("riak.bucketName")

    if (bucketName == null) {
      throw new Exception("missing keys: stores." + storeName + ".riak.{bucketName}")
    }

    new RiakKeyValueStore(
      url,
      port,
      bucketName
    )
  }
}
