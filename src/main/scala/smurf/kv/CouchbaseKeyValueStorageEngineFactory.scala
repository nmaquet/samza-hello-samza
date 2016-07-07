package smurf.kv

import java.io.File

import org.apache.samza.container.SamzaContainerContext
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.storage.kv.{BaseKeyValueStorageEngineFactory, _}
import org.apache.samza.system.SystemStreamPartition

class CouchbaseKeyValueStorageEngineFactory [K, V] extends BaseKeyValueStorageEngineFactory[K, V]
{

  override def getKVStore(storeName: String,
                          storeDir: File,
                          registry: MetricsRegistry,
                          changeLogSystemStreamPartition: SystemStreamPartition,
                          containerContext: SamzaContainerContext): KeyValueStore[Array[Byte], Array[Byte]] = {
    val storageConfig = containerContext.config.subset("stores." + storeName + ".", true)
    val url = storageConfig.get("couchbase.url", "localhost")
    val bucket = storageConfig.get("couchbase.bucket")

    if (bucket == null) {
      throw new Exception("missing keys: stores." + storeName + ".couchbase.{bucket}")
    }

    new CouchbaseKeyValueStore(
      url,
      bucket
    )
  }
}
