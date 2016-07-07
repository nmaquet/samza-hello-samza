package smurf.kv

import java.io.File
import org.apache.samza.container.SamzaContainerContext
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.storage.kv.BaseKeyValueStorageEngineFactory
import org.apache.samza.storage.kv._
import org.apache.samza.system.SystemStreamPartition

class MySQLKeyValueStorageEngineFactory [K, V] extends BaseKeyValueStorageEngineFactory[K, V]
{

  override def getKVStore(storeName: String,
                          storeDir: File,
                          registry: MetricsRegistry,
                          changeLogSystemStreamPartition: SystemStreamPartition,
                          containerContext: SamzaContainerContext): KeyValueStore[Array[Byte], Array[Byte]] = {
    val storageConfig = containerContext.config.subset("stores." + storeName + ".", true)
    val host = storageConfig.get("mysql.host", "localhost")
    val port = storageConfig.getInt("mysql.port", 3306)
    val user = storageConfig.get("mysql.user", "root")
    val password = storageConfig.get("mysql.password", "")
    val database = storageConfig.get("mysql.database")
    val table = storageConfig.get("mysql.table")

    if (database == null || table == null) {
      throw new Exception("missing keys: stores." + storeName + ".mysql.{database,table}")
    }

    new MySQLKeyValueStore(
      host,
      port,
      user,
      password,
      database,
      table
    )
  }
}
