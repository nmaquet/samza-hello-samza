package smurf.kv

import org.apache.samza.storage.kv.KeyValueStore
import org.apache.samza.util.Logging

class MySQLKeyValueStore(host: String, port: Int, user: String, password: String, database: String, table: String) extends KeyValueStore[Array[Byte], Array[Byte]] with Logging {

}
