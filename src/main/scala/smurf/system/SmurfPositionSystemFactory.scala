
package smurf.system

import org.apache.samza.SamzaException
import org.apache.samza.config.Config
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.{SystemAdmin, SystemConsumer, SystemFactory, SystemProducer}
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin

class SmurfPositionSystemFactory extends SystemFactory {

  override def getAdmin(systemName: String, config: Config): SystemAdmin = {
    new SinglePartitionWithoutOffsetsSystemAdmin()
  }

  override def getConsumer(systemName: String, config: Config, registry: MetricsRegistry): SystemConsumer = {
    val messagesPerBatch: Int = config.getInt("systems." + systemName + ".messages-per-batch")
    val threadCount: Int = config.getInt("systems." + systemName + ".thread-count")
    val brokerSleepMs: Int = config.getInt("systems." + systemName + ".broker-sleep-ms")
    new SmurfPositionConsumer(messagesPerBatch, threadCount, brokerSleepMs)
  }

  override def getProducer(systemName: String, config: Config, registry: MetricsRegistry): SystemProducer  = {
    throw new SamzaException("You can't produce to the smurf position system!")
  }
}
