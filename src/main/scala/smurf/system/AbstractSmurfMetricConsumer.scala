package smurf.system

import org.apache.samza.system.{IncomingMessageEnvelope, SystemStreamPartition}
import org.apache.samza.util.BlockingEnvelopeMap

abstract class AbstractSmurfMetricConsumer(val messagesPerBatch: Int, val threadCount: Int, val brokerSleepMs: Int) extends BlockingEnvelopeMap {

  val consumerPrefix: String
  def getKey(): Integer
  def getMessage(key: Integer): java.util.Map[String, Object]

  var streamPartitions = Set[SystemStreamPartition]()
  var threads = Seq[Thread]()

  def createThreads(): Seq[Thread] = {
    for {
      i <- 0 until threadCount
      threadStreamPartitions = streamPartitions filter { streamPartition =>
        Math.abs(streamPartition.hashCode()) % threadCount == i
      }
    } yield new Thread(new MockSystemConsumerRunnable(threadStreamPartitions), consumerPrefix + i)
  }

  override def start(): Unit = {
    threads = createThreads()
    for (thread <- threads) {
      thread.setDaemon(true)
      thread.start()
    }
  }

  override def stop(): Unit = {
    for (thread <- threads) {
      thread.interrupt()
    }
    for (thread <- threads) {
      thread.join()
    }
  }

  override def register(systemStreamPartition: SystemStreamPartition, offset: String): Unit = {
    super.register(systemStreamPartition, offset)
    streamPartitions += systemStreamPartition
    setIsAtHead(systemStreamPartition, true)
  }

  class MockSystemConsumerRunnable(val streamPartitions: scala.collection.Set[SystemStreamPartition]) extends Runnable {

    override def run() {
      try {
        while (!Thread.interrupted() && streamPartitions.size > 0) {
          val streamPartitionsToFetch = streamPartitions.filter(getNumMessagesInQueue(_) <= 0)
          Thread.sleep(brokerSleepMs)
          for (streamPartition <- streamPartitionsToFetch) {
            for (i <- 0 until messagesPerBatch) {
              val key: Integer = getKey()
              put(streamPartition, new IncomingMessageEnvelope(streamPartition, "0", key, getMessage(key)))
            }
          }
        }
      } catch {
        case e: InterruptedException => println("Got interrupt. Shutting down.")
      }
    }
  }

}
