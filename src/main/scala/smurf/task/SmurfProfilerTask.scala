package smurf.task

import org.apache.samza.config.Config
import org.apache.samza.metrics.Counter
import org.apache.samza.storage.kv.KeyValueStore
import org.apache.samza.system.{IncomingMessageEnvelope, OutgoingMessageEnvelope, SystemStream}
import org.apache.samza.task.{InitableTask, WindowableTask, MessageCollector, StreamTask, TaskContext, TaskCoordinator}
import org.apache.samza.util.{Logging, Util}

class SmurfProfilerTask extends StreamTask with InitableTask with WindowableTask with Logging {

  var outputSystemStream: Option[SystemStream] = None
  var store: KeyValueStore[Integer, java.util.Map[String, Object]] = _
  var processedPositionUpdates: Counter = null
  var processedMoodUpdates: Counter = null
  var processCount = 0

  def init(config: Config, context: TaskContext) {
    info("starting init...")
    outputSystemStream = Option(config.get("task.outputs", null)).map(Util.getSystemStreamFromNames)
    store = context.getStore("smurf-store").asInstanceOf[KeyValueStore[Integer, java.util.Map[String, Object]]]
    processedPositionUpdates = context.getMetricsRegistry.newCounter(getClass.getName, "processed-position-updates")
    processedMoodUpdates = context.getMetricsRegistry.newCounter(getClass.getName, "processed-mood-updates")
    info("init done")
  }

  override def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator): Unit = {
    processEnvelope(envelope, collector, coordinator)
    processCount += 1
  }

  private def processEnvelope(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator): Unit = {
    if (outputSystemStream.isDefined) {
      val key = envelope.getKey.asInstanceOf[Integer]
      val message = envelope.getMessage.asInstanceOf[java.util.Map[String, Object]]
      envelope.getSystemStreamPartition.getStream match {
        case "smurf-position" => processPosition(key, message, collector) ; processedPositionUpdates.inc()
        case "smurf-mood" => processMood(key, message, collector) ; processedMoodUpdates.inc()
      }
      collector.send(new OutgoingMessageEnvelope(outputSystemStream.get, key, store.get(key)))
    }
  }

  private def processPosition(key: Integer, message: java.util.Map[String, Object], collector: MessageCollector): Unit = {
    val smurf: java.util.Map[String, Object] = Option(store.get(key)).getOrElse(new java.util.HashMap())
    val x: java.lang.Integer = Option(smurf.get("x").asInstanceOf[java.lang.Integer]).getOrElse(0)
    val y: java.lang.Integer = Option(smurf.get("y").asInstanceOf[java.lang.Integer]).getOrElse(0)
    val newX: java.lang.Integer = x + message.get("deltaX").asInstanceOf[java.lang.Integer]
    val newY: java.lang.Integer = y + message.get("deltaY").asInstanceOf[java.lang.Integer]
    smurf.put("x", newX)
    smurf.put("y", newY)
    smurf.put("smurfId", key)
    store.put(key, smurf)
  }

  private def processMood(key: Integer, message: java.util.Map[String, Object], collector: MessageCollector): Unit = {
    val smurf: java.util.Map[String, Object] = Option(store.get(key)).getOrElse(new java.util.HashMap())
    val mood: String = message.get("mood").asInstanceOf[String]
    val delta: Integer = message.get("delta").asInstanceOf[Integer]
    val current: Integer = Option(smurf.get(mood).asInstanceOf[java.lang.Integer]).getOrElse(0)
    val newValue: Integer = current + delta
    smurf.put(mood, newValue)
    smurf.put("smurfId", key)
    store.put(key, smurf)
  }

  override def window(messageCollector: MessageCollector, taskCoordinator: TaskCoordinator): Unit = {
    info(s"processed $processCount in 10s (${processCount / 10.0} / s)")
    processCount = 0
  }
}
