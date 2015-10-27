package smurf.task

import org.apache.samza.config.Config
import org.apache.samza.storage.kv.KeyValueStore
import org.apache.samza.system.{IncomingMessageEnvelope, OutgoingMessageEnvelope, SystemStream}
import org.apache.samza.task.{InitableTask, MessageCollector, StreamTask, TaskContext, TaskCoordinator}
import org.apache.samza.util.{Logging, Util}

class SmurfProfilerTask extends StreamTask with InitableTask with Logging {

  var outputSystemStream: Option[SystemStream] = None
  var store: KeyValueStore[Integer, java.util.Map[String, Object]] = _

  def init(config: Config, context: TaskContext) {
    outputSystemStream = Option(config.get("task.outputs", null)).map(Util.getSystemStreamFromNames)
    store = context.getStore("smurf-store").asInstanceOf[KeyValueStore[Integer, java.util.Map[String, Object]]]
  }

  override def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator): Unit = {
    try {
      processEnvelope(envelope, collector, coordinator)
    }
    catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  private def processEnvelope(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator): Unit = {
    if (outputSystemStream.isDefined) {
      val key = envelope.getKey.asInstanceOf[Integer]
      val message = envelope.getMessage.asInstanceOf[java.util.Map[String, Object]]
      envelope.getSystemStreamPartition.getStream match {
        case "smurf-position" => processPosition(key, message, collector)
        case "smurf-mood" => processMood(key, message, collector)
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
    store.put(key, smurf)
  }

  private def processMood(key: Integer, message: java.util.Map[String, Object], collector: MessageCollector): Unit = {
    val smurf: java.util.Map[String, Object] = Option(store.get(key)).getOrElse(new java.util.HashMap())
    val mood: String = message.get("mood").asInstanceOf[String]
    val delta: Integer = message.get("delta").asInstanceOf[Integer]
    val current: Integer = Option(smurf.get(mood).asInstanceOf[java.lang.Integer]).getOrElse(0)
    val newValue: Integer = current + delta
    smurf.put(mood, newValue)
    store.put(key, smurf)
  }
}