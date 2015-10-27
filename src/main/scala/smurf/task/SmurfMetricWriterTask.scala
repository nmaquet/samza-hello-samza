package smurf.task

import org.apache.samza.task.TaskContext
import org.apache.samza.task.InitableTask
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.task.MessageCollector
import org.apache.samza.task.StreamTask
import org.apache.samza.task.TaskCoordinator
import org.apache.samza.task.TaskCoordinator.RequestScope
import org.apache.samza.config.Config
import org.apache.samza.util.{Util, Logging}
import org.apache.samza.system.SystemStream
import org.apache.samza.system.OutgoingMessageEnvelope

object SmurfMetricWriterTask {
  var messagesProcessed = 0
  var startTime = 0L
}

class SmurfMetricWriterTask extends StreamTask with InitableTask with Logging {

  import SmurfMetricWriterTask._

  var logInterval = 10000

  var maxMessages = 10000000

  var outputSystemStream: Option[SystemStream] = None

  def init(config: Config, context: TaskContext) {
    logInterval = config.getInt("task.log.interval", 10000)
    maxMessages = config.getInt("task.max.messages", 10000000)
    outputSystemStream = Option(config.get("task.outputs", null)).map(Util.getSystemStreamFromNames)
  }

  override def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
    try {
      processEnvelope(envelope, collector, coordinator)
    }
    catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  private def processEnvelope(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
    if (startTime == 0) {
      startTime = System.currentTimeMillis
    }

    if (outputSystemStream.isDefined) {
      collector.send(new OutgoingMessageEnvelope(outputSystemStream.get, envelope.getKey, envelope.getMessage))
    }

    messagesProcessed += 1

    if (messagesProcessed % logInterval == 0) {
      val seconds = (System.currentTimeMillis - startTime) / 1000
      println("Processed %s messages in %s seconds." format(messagesProcessed, seconds))
    }

    if (messagesProcessed >= maxMessages) {
      coordinator.shutdown(RequestScope.ALL_TASKS_IN_CONTAINER)
    }
  }
}