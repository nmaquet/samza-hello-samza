package demo.task

import org.apache.samza.system.{IncomingMessageEnvelope, OutgoingMessageEnvelope}
import org.apache.samza.task.{MessageCollector, StreamTask, TaskCoordinator}
import org.apache.samza.util.{Logging, Util}

class StringSmurferTask extends StreamTask with Logging {

  val WORDS = List("move", "go", "set", "get", "do", "make", "use")

  val outputStreamName = "kafka.smurfed-text"

  override def process(envelope: IncomingMessageEnvelope,
                       collector: MessageCollector,
                       coordinator: TaskCoordinator): Unit = {
    val message = envelope.getMessage.asInstanceOf[String]
    collector.send(new OutgoingMessageEnvelope(Util.getSystemStreamFromNames(outputStreamName), smurf(message)))
  }

  def smurf(message: String): String = {
    var result = message
    for (word <- WORDS) {
      result = result.replaceAll("(\\s+)" + word + "(\\s+)", "$1smurf$2")
    }
    result
  }

}
