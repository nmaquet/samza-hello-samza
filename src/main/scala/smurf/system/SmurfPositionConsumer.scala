package smurf.system

class SmurfPositionConsumer(messagesPerBatch: Int, threadCount: Int, brokerSleepMs: Int) extends
  AbstractSmurfMetricConsumer(messagesPerBatch, threadCount, brokerSleepMs) {

  val random = new java.util.Random()

  val consumerPrefix: String = "SmurfPositionConsumer-"

  def getKey(): Integer = {
    Math.abs(random.nextInt()) % 10000000
  }

  def getMessage(key: Integer): java.util.Map[String, Object] = {
    val result = new java.util.HashMap[String, Object]()
    result.put("smurfId", key)
    result.put("deltaX", new java.lang.Integer(random.nextInt() % 5))
    result.put("deltaY", new java.lang.Integer(random.nextInt() % 5))
    result
  }

}
