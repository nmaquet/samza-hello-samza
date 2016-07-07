package smurf.system

class SmurfMoodConsumer (messagesPerBatch: Int, threadCount: Int, brokerSleepMs: Int) extends
AbstractSmurfMetricConsumer(messagesPerBatch, threadCount, brokerSleepMs) {

  val MOODS = List(
    "Aggravated", "Alone", "Amused", "Angry", "Annoyed", "Anxious", "Apathetic", "Ashamed", "Awake",
    "Bewildered", "Bitchy", "Bittersweet", "Blah", "Blissful", "Bored", "Bouncy", "Calm", "Cheerful", "Cold",
    "Complacent", "Confused", "Content", "Cranky", "Crappy", "Crazy", "Crushed", "Curious", "Cynical", "Dark",
    "Depressed", "Determined", "Devious", "Dirty", "Disappointed", "Discontent", "Dorky", "Drained",
    "Drunk", "Ecstatic", "Energetic", "Enraged", "Enthralled", "Envious", "Excited", "Exhausted",
    "Flirty", "Frustrated", "Full", "Geeky", "Giddy", "Giggly", "Gloomy", "Good", "Grateful", "Groggy", "Grumpy",
    "Guilty", "Happy", "High", "Hopeful", "Hot", "Hungry", "Hyper", "Impressed", "Indescribable", "Indifferent",
    "Infuriated", "Irate", "Irritated", "Jealous", "Jubilant", "Lazy", "Lethargic", "Listless", "Lonely", "Loved",
    "Mad", "Melancholy", "Mellow", "Mischievous", "Moody", "Morose", "Naughty", "Nerdy", "Numb", "Okay", "Optimistic",
    "Peaceful", "Pessimistic", "Pissed off", "Pleased", "Predatory", "Quixotic", "Recumbent", "Refreshed", "Rejected",
    "Rejuvenated", "Relaxed", "Relieved", "Restless", "Rushed", "Sad", "Satisfied", "Shocked", "Sick", "Silly", "Sleepy",
    "Smart", "Stressed", "Surprised", "Sympathetic", "Thankful", "Tired", "Touched", "Uncomfortable", "Weird"
  )

  val random = new java.util.Random()

  val consumerPrefix: String = "SmurfMoodConsumer-"

  def getKey(): Integer = {
    Math.abs(random.nextInt()) % 10000000
  }

  def getMessage(key: Integer): java.util.Map[String, Object] = {
    val result = new java.util.HashMap[String, Object]()
    result.put("smurfId", key)
    result.put("mood", scala.util.Random.shuffle(MOODS).head)
    result.put("delta", new java.lang.Integer(random.nextInt() % 10))
    result
  }

}
