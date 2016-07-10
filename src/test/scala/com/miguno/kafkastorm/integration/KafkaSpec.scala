package com.miguno.kafkastorm.integration

import java.util.Properties

import _root_.kafka.message.MessageAndMetadata
import com.miguno.avro.Tweet
import com.miguno.kafkastorm.kafka.{ConsumerTaskContext, KafkaProducerApp}
import com.miguno.kafkastorm.logging.LazyLogging
import com.miguno.kafkastorm.testing.{EmbeddedKafkaZooKeeperCluster, KafkaTopic}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import org.scalatest._

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.reflectiveCalls
import scala.util.Try

@DoNotDiscover
class KafkaSpec extends FunSpec with Matchers with BeforeAndAfterEach with GivenWhenThen with LazyLogging {

  implicit val specificAvroBinaryInjectionForTweet: Injection[Tweet, Array[Byte]] = SpecificAvroCodecs.toBinary[Tweet]

  private val topic: KafkaTopic = KafkaTopic("testing")
  private val kafkaZkCluster: EmbeddedKafkaZooKeeperCluster = new EmbeddedKafkaZooKeeperCluster(topics = Seq(topic))

  override def beforeEach() {
    kafkaZkCluster.start()
  }

  override def afterEach() {
    kafkaZkCluster.stop()
  }

  val fixture: Object {val messages: Seq[Tweet]; val t1: Tweet; val t3: Tweet; val t2: Tweet} = {
    val BeginningOfEpoch: FiniteDuration = 0.seconds
    val AnyTimestamp: FiniteDuration = 1234.seconds
    val now: FiniteDuration = System.currentTimeMillis().millis

    new {
      val t1: Tweet = new Tweet("ANY_USER_1", "ANY_TEXT_1", now.toSeconds)
      val t2: Tweet = new Tweet("ANY_USER_2", "ANY_TEXT_2", BeginningOfEpoch.toSeconds)
      val t3: Tweet = new Tweet("ANY_USER_3", "ANY_TEXT_3", AnyTimestamp.toSeconds)

      val messages: Seq[Tweet] = Seq(t1, t2, t3)
    }
  }

  describe("Kafka") {

    it("should synchronously send and receive a Tweet in Avro format", IntegrationTest) {
      Given("a ZooKeeper instance")
      And("a Kafka broker instance")
      And("some tweets")
      val tweets: Seq[Tweet] = fixture.messages
      And("a single-threaded Kafka consumer group")
      // The Kafka consumer group must be running before the first messages are being sent to the topic.
      val actualTweets: mutable.SynchronizedQueue[Tweet] = new mutable.SynchronizedQueue[Tweet]
      def consume(m: MessageAndMetadata[Array[Byte], Array[Byte]], c: ConsumerTaskContext): Unit = {
        val tweet: Try[Tweet] = Injection.invert[Tweet, Array[Byte]](m.message)
        for {t <- tweet} {
          logger.info(s"Consumer thread ${c.threadId}: received Tweet $t from ${m.topic}:${m.partition}:${m.offset}")
          actualTweets += t
        }
      }
      kafkaZkCluster.createAndStartConsumer(topic.name, consume)

      When("I start a synchronous Kafka producer that sends the tweets in Avro binary format")
      val producerApp: KafkaProducerApp = {
        val c: Properties = new Properties
        c.put("producer.type", "sync")
        c.put("client.id", "test-sync-producer")
        c.put("request.required.acks", "1")
        kafkaZkCluster.createProducer(topic.name, c).get
      }
      tweets foreach {
        case tweet =>
          val bytes: Array[Byte] = Injection[Tweet, Array[Byte]](tweet)
          logger.info(s"Synchronously sending Tweet $tweet to topic ${producerApp.defaultTopic}")
          producerApp.send(bytes)
      }

      Then("the consumer app should receive the tweets")
      val waitForConsumerToReadStormOutput: FiniteDuration = 300.millis
      logger.debug(s"Waiting $waitForConsumerToReadStormOutput for Kafka consumer threads to read messages")
      Thread.sleep(waitForConsumerToReadStormOutput.toMillis)
      logger.debug("Finished waiting for Kafka consumer threads to read messages")
      actualTweets.toSeq should be(tweets)
    }

    it("should asynchronously send and receive a Tweet in Avro format", IntegrationTest) {
      Given("a ZooKeeper instance")
      And("a Kafka broker instance")
      And("some tweets")
      val tweets: Seq[Tweet] = fixture.messages
      And("a single-threaded Kafka consumer group")
      // The Kafka consumer group must be running before the first messages are being sent to the topic.
      val actualTweets: mutable.SynchronizedQueue[Tweet] = new mutable.SynchronizedQueue[Tweet]
      def consume(m: MessageAndMetadata[Array[Byte], Array[Byte]], c: ConsumerTaskContext) {
        val tweet: Try[Tweet] = Injection.invert[Tweet, Array[Byte]](m.message())
        for {t <- tweet} {
          logger.info(s"Consumer thread ${c.threadId}: received Tweet $t from ${m.topic}:${m.partition}:${m.offset}")
          actualTweets += t
        }
      }
      kafkaZkCluster.createAndStartConsumer(topic.name, consume)

      val waitForConsumerStartup: FiniteDuration = 300.millis
      logger.debug(s"Waiting $waitForConsumerStartup for Kafka consumer threads to launch")
      Thread.sleep(waitForConsumerStartup.toMillis)
      logger.debug("Finished waiting for Kafka consumer threads to launch")

      When("I start an asynchronous Kafka producer that sends the tweets in Avro binary format")
      val producerApp: KafkaProducerApp = {
        val asyncConfig: Properties = {
          val c: Properties = new Properties
          c.put("producer.type", "async")
          c.put("client.id", "test-sync-producer")
          c.put("request.required.acks", "1")
          // We must set `batch.num.messages` and/or `queue.buffering.max.ms` so that the async producer will actually
          // send our (typically few) test messages before the unit test finishes.
          c.put("batch.num.messages", tweets.size.toString)
          c
        }
        kafkaZkCluster.createProducer(topic.name, asyncConfig).get
      }
      tweets foreach {
        case tweet =>
          val bytes: Array[Byte] = Injection[Tweet, Array[Byte]](tweet)
          logger.info(s"Asynchronously sending Tweet $tweet to topic ${producerApp.defaultTopic}")
          producerApp.send(bytes)
      }

      Then("the consumer app should receive the tweets")
      val waitForConsumerToReadStormOutput: FiniteDuration = 300.millis
      logger.debug(s"Waiting $waitForConsumerToReadStormOutput for Kafka consumer threads to read messages")
      Thread.sleep(waitForConsumerToReadStormOutput.toMillis)
      logger.debug("Finished waiting for Kafka consumer threads to read messages")
      actualTweets.toSeq should be(tweets)
    }

  }

}