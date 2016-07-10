package com.miguno.kafkastorm.integration

import java.util.Properties

import _root_.storm.kafka.{KafkaSpout, SpoutConfig, ZkHosts}
import backtype.storm.generated.StormTopology
import backtype.storm.spout.SchemeAsMultiScheme
import backtype.storm.testing._
import backtype.storm.topology.TopologyBuilder
import backtype.storm.{Config, ILocalCluster, Testing}
import com.miguno.avro.Tweet
import com.miguno.kafkastorm.kafka._
import com.miguno.kafkastorm.logging.LazyLogging
import com.miguno.kafkastorm.storm.bolts.{AvroDecoderBolt, AvroKafkaSinkBolt}
import com.miguno.kafkastorm.storm.serialization.{AvroScheme, TweetAvroKryoDecorator}
import com.miguno.kafkastorm.testing.{EmbeddedKafkaZooKeeperCluster, KafkaTopic}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import kafka.message.MessageAndMetadata
import org.scalatest._

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.reflectiveCalls
import scala.util.Try

/**
 * This Kafka/Storm integration test code is slightly more complicated than the other tests in this project.  This is
 * due to a number of reasons, such as:  the way Storm topologies are "wired" and configured, the test facilities
 * exposed by Storm, and -- on a higher level -- because there are quite a number of components involved (ZooKeeper,
 * Kafka producers and consumers, Storm) which must be set up, run, and terminated in the correct order.  For these
 * reasons the integration tests are not simple "given/when/then" style tests.
 */
@DoNotDiscover
class KafkaStormSpec extends FeatureSpec with Matchers with BeforeAndAfterEach with GivenWhenThen with LazyLogging {

  implicit val specificAvroBinaryInjectionForTweet: Injection[Tweet, Array[Byte]] = SpecificAvroCodecs.toBinary[Tweet]

  private val inputTopic: KafkaTopic = KafkaTopic("testing-input")
  private val outputTopic: KafkaTopic = KafkaTopic("testing-output")
  private val kafkaZkCluster: EmbeddedKafkaZooKeeperCluster = new EmbeddedKafkaZooKeeperCluster(topics = Seq(inputTopic, outputTopic))

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

  info("As a user of Storm")
  info("I want to read Avro-encoded data from Kafka")
  info("so that I can quickly build Kafka<->Storm data flows")

  feature("AvroDecoderBolt[T]") {

    scenario("User creates a Storm topology that uses AvroDecoderBolt", IntegrationTest) {
      Given("a ZooKeeper instance")
      And("a Kafka broker instance")
      And(s"a Storm topology that uses AvroDecoderBolt and that reads tweets from topic ${inputTopic.name}} and " +
        s"writes them as-is to topic ${outputTopic.name}")
      // We create a topology instance that makes use of an Avro decoder bolt to deserialize the Kafka spout's output
      // into pojos.  Here, the data flow is KafkaSpout -> AvroDecoderBolt -> AvroKafkaSinkBolt.
      val builder: TopologyBuilder = new TopologyBuilder
      val kafkaSpoutId = "kafka-spout"
      val kafkaSpoutConfig: SpoutConfig = kafkaSpoutBaseConfig(kafkaZkCluster.zookeeper.connectString, inputTopic.name)
      val kafkaSpout: KafkaSpout = new KafkaSpout(kafkaSpoutConfig)
      val numSpoutExecutors = 1
      builder.setSpout(kafkaSpoutId, kafkaSpout, numSpoutExecutors)

      val decoderBoltId = "avro-decoder-bolt"
      val decoderBolt: AvroDecoderBolt[Tweet] = new AvroDecoderBolt[Tweet]
      // Note: Should test messages arrive out-of-order, we may want to enforce a parallelism of 1 for this bolt.
      builder.setBolt(decoderBoltId, decoderBolt).globalGrouping(kafkaSpoutId)

      val kafkaSinkBoltId = "avro-kafka-sink-bolt"
      val producerAppFactory: BaseKafkaProducerAppFactory =
        new BaseKafkaProducerAppFactory(kafkaZkCluster.kafka.brokerList, defaultTopic = Option(outputTopic.name))
      val kafkaSinkBolt: AvroKafkaSinkBolt[Tweet] = new AvroKafkaSinkBolt[Tweet](producerAppFactory)
      // Note: Should test messages arrive out-of-order, we may want to enforce a parallelism of 1 for this bolt.
      builder.setBolt(kafkaSinkBoltId, kafkaSinkBolt).globalGrouping(decoderBoltId)
      val topology: StormTopology = builder.createTopology()

      baseIntegrationTest(topology, inputTopic.name, outputTopic.name)
    }
  }

  feature("AvroScheme[T] for Kafka spout") {
    scenario("User creates a Storm topology that uses AvroScheme in Kafka spout", IntegrationTest) {
      Given("a ZooKeeper instance")
      And("a Kafka broker instance")
      And(s"a Storm topology that uses AvroScheme and that reads tweets from topic ${inputTopic.name} and writes " +
        s"them as-is to topic ${outputTopic.name}")
      // Creates a topology instance that adds an Avro decoder "scheme" to the Kafka spout, so that the spout's
      // output are ready-to-use pojos.  Here, the data flow is KafkaSpout -> AvroKafkaSinkBolt.
      //
      // Note that Storm will still need to re-serialize the spout's pojo output to send the data across the wire to
      // downstream consumers/bolts, which will then deserialize the data again.  In our case we have a custom Kryo
      // serializer registered with Storm to make this serde step as fast as possible.
      val builder: TopologyBuilder = new TopologyBuilder
      val kafkaSpoutId = "kafka-spout"
      val kafkaSpoutConfig: SpoutConfig = kafkaSpoutBaseConfig(kafkaZkCluster.zookeeper.connectString, inputTopic.name)
      // You can provide the Kafka spout with a custom `Scheme` to deserialize incoming messages in a particular way.
      // The default scheme is Storm's `backtype.storm.spout.RawMultiScheme`, which simply returns the raw bytes of the
      // incoming data (i.e. leaving deserialization up to you).  In this example, we configure the spout to use
      // a custom scheme, AvroScheme[Tweet], which will modify the spout to automatically deserialize incoming data
      // into pojos.
      kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new AvroScheme[Tweet])
      val kafkaSpout: KafkaSpout = new KafkaSpout(kafkaSpoutConfig)
      val numSpoutExecutors = 1
      builder.setSpout(kafkaSpoutId, kafkaSpout, numSpoutExecutors)

      val kafkaSinkBoltId = "avro-kafka-sink-bolt"
      val producerAppFactory: BaseKafkaProducerAppFactory =
        new BaseKafkaProducerAppFactory(kafkaZkCluster.kafka.brokerList, defaultTopic = Option(outputTopic.name))
      val kafkaSinkBolt: AvroKafkaSinkBolt[Tweet] = new AvroKafkaSinkBolt[Tweet](producerAppFactory)
      // Note: Should test messages arrive out-of-order, we may want to enforce a parallelism of 1 for this bolt.
      builder.setBolt(kafkaSinkBoltId, kafkaSinkBolt).globalGrouping(kafkaSpoutId)
      val topology: StormTopology = builder.createTopology()

      baseIntegrationTest(topology, inputTopic.name, outputTopic.name)
    }
  }

  private def kafkaSpoutBaseConfig(zookeeperConnect: String, inputTopic: String): SpoutConfig = {
    val zkHosts: ZkHosts = new ZkHosts(zookeeperConnect)
    val zkRoot = "/kafka-storm-starter-spout"
    //  This id is appended to zkRoot for constructing a ZK path under which the spout stores partition information.
    val zkId = "kafka-spout"
    // To configure the spout to read from the very beginning of the topic (auto.offset.reset = smallest), you can use
    // either of the following two equivalent approaches:
    //
    //    1. spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime
    //    2. spoutConfig.forceFromStart = true
    //
    // To configure the spout to read from the end of the topic (auto.offset.reset = largest), you can use either of
    // the following two equivalent approaches:
    //
    //    1. Do nothing -- reading from the end of the topic is the default behavior.
    //    2. spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime
    //
    val spoutConfig: SpoutConfig = new SpoutConfig(zkHosts, inputTopic, zkRoot, zkId)
    spoutConfig
  }


  /**
   * This method sends Avro-encoded test data into a Kafka "input" topic.  This data is read from Kafka into Storm,
   * which will then decode and re-encode the data, and then write the data to an "output" topic in Kafka (which is our
   * means/workaround to "tap into" Storm's output, as we haven't been able yet to use Storm's built-in testing
   * facilities for such integration tests).  Lastly, we read the data from the "output" topic via a Kafka consumer
   * group, and then compare the output data with the input data, with the latter serving the dual purpose of also
   * being the expected output data.
   */
  private def baseIntegrationTest(topology: StormTopology, inputTopic: String, outputTopic: String) {
    And("some tweets")
    val f: Object {val messages: Seq[Tweet]; val t1: Tweet; val t3: Tweet; val t2: Tweet} = fixture
    val tweets: Seq[Tweet] = f.messages

    And(s"a synchronous Kafka producer app that writes to the topic $inputTopic")
    val producerApp: KafkaProducerApp = {
      val config: Properties = {
        val c: Properties = new Properties
        c.put("producer.type", "sync")
        c.put("client.id", "kafka-storm-test-sync-producer")
        c.put("request.required.acks", "1")
        c
      }
      kafkaZkCluster.createProducer(inputTopic, config).get
    }

    And(s"a single-threaded Kafka consumer app that reads from topic $outputTopic and Avro-decodes the incoming data")
    // We start the Kafka consumer group, which (in our case) must be running before the first messages are being sent
    // to the output Kafka topic.  The Storm topology will write its output to this topic.  We use the Kafka consumer
    // group to learn which data was created by Storm, and compare this actual output data to the expected data (which
    // in our case is the original input data).
    val actualTweets: mutable.SynchronizedQueue[Tweet] = new mutable.SynchronizedQueue[Tweet]
    def consume(m: MessageAndMetadata[Array[Byte], Array[Byte]], c: ConsumerTaskContext) {
      val tweet: Try[Tweet] = Injection.invert[Tweet, Array[Byte]](m.message())
      for {t <- tweet} {
        logger.info(s"Consumer thread ${c.threadId}: received Tweet $t from ${m.topic}:${m.partition}:${m.offset}")
        actualTweets += t
      }
    }
    kafkaZkCluster.createAndStartConsumer(outputTopic, consume)

    And("a Storm topology configuration that registers an Avro Kryo decorator for Tweet")
    // We create the topology configuration here simply to clarify that it is part of the test's initial context defined
    // under "Given".
    val topologyConfig: Config = {
      val conf: Config = new Config
      // Use more than one worker thread.  It looks as if serialization occurs only if you have actual parallelism in
      // LocalCluster (i.e. numWorkers > 1).
      conf.setNumWorkers(2)
      // Never use Java's default serialization.  This allows us to see whether Kryo serialization is properly
      // configured and working for all types.
      conf.setFallBackOnJavaSerialization(false)
      // Serialization config, see http://storm.incubator.apache.org/documentation/Serialization.html
      // Note: We haven't been able yet to come up with a KryoDecorator[Tweet] approach.
      conf.registerDecorator(classOf[TweetAvroKryoDecorator])
      conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 0: Integer)
      conf
    }

    When("I run the Storm topology")
    val stormTestClusterParameters: MkClusterParam = {
      val mkClusterParam: MkClusterParam = new MkClusterParam
      mkClusterParam.setSupervisors(2)
      val stormClusterConfig: Config = new Config

      // (requires Storm 0.9.3+ with STORM-213):
      // Storm shall use our existing in-memory ZK instance instead of starting its own, which it does by default as
      // part of its Testing API workflow (which relies on LocalCluster).  Using the same ZK instance for Kafka and
      // Storm is a setup often used in production, hence we can use this example to test such setups.  Also, the shared
      // ZK setup means our tests run slightly faster.
      import scala.collection.JavaConverters._
      stormClusterConfig.put(Config.STORM_ZOOKEEPER_SERVERS, List(kafkaZkCluster.zookeeper.hostname).asJava)
      stormClusterConfig.put(Config.STORM_ZOOKEEPER_PORT, kafkaZkCluster.zookeeper.port: Integer)

      mkClusterParam.setDaemonConf(stormClusterConfig)
      mkClusterParam
    }
    Testing.withLocalCluster(stormTestClusterParameters, new TestJob() {
      override def run(stormCluster: ILocalCluster) {
        val topologyName = "storm-kafka-integration-test"
        stormCluster.submitTopology(topologyName, topologyConfig, topology)
        val waitForTopologyStartup: FiniteDuration = 5.seconds
        Thread.sleep(waitForTopologyStartup.toMillis)

        And("I Avro-encode the tweets and use the Kafka producer app to sent them to Kafka")
        tweets foreach {
          case tweet =>
            val bytes: Array[Byte] = Injection[Tweet, Array[Byte]](tweet)
            info(s"Synchronously sending Tweet $tweet to topic ${producerApp.defaultTopic}")
            producerApp.send(bytes)
        }

        val waitForStormToReadFromKafka: FiniteDuration = 3.second
        Thread.sleep(waitForStormToReadFromKafka.toMillis)
      }
    })

    Then("the Kafka consumer app should receive the original tweets from the Storm topology")
    val waitForConsumerToReadStormOutput: FiniteDuration = 300.millis
    Thread.sleep(waitForConsumerToReadStormOutput.toMillis)
    actualTweets.toSeq should be(tweets.toSeq)
  }

}