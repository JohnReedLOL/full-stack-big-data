package com.miguno.kafkastorm.kafka

import java.io.File
import java.util.Properties

import com.miguno.kafkastorm.logging.LazyLogging
import kafka.admin.AdminUtils
import kafka.server.{KafkaConfig, KafkaServerStartable}
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.io.FileUtils
import scala.concurrent.duration._
import scala.trace.{Pos, implicitlyFormatable}

/**
 * Runs an in-memory, "embedded" instance of a Kafka broker, which listens at `127.0.0.1:9092` by default.
 *
 * Requires a running ZooKeeper instance to connect to.  By default, it expects a ZooKeeper instance running at
 * `127.0.0.1:2181`.  You can specify a different ZooKeeper instance by setting the `zookeeper.connect` parameter in the
 * broker's configuration.
 *
 * @param config Broker configuration settings.  Used to modify, for example, on which port the broker should listen to.
 *               Note that you cannot change the `log.dirs` setting currently.
 */
class KafkaEmbedded(config: Properties = new Properties) extends LazyLogging {

  private val defaultZkConnect = "127.0.0.1:2181"
  private val logDir: File = {
    val random: Int = (new scala.util.Random).nextInt()
    val path: String = Seq(System.getProperty("java.io.tmpdir"), "kafka-test", "logs-" + random).mkString(File.separator)
    new File(path)
  }

  private val effectiveConfig: Properties = {
    val c: Properties = new Properties
    c.load(this.getClass.getResourceAsStream("/broker-defaults.properties"))
    c.putAll(config)
    c.setProperty("log.dirs", logDir.getAbsolutePath)
    c
  }

  private val kafkaConfig: KafkaConfig = new KafkaConfig(effectiveConfig)
  private val kafka: KafkaServerStartable = new KafkaServerStartable(kafkaConfig)

  /**
   * This broker's `metadata.broker.list` value.  Example: `127.0.0.1:9092`.
   *
   * You can use this to tell Kafka producers and consumers how to connect to this instance.
   */
  val brokerList: String = kafka.serverConfig.hostName + ":" + kafka.serverConfig.port

  /**
   * The ZooKeeper connection string aka `zookeeper.connect`.
   */
  val zookeeperConnect: String = {
    val zkConnectLookup: Option[String] = Option(effectiveConfig.getProperty("zookeeper.connect"))
    zkConnectLookup match {
      case Some(zkConnect) => zkConnect
      case _ =>
        logger.warn((s"zookeeper.connect is not configured -- falling back to default setting $defaultZkConnect" +
          Pos()).wrap)
        defaultZkConnect
    }
  }

  /**
   * Start the broker.
   */
  def start() {
    logger.debug((s"Starting embedded Kafka broker at $brokerList (with ZK server at $zookeeperConnect) ..." +
      Pos()).wrap)
    kafka.startup()
    logger.debug((s"Startup of embedded Kafka broker at $brokerList completed (with ZK server at $zookeeperConnect)" +
      Pos()).wrap)
  }

  /**
   * Stop the broker.
   */
  def stop() {
    logger.debug((s"Shutting down embedded Kafka broker at $brokerList (with ZK server at $zookeeperConnect)..." +
      Pos()).wrap)
    kafka.shutdown()
    FileUtils.deleteQuietly(logDir)
    logger.debug((s"Shutdown of embedded Kafka broker at $brokerList completed (with ZK server at $zookeeperConnect)" +
      Pos()).wrap)
  }

  def createTopic(topic: String, partitions: Int = 1, replicationFactor: Int = 1, config: Properties = new Properties): Unit = {
    logger.debug(
      (s"Creating topic { name: $topic, partitions: $partitions, replicationFactor: $replicationFactor, config: $config }" +
        Pos()).wrap)
    val sessionTimeout: FiniteDuration = 10.seconds
    val connectionTimeout: FiniteDuration = 8.seconds
    // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then createTopic() will only
    // seem to work (it will return without error).  Topic will exist in only ZooKeeper, and will be returned when
    // listing topics, but Kafka itself does not create the topic.
    val zkClient: ZkClient = new ZkClient(zookeeperConnect, sessionTimeout.toMillis.toInt, connectionTimeout.toMillis.toInt,
      ZKStringSerializer)
    AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor, config)
    zkClient.close()
  }

}