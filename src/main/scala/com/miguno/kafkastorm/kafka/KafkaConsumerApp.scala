package com.miguno.kafkastorm.kafka

import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors}

import com.miguno.kafkastorm.logging.LazyLogging
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector, KafkaStream}
import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder

import scala.collection.Map
import scala.trace.{Pos, implicitlyFormatable}

/**
 * Demonstrates how to implement a simple Kafka consumer application to read data from Kafka.
 *
 * Don't read too much into the actual implementation of this class.  Its sole purpose is to showcase the use of the
 * Kafka API.
 *
 * @param topic The Kafka topic to read data from.
 * @param zookeeperConnect The ZooKeeper connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
 *                         Example: `127.0.0.1:2181`.
 * @param numStreams The number of Kafka streams to create for consuming the topic.  Currently, each stream will be
 *                   consumed by its own dedicated thread (1:1 mapping), but this behavior is not guaranteed in the
 *                   future.
 * @param config Additional consumer configuration settings.
 */
class KafkaConsumerApp[T](val topic: String,
                          val zookeeperConnect: String,
                          val numStreams: Int,
                          config: Properties = new Properties) extends LazyLogging {

  private val effectiveConfig: Properties = {
    val c: Properties = new Properties
    c.load(this.getClass.getResourceAsStream("/consumer-defaults.properties"))
    c.putAll(config)
    c.put("zookeeper.connect", zookeeperConnect)
    c
  }

  private val executor: ExecutorService = Executors.newFixedThreadPool(numStreams)
  private val consumerConnector: ConsumerConnector = Consumer.create(new ConsumerConfig(effectiveConfig))

  logger.info((s"Connecting to topic $topic via ZooKeeper $zookeeperConnect" + Pos()).wrap)

  def startConsumers(f: (MessageAndMetadata[Array[Byte], Array[Byte]], ConsumerTaskContext, Option[T]) => Unit,
                     startup: (ConsumerTaskContext) => Option[T] = (c: ConsumerTaskContext) => None,
                     shutdown: (ConsumerTaskContext, Option[T]) => Unit = (c: ConsumerTaskContext, t: Option[T]) => ()) {
    val topicCountMap: Map[String, Int] = Map(topic -> numStreams)
    val valueDecoder: DefaultDecoder = new DefaultDecoder
    val keyDecoder: DefaultDecoder = valueDecoder
    val consumerMap: Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] = consumerConnector.createMessageStreams(topicCountMap, keyDecoder, valueDecoder)
    val consumerThreads: Seq[ConsumerTask[Array[Byte], Array[Byte], T, ConsumerTaskContext]] = consumerMap.get(topic) match {
      case Some(streams) => streams.view.zipWithIndex map {
        case (stream, threadId) =>
          new ConsumerTask(stream, new ConsumerTaskContext(threadId, effectiveConfig), f, startup, shutdown)
      }
      case _ => Seq()
    }
    consumerThreads foreach executor.submit
  }

  def shutdown() {
    logger.debug(("Shutting down Kafka consumer connector" + Pos()).wrap)
    consumerConnector.shutdown()
    logger.debug(("Shutting down thread pool of consumer tasks" + Pos()).wrap)
    executor.shutdown()
  }

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run() {
      shutdown()
    }
  })

}

class ConsumerTask[K, V, T, C <: ConsumerTaskContext](stream: KafkaStream[K, V],
                                                      context: C,
                                                      f: (MessageAndMetadata[K, V], C, Option[T]) => Unit,
                                                      startup: (C) => Option[T],
                                                      shutdown: (C, Option[T]) => Unit)
  extends Runnable with LazyLogging {

  private var t: Option[T] = _

  @volatile private var shutdownAlreadyRanOnce = false

  override def run() {
    logger.debug((s"Consumer task of thread ${context.threadId} entered run()" + Pos()).wrap)
    t = startup(context)
    try {
      stream foreach {
        case msg: MessageAndMetadata[_, _] =>
          logger.trace((s"Thread ${context.threadId} received message: " + msg + Pos()).wrap)
          f(msg, context, t)
        case _ => logger.trace(s"Received unexpected message type from broker")
      }
      gracefulShutdown()
    }
    catch {
      case e: InterruptedException => {
        logger.debug((s"Consumer task of thread ${context.threadId} was interrupted" + Pos()).wrap)
      }
    }
  }

  def gracefulShutdown() {
    if (!shutdownAlreadyRanOnce) {
      logger.debug(("Performing graceful shutdown" + Pos()).wrap)
      shutdownAlreadyRanOnce = true
      shutdown(context, t)
    }
    else logger.debug(("Graceful shutdown requested but it already ran once, so it will not be run again." +
      Pos()).wrap)
  }

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run() {
      logger.debug(("Shutdown hook triggered!" + Pos()).wrap)
      gracefulShutdown()
    }
  })

}

case class ConsumerTaskContext(threadId: Int, config: Properties)