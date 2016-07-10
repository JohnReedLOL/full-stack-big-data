package com.miguno.kafkastorm.storm.bolts

import java.util

import backtype.storm.task.TopologyContext
import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.tuple.Tuple
import com.miguno.avro.Tweet
import com.miguno.kafkastorm.kafka.{KafkaProducerApp, KafkaProducerAppFactory}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import org.mockito.AdditionalMatchers
import org.mockito.Mockito.{when => mwhen, _}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSpec, GivenWhenThen, Matchers}

import scala.concurrent.duration._

class AvroKafkaSinkBoltSpec extends FunSpec with Matchers with GivenWhenThen with MockitoSugar {

  implicit val specificAvroBinaryInjection: Injection[Tweet, Array[Byte]] = SpecificAvroCodecs.toBinary[Tweet]

  private type AnyAvroSpecificRecordBase = Tweet

  private val AnyTweet: AnyAvroSpecificRecordBase = new Tweet("ANY_USER_1", "ANY_TEXT_1", 1234.seconds.toSeconds)
  private val AnyTweetInAvroBytes: Array[Byte] = Injection[Tweet, Array[Byte]](AnyTweet)
  private val DummyStormConf: util.HashMap[Object, Object] = new util.HashMap[Object, Object]
  private val DummyStormContext: TopologyContext = mock[TopologyContext]

  describe("An AvroKafkaSinkBolt") {

    it("should send pojos of the configured type to Kafka in Avro-encoded binary format") {
      Given("a bolt for type Tweet")
      val producerApp: KafkaProducerApp = mock[KafkaProducerApp]
      val producerAppFactory: KafkaProducerAppFactory = mock[KafkaProducerAppFactory]
      mwhen(producerAppFactory.newInstance()).thenReturn(producerApp)
      val bolt: AvroKafkaSinkBolt[AnyAvroSpecificRecordBase] = new AvroKafkaSinkBolt[Tweet](producerAppFactory)
      bolt.prepare(DummyStormConf, DummyStormContext)

      When("it receives a Tweet pojo")
      val tuple: Tuple = mock[Tuple]
      // The `Nil: _*` is required workaround because of a known Scala-Java interop problem related to Scala's treatment
      // of Java's varargs.  See http://stackoverflow.com/a/13361530/1743580.
      mwhen(tuple.getValueByField("pojo")).thenReturn(AnyTweet, Nil: _*)
      val collector: BasicOutputCollector = mock[BasicOutputCollector]
      bolt.execute(tuple, collector)

      Then("it should send the Avro-encoded pojo to Kafka")
      // Note: The simpler Mockito variant of `verify(kafkaProducer).send(AnyTweetInAvroBytes)` is not enough because
      // this variant will not verify whether the Array[Byte] parameter passed to `send()` has the correct value.
      verify(producerApp).send(AdditionalMatchers.aryEq(AnyTweetInAvroBytes))
      And("it should not send any data to downstream bolts")
      verifyZeroInteractions(collector)
    }

    it("should ignore pojos of an unexpected type") {
      Given("a bolt for type Tweet")
      val producerApp: KafkaProducerApp = mock[KafkaProducerApp]
      val producerAppFactory: KafkaProducerAppFactory = mock[KafkaProducerAppFactory]
      mwhen(producerAppFactory.newInstance()).thenReturn(producerApp)
      val bolt: AvroKafkaSinkBolt[AnyAvroSpecificRecordBase] = new AvroKafkaSinkBolt[Tweet](producerAppFactory)
      bolt.prepare(DummyStormConf, DummyStormContext)

      When("receiving a non-Tweet pojo")
      val tuple: Tuple = mock[Tuple]
      val invalidPojo = "I am not of the expected type!"
      // The `Nil: _*` is required workaround because of a known Scala-Java interop problem related to Scala's treatment
      // of Java's varargs.  See http://stackoverflow.com/a/13361530/1743580.
      mwhen(tuple.getValueByField("pojo")).thenReturn(invalidPojo, Nil: _*)
      val collector: BasicOutputCollector = mock[BasicOutputCollector]
      bolt.execute(tuple, collector)

      Then("it should not send any data to Kafka")
      verifyZeroInteractions(producerApp)
      And("it should not send any data to downstream bolts")
      verifyZeroInteractions(collector)
    }

    it("should not declare any output fields") {
      Given("no bolt")

      When("I create a bolt")
      val producerAppFactory: KafkaProducerAppFactory = mock[KafkaProducerAppFactory]
      val bolt: AvroKafkaSinkBolt[AnyAvroSpecificRecordBase] = new AvroKafkaSinkBolt[AnyAvroSpecificRecordBase](producerAppFactory)

      Then("it should declare zero output fields")
      val declarer: OutputFieldsDeclarer = mock[OutputFieldsDeclarer]
      bolt.declareOutputFields(declarer)
      verifyZeroInteractions(declarer)
    }

  }

  describe("An AvroKafkaSinkBolt companion object") {

    it("should create an AvroKafkaSinkBolt for the correct type") {
      Given("a companion object")

      When("I ask it to create a bolt for type Tweet")
      val producerAppFactory: KafkaProducerAppFactory = mock[KafkaProducerAppFactory]
      val bolt: AvroKafkaSinkBolt[AnyAvroSpecificRecordBase] = AvroKafkaSinkBolt.ofType(classOf[Tweet])(producerAppFactory)

      Then("the bolt should be an AvroKafkaSinkBolt")
      bolt shouldBe an[AvroKafkaSinkBolt[_]]
      And("the bolt should be parameterized with the type Tweet")
      bolt.tpe.shouldEqual(manifest[Tweet])
    }

  }

}