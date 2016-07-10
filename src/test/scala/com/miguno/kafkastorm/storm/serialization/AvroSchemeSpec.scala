package com.miguno.kafkastorm.storm.serialization

import backtype.storm.tuple.Fields
import com.miguno.avro.Tweet
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import org.scalatest.{FunSpec, GivenWhenThen, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.reflectiveCalls

class AvroSchemeSpec extends FunSpec with Matchers with GivenWhenThen {

  implicit val specificAvroBinaryInjectionForTweet: Injection[Tweet, Array[Byte]] = SpecificAvroCodecs.toBinary[Tweet]

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

  describe("An AvroScheme") {

    it("should have a single output field named 'pojo'") {
      Given("a scheme")
      val scheme: AvroScheme[Nothing] = new AvroScheme

      When("I get its output fields")
      val outputFields: Fields = scheme.getOutputFields()

      Then("there should only be a single field")
      outputFields.size() should be(1)
      And("this field should be named 'pojo'")
      outputFields.contains("pojo") should be(true)
    }


    it("should deserialize binary records of the configured type into pojos") {
      Given("a scheme for type Tweet ")
      val scheme: AvroScheme[Tweet] = new AvroScheme[Tweet]
      And("some binary-encoded Tweet records")
      val tweets: Seq[Tweet] = fixture.messages
      val encodedTweets: Seq[Array[Byte]] = tweets.map(Injection(_))

      When("I deserialize the binary records into pojos")
      val actualTweets: Seq[AnyRef] = for {
        l <- encodedTweets.map(scheme.deserialize)
        tweet <- l.asScala
      } yield tweet

      Then("the pojos should be equal to the original pojos")
      actualTweets should be(tweets)
    }

    it("should throw a runtime exception when serialization fails") {
      Given("a scheme for type Tweet ")
      val scheme: AvroScheme[Tweet] = new AvroScheme[Tweet]
      And("an invalid binary record")
      val invalidBytes: Array[Byte] = Array[Byte](1, 2, 3, 4)

      When("I deserialize the record into a pojo")

      Then("the scheme should throw a runtime exception")
      val exception: RuntimeException = intercept[RuntimeException] {
        scheme.deserialize(invalidBytes)
      }
      And("the exception should provide a meaningful explanation")
      exception.getMessage should be("Could not decode input bytes")
    }

  }

  describe("An AvroScheme companion object") {

    it("should create an AvroScheme for the correct type") {
      Given("a companion object")

      When("I ask it to create a scheme for type Tweet")
      val scheme: AvroScheme[Tweet] = AvroScheme.ofType(classOf[Tweet])

      Then("the scheme should be an AvroScheme")
      scheme shouldBe an[AvroScheme[_]]
      And("the scheme should be parameterized with the type Tweet")
      scheme.tpe.shouldEqual(manifest[Tweet])
    }

  }

}