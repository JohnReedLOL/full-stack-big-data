package com.miguno.kafkastorm.storm

import backtype.storm.tuple.Fields
import org.mockito.ArgumentMatcher

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * [[org.mockito.ArgumentMatcher]] for Storm's [[backtype.storm.tuple.Fields]].
 *
 * ==Why this approach is required==
 * We must use an ArgumentMatcher as a workaround because Storm's Field class does not implement a proper `equals()`
 * method, and Mockito relies on `equals()` for verification.  Because of that the following intuitive approach for
 * Mockito does not work: `verify(declarer, times(1)).declare(new Fields("bytes"))`.
 *
 * @example {{{
 * // Verify that a single field named "pojo" is declared.
 * verify(declarer).declare(argThat(FieldsEqualTo(new Fields("pojo"))))
 * }}}
 *
 * @param expectedFields
 */
class FieldsEqualTo(val expectedFields: Fields) extends ArgumentMatcher[Fields] {
  override def matches(o: scala.Any): Boolean = {
    val fields: mutable.Buffer[String] = o.asInstanceOf[Fields].toList.asScala
    fields == expectedFields.toList.asScala
  }
}

object FieldsEqualTo {
  def apply(expFields: Fields): FieldsEqualTo = new FieldsEqualTo(expFields)
}