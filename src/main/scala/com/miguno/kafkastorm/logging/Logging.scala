package com.miguno.kafkastorm.logging

import org.slf4j.{Logger, LoggerFactory}
import scala.trace.{Pos, implicitlyFormatable}

trait LazyLogging {

  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

}

trait StrictLogging {

  protected val logger: Logger = LoggerFactory.getLogger(getClass.getName)

}