package scalaMapReduce;

import akka.actor._
import com.typesafe.config._

object Config {
  private val root = ConfigFactory.load()
  val JobTrackerConfig = root.getConfig("JobTrackerSystem")
  val JobSubmitterConfig = root.getConfig("JobSubmitterSystem")
}