package com.linqia.spark.enhance

import com.linqia.formats.TrafficType
import com.linqia.spark.traffic.Traffic
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

abstract class Enhancer extends Serializable {
  this: Operations =>

  def sparkContext: SparkContext

  def run(): Unit

  def readRaw(input: String): RDD[Traffic] = {
    sparkContext.textFile(input).flatMap(toTraffic)
  }
}

abstract class BadgeEnhancer(input: String, output: String) extends Enhancer {
  this: Operations =>

  private[this] def selector(traffic: Traffic): TrafficType = traffic.trafficType

  // Some trade offs:
  // Place all the mapper operations here to be executed by `collect` might have better
  // performance (?), but will likely increase the failure recovery overhead since doing so will
  // end up with a coarse grained RDD, and any failure or missing part in the RDD will trigger a
  // regeneration of the whole RDD.
  //
  // Another implementation approach is to chain the operations together, this will generate many
  // small RDDs, which will likely reduce the re-computation during fault recovery, but might
  // have some impact on performance.
  private[this] def action(traffic: Traffic): Traffic = {
    parseDeviceType(traffic)
    checkBot(traffic)
    resolveCampaignCommunityId(traffic)
  }

  override def run(): Unit = {

    val traffic = readRaw(input)

    val typesToPickup = Set(
      TrafficType.JAVASCRIPT,
      TrafficType.BADGE,
      TrafficType.JAVASCRIPT,
      TrafficType.CONVERSIONJS,
      TrafficType.DISCLOSURE,
      TrafficType.CONVERSION,
      TrafficType.CLICK
    )

    val badgeTraffic = traffic.collect(pickup(typesToPickup, selector, action))

    badgeTraffic.collect().foreach { traffic =>
      traffic.toEnhancedJson() match {
        case Some(json) => println(json)
        case _ =>
      }
    }
  }
}

abstract class CampaignEnhancer(
    rawInput: String,
    enhancedOutput: String,
    summaryInput: String,
    summaryOutput: String) extends Enhancer {

  this: Operations =>

}
