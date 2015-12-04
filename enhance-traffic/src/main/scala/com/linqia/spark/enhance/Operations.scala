package com.linqia.spark.enhance

import com.linqia.formats.FormatException.IgnorableFormatException
import com.linqia.formats.{TrafficType, Parser}
import com.linqia.rules.enhancers.{HashResolver, BotChecker, UserAgentParser}
import com.linqia.spark.traffic.{Logger, Traffic}

import scala.reflect.ClassTag
import scala.util.control.NonFatal

trait Operations extends Serializable with Logger {
  protected def botChecker: BotChecker

  protected def userAgentParser: UserAgentParser

  protected def hashResolver: HashResolver

  protected def checkBot(traffic: Traffic): Traffic = {
    traffic.isBot = Option(botChecker.isBot(traffic.dateTime, traffic.userAgent.getOrElse("")))
  }

  protected def parseDeviceType(traffic: Traffic): Traffic = {
    traffic.deviceType = Option(userAgentParser.parse(traffic.userAgent.orNull).deviceType)
  }

  protected def resolveCampaignCommunityId(traffic: Traffic): Traffic = {
    val trafficType = traffic.trafficType
    val url = traffic.url.getOrElse("")

    traffic.trafficType match {
      // do nothing, since it is already filled in
      case TrafficType.CLICK | TrafficType.IMPRESSION => traffic
      case TrafficType.CONVERSION | TrafficType.CONVERSIONJS => {
        traffic.campaignId = Option(hashResolver.getCampaignIdForUrl(trafficType, url))
      }
      case TrafficType.BADGE | TrafficType.JAVASCRIPT => {
        traffic.communityId = Option(hashResolver.getCommunityIdForUrl(trafficType, url))
      }
      case TrafficType.DISCLOSURE => {
        traffic.campaignId = Option(hashResolver.getCampaignIdForUrl(trafficType, url))
        traffic.communityId = Option(hashResolver.getCommunityIdForUrl(trafficType, url))
      }
      case TrafficType.UPDATE => {
        if (url == "/update") {
          traffic
        } else {
          throw new IllegalArgumentException(s"URL $url does not match traffic type $trafficType.")
        }
      }
      case _ => throw new IllegalArgumentException(s"HashLookup can't handle $trafficType.")
    }
  }

  /**
   * Translate a json string to an [[Traffic]] instance.
   *
   * Will throw exception if the json string is invalid.
   *
   * @param jsonStr a json string
   * @return an [[Traffic]] instance wrapped in [[scala.Some]], or None if
   *         [[IgnorableFormatException]] is raised
   */
  protected def toTraffic(jsonStr: String): Option[Traffic] = try {
    Some(Traffic(Parser.instance().read(jsonStr)))
  } catch {
    case e: IgnorableFormatException => None
    case NonFatal(e) => {
      logger.error(s"Found unparsable request: $jsonStr")
      throw e
    }
  }

  /**
   * Generate a partial function that returns the input element only if the `selector` result is in
   * the supplied `set`.
   *
   * This method generates a partial function that:
   * 1, take an element
   * 2, run `selector` on it, and check if the result is in `set`
   * 3, if ''2'' is true, run `action` on the element and return the result
   * 4, else ignore this element
   *
   * @param set the set to check against
   * @param selector a function to be applied on the input element of the partial function, the
   *                 application result will be used to check against `set`
   * @param action a function to be applied on the input element of the partial function if the
   *               check passes, the application result will be the return value of the generated
   *               partial function
   * @return a partial function described above
   */
  protected def pickup[A: ClassTag, B: ClassTag, S: ClassTag](
      set: Set[S],
      selector: A => S,
      action: A => B): PartialFunction[A, B] = {

    case elem if set.contains(selector(elem)) => action(elem)
  }
}
