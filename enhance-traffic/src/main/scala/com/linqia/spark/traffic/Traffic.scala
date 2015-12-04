package com.linqia.spark.traffic

import com.linqia.formats._
import org.joda.time.DateTime

import scala.util.Try


object Traffic {
  type JavaTraffic = com.linqia.formats.Traffic

  private lazy val tabSanitizer = new Sanitizer {
    override def sanitize(s: String): String = Option(s).map(t => t.replace('\t', '_')).orNull
  }

  def apply(javaTraffic: JavaTraffic): Traffic = new Traffic(javaTraffic)
}

/**
 * A wrapper for [[com.linqia.formats.Traffic]]
 *
 * The purpose of this class is to simulate the Tuple in Cascading, and the way it is used in the
 * enhance click/badge Hadoop jobs.
 *
 * @param javaTraffic the Java traffic instance to be wrapped
 * @param sanitizer a sanitizer to be used for cleaning up user agent stings
 */
sealed class Traffic(
    private[this] val javaTraffic: Traffic.JavaTraffic,
    private[this] val sanitizer: Sanitizer)
    extends Serializable with Logger {

  private[this] var _id, _time: String = _

  private[this] var _event, _userAgent, _location, _url, _remoteIp, _permCookie, _referer, _floodId,
  _countryCode, _stateCode, _stateName, _cityName, _timeZone, _reverseDNS, _lastClick,
  _lastClickReferer: Option[String] = None

  private[this] var _campaignId, _communityId, _assetId, _viewId, _responseCode, _lastCommunity,
  _secondsSinceLastClick, _viewLength: Option[Int] = None

  private[this] var _dateTime: DateTime = _

  private[this] var _trafficType: TrafficType = _

  private[this] var _isBot, _isCapped, _isLive, _viewStart, _viewEnd,
  _endOfView: Option[Boolean] = None

  private[this] var _deviceType: Option[DeviceType] = None

  def this(javaTraffic: Traffic.JavaTraffic) = {
    this(javaTraffic, Traffic.tabSanitizer)
  }

  def id = _id

  def id_=(_id: String) = {
    this._id = _id
    this
  }

  def time = _time

  def time_=(_time: String) = {
    this._time = _time
    this
  }

  def dateTime = _dateTime

  def dateTime_=(_dateTime: DateTime) = {
    this._dateTime = _dateTime
    this
  }

  def trafficType = _trafficType

  def trafficType_=(_trafficType: TrafficType) = {
    this._trafficType = _trafficType
    this
  }

  def campaignId = _campaignId

  def campaignId_=(_campaignId: Option[Int]) = {
    this._campaignId = _campaignId
    this
  }

  def communityId = _communityId

  def communityId_=(_communityId: Option[Int]) = {
    this._communityId = _communityId
    this
  }

  def assetId = _assetId

  def assetId_=(_assetId: Option[Int]) = {
    this._assetId = _assetId
    this
  }

  def userAgent = _userAgent

  def userAgent_=(_userAgent: Option[String]) = {
    this._userAgent = _userAgent
    this
  }

  def remoteIp = _remoteIp

  def remoteIp_=(_remoteIp: Option[String]) = {
    this._remoteIp = _remoteIp
    this
  }

  def location = _location

  def location_=(_location: Option[String]) = {
    this._location = _location
    this
  }

  def url = _url

  def url_=(_url: Option[String]) = {
    this._url = _url
    this
  }

  def responseCode = _responseCode

  def responseCode_=(_responseCode: Option[Int]) = {
    this._responseCode = _responseCode
    this
  }

  def isCapped = _isCapped

  def isCapped_=(_isCapped: Option[Boolean]) = {
    this._isCapped = _isCapped
    this
  }

  def isBot = _isBot

  def isBot_=(_isBot: Option[Boolean]) = {
    this._isBot = _isBot
    this
  }

  def floodId = _floodId

  def floodId_=(_floodId: Option[String]) = {
    this._floodId = _floodId
    this
  }

  def isLive = _isLive

  def isLive_=(_isLive: Option[Boolean]) = {
    this._isLive = _isLive
    this
  }

  def deviceType = _deviceType

  def deviceType_=(_deviceType: Option[DeviceType]) = {
    this._deviceType = _deviceType
    this
  }

  def countryCode = _countryCode

  def countryCode_=(_countryCode: Option[String]) = {
    this._countryCode = _countryCode
    this
  }

  def stateCode = _stateCode

  def stateCode_=(_stateCode: Option[String]) = {
    this._stateCode = _stateCode
    this
  }

  def stateName = _stateName

  def stateName_=(_stateName: Option[String]) = {
    this._stateName = _stateName
    this
  }

  def cityName = _cityName

  def cityName_=(_cityName: Option[String]) = {
    this._cityName = _cityName
    this
  }

  def timeZone = _timeZone

  def timeZone_=(_timeZone: Option[String]) = {
    this._timeZone = _timeZone
    this
  }

  def reverseDNS = _reverseDNS

  def reverseDNS_=(_reverseDNS: Option[String]) = {
    this._reverseDNS = _reverseDNS
    this
  }

  def permCookie = _permCookie

  def permCookie_=(_permCookie: Option[String]) = {
    this._permCookie = _permCookie
    this
  }

  def endOfView = _endOfView

  def endOfView_=(_endOfView: Option[Boolean]) = {
    this._endOfView = _endOfView
    this
  }

  def lastCommunity = _lastCommunity

  def lastCommunity_=(_lastCommunity: Option[Int]) = {
    this._lastCommunity = _lastCommunity
    this
  }

  def lastClick = _lastClick

  def lastClick_=(_lastClick: Option[String]) = {
    this._lastClick = _lastClick
    this
  }

  def secondsSinceLastClick = _secondsSinceLastClick

  def secondsSinceLastClick_=(_secondsSinceLastClick: Option[Int]) = {
    this._secondsSinceLastClick = _secondsSinceLastClick
    this
  }

  def viewId = _viewId

  def viewId_=(_viewId: Option[Int]) = {
    this._viewId = _viewId
    this
  }

  def viewStart = _viewStart

  def viewStart_=(_viewStart: Option[Boolean]) = {
    this._viewStart = _viewStart
    this
  }

  def viewEnd = _viewEnd

  def viewEnd_=(_viewEnd: Option[Boolean]) = {
    this._viewEnd = _viewEnd
    this
  }

  def viewLength = _viewLength

  def viewLength_=(_viewLength: Option[Int]) = {
    this._viewLength = _viewLength
    this
  }

  def event = _event

  def event_=(_event: Option[String]) = {
    this._event = _event
    this
  }

  def lastClickReferer = _lastClickReferer

  def lastClickReferer_=(_lastClickReferer: Option[String]) = {
    this._lastClickReferer = _lastClickReferer
    this
  }

  def referer = _referer

  def referer_=(_referer: Option[String]) = {
    this._referer = _referer
    this
  }

  /**
   * Returns the json representation of the '''enhanced''' traffic instance being wrapped.
   *
   * @return An Option of the enhanced json string, or None if
   *         [[com.linqia.formats.FormatException]] is raised.
   */
  def toEnhancedJson(): Option[String] = try {
    Option(Parser.instance().write(enhance()))
  } catch {
    case e: FormatException => {
      logger.error(s"Could not serialize enhanced request: ${e.getMessage}")
      None
    }
  }

  private[this] def enhance(): TrafficEnhanced = javaTraffic match {
    case t: TrafficImpression => enhanceAdlink(t.enhance())
    case t: TrafficClick => enhanceClick(t.enhance())
    case t: TrafficConversion => enhanceConversion(t.enhance())
    case t: TrafficConversionJs => enhanceConversionJS(t.enhance())
    case t: TrafficDisclosure => enhanceDisclosure(t.enhance())
    case t: TrafficJavascript => enhanceCommunity(t.enhance())
    case t: TrafficBadge => enhanceCommunity(t.enhance())
    case t: TrafficUpdate => enhanceCommunity(t.enhance())
    case _ => throw new IllegalArgumentException(s"Unsupported type: ${_trafficType}.")
  }

  private[this] def enhanceTraffic(traffic: TrafficEnhanced): TrafficEnhanced = {
    _deviceType.map(dt => Try(traffic.setDeviceType(dt)))
    traffic.setBot(_isBot.getOrElse(false))
    traffic.setCountryCode(_countryCode.orNull)
    traffic.setStateCode(_stateCode.orNull)
    traffic.setStateName(_stateName.orNull)
    traffic.setCityName(_cityName.orNull)
    traffic.setTimeZone(_timeZone.orNull)
    traffic.setReverseDns(_reverseDNS.orNull)

    traffic
  }

  private[this] def enhanceAdlink(traffic: TrafficCampaignCommunityEnhanced): TrafficEnhanced = {
    enhanceTraffic(traffic)
    enhanceCampaign(traffic)
    traffic.setLocation(_location.orNull)
    traffic.setFloodId(_floodId.orNull)

    traffic
  }

  private[this] def enhanceClick(traffic: TrafficClickEnhanced): TrafficEnhanced = {
    enhanceAdlink(traffic)
    traffic.setCapped(_isCapped.getOrElse(false))

    traffic
  }

  private[this] def enhanceCommunity(traffic: TrafficCommunityEnhanced): TrafficEnhanced = {
    enhanceTraffic(traffic)
    traffic.setCommunityId(_communityId.getOrElse(-1))

    traffic
  }

  private[this] def enhanceDisclosure(traffic: TrafficDisclosureEnhanced): TrafficEnhanced = {
    enhanceTraffic(traffic)
    enhanceCampaign(traffic)
    enhanceCommunity(traffic)
    traffic.setFloodId(_floodId.orNull)

    traffic
  }

  private[this] def enhanceCampaign(traffic: TrafficCampaignEnhanced): TrafficEnhanced = {
    enhanceTraffic(traffic)
    traffic.setCampaignId(_campaignId.getOrElse(-1))
    traffic.setLive(_isLive.getOrElse(false))

    traffic
  }

  private[this] def enhanceConversion(traffic: TrafficConversionEnhanced): TrafficEnhanced = {
    enhanceCampaign(traffic)
    traffic.setLastClickId(_lastClick.orNull)
    traffic.setLastCommunityId(_lastCommunity.getOrElse(-1))
    traffic.setSecondsSinceLastClick(_secondsSinceLastClick.getOrElse(-1))
    traffic.setViewStart(_viewStart.getOrElse(false))
    traffic.setViewEnd(_viewEnd.getOrElse(false))
    traffic.setViewSeconds(_viewLength.getOrElse(-1))
    traffic.setLastClickReferer(_lastClickReferer.orNull)

    traffic
  }

  private[this] def enhanceConversionJS(traffic: TrafficConversionJsEnhanced): TrafficEnhanced = {
    enhanceCampaign(traffic)
    traffic.setLastClickId(_lastClick.orNull)
    traffic.setLastCommunityId(_lastCommunity.getOrElse(-1))
    traffic.setSecondsSinceLastClick(_secondsSinceLastClick.getOrElse(-1))
    traffic.setLastClickReferer(_lastClickReferer.orNull)

    traffic
  }

  override def toString: String = s"Traffic(id: ${_id}, " +
      s"time: ${_time}, " +
      s"dateTime: ${_dateTime}, " +
      s"trafficType: ${_trafficType}, " +
      s"campaignId: ${_campaignId.getOrElse("NULL")}, " +
      s"communityId: ${_communityId.getOrElse("NULL")}, " +
      s"assetId: ${_assetId.getOrElse("NULL")}, " +
      s"userAgent: ${_userAgent.getOrElse("NULL")}, " +
      s"remoteIp: ${_remoteIp.getOrElse("NULL")}, " +
      s"location: ${_location.getOrElse("NULL")}, " +
      s"url: ${_url.getOrElse("NULL")}, " +
      s"responseCode: ${_responseCode.getOrElse("NULL")}, " +
      s"isCapped: ${_isCapped.getOrElse("NULL")}, " +
      s"isBot: ${_isBot.getOrElse("NULL")}, " +
      s"floodId: ${_floodId.getOrElse("NULL")}, " +
      s"isLive: ${_isLive.getOrElse("NULL")}, " +
      s"deviceType: ${_deviceType.getOrElse("NULL")}, " +
      s"countryCode: ${_countryCode.getOrElse("NULL")}, " +
      s"stateCode: ${_stateCode.getOrElse("NULL")}, " +
      s"stateName: ${_stateName.getOrElse("NULL")}, " +
      s"cityName: ${_cityName.getOrElse("NULL")}, " +
      s"timeZone: ${_timeZone.getOrElse("NULL")}, " +
      s"reverseDNS: ${_reverseDNS.getOrElse("NULL")}, " +
      s"permCookie: ${_permCookie.getOrElse("NULL")}, " +
      s"endOfView: ${_endOfView.getOrElse("NULL")}, " +
      s"lastCommunity: ${_lastCommunity.getOrElse("NULL")}, " +
      s"lastClick: ${_lastClick.getOrElse("NULL")}, " +
      s"secondsSinceLastClick: ${_secondsSinceLastClick.getOrElse("NULL")}, " +
      s"viewId: ${_viewId.getOrElse("NULL")}, " +
      s"viewStart: ${_viewStart.getOrElse("NULL")}, " +
      s"viewEnd: ${_viewEnd.getOrElse("NULL")}, " +
      s"viewLength: ${_viewLength.getOrElse("NULL")}, " +
      s"event: ${_event.getOrElse("NULL")}, " +
      s"lastClickReferer: ${_lastClickReferer.getOrElse("NULL")}, " +
      s"referer: ${_referer.getOrElse("NULL")})"

  javaTraffic match {
    case t: TrafficCampaignCommunity => {
      _assetId = Option(t.getAssetId())
      _responseCode = Option(t.getResponseCode())
    }
    case t: TrafficCampaign => _campaignId = Option(t.getCampaignId())
    case t: TrafficCommunity => _communityId = Option(t.getCommunityId())
    case t: TrafficConversion => {
      _viewId = Option(t.getViewId())
      _event = Option(t.getEvent())
    }
    case _ =>
  }

  _id = javaTraffic.getId()
  _trafficType = javaTraffic.getType()
  _dateTime = javaTraffic.getDateTime()
  _userAgent = Option(javaTraffic.getUserAgent(sanitizer))
  _location = Option(javaTraffic.getLocation())
  _url = Option(javaTraffic.getUrl())
  _remoteIp = Option(javaTraffic.getRemoteIp())
  _permCookie = Option(javaTraffic.getPermanentCookie())
  _referer = Option(javaTraffic.getReferer())
}

