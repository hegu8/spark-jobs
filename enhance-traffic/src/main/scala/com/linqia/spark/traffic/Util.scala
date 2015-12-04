package com.linqia.spark.traffic

import org.apache.log4j.Logger

trait Logger extends Serializable {
  @transient lazy val logger = Logger.getLogger(getClass.getName.stripSuffix("$"))
}

object Util extends Serializable {

}

