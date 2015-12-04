package com.linqia.spark.traffic

import com.linqia.formats.Sanitizer
import org.apache.log4j.Logger

trait Logger extends Serializable {
  @transient lazy val logger = Logger.getLogger(getClass.getName.stripSuffix("$"))
}

object Util extends Serializable {

  class TabSanitizer extends Sanitizer {
    override def sanitize(input: String): String = if (input != null) input.replace('\t', '_') else null
  }

}

