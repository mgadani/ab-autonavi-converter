package com.placeiq

import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}

package object scalding {
  val IncomingDateTimeParser = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC()
  val OutgoingISODateZTimeFormatter = ISODateTimeFormat.dateTimeNoMillis.withOffsetParsed.withZoneUTC
}
