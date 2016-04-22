package com.placeiq.scalding

import java.util

import com.placeiq.commons.serialization.avro.Observation
import com.twitter.scalding._
import com.twitter.scalding.avro.PackedAvroSource
import org.joda.time.format.ISODateTimeFormat

import scala.util.Try

/**
  * AutoNavi to Observation converter
  * Input CSV has variable number of fields
  */
class ObservationConverterJob(args: Args) extends Job(args) {
  val discardedCount = Stat("DISCARDED_LINES_NOT_ENOUGH_FIELDS")
  val unparseableDateTimeCount = Stat("UNPARSEABLE_DATETIME_BUT_STILL_CONVERTED")
  val observationsAvroCount = Stat("OBSERVATION_AVROS_PRODUCED")

  TypedPipe.from(TextLine(args("input")))
    .flatMap { line =>
      val fields = line.split(',')
      if(fields.length < 5) {
        discardedCount.inc
        Iterator()
      } else Iterator((fields(0), fields(1), fields(2), fields(3), fields(4)))
    }
    .map(toObservation)
    .write(PackedAvroSource[Observation](args("output")))

  def toObservation(record: (String, String, String, String, String)): Observation = record match {
    case (id, long, lat, network, datetime) =>
      val datetimeToISOTry: Try[String] = Try(OutgoingISODateZTimeFormatter.print(IncomingDateTimeParser.parseDateTime(datetime)))
      if(datetimeToISOTry.isFailure) unparseableDateTimeCount.inc
      observationsAvroCount.inc
      Observation.newBuilder()
        .setDeviceId(id)
        .setSource(network)
        .setLocationGeometry(s"POINT ($long $lat)")
        .setDateTime(datetimeToISOTry.getOrElse(datetime))
        .setDuration(1l)
        .setMetadata(new util.HashMap())
        .build()
  }
}

