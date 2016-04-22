package com.placeiq.scalding

import com.placeiq.commons.serialization.avro.Observation
import com.twitter.scalding.avro.PackedAvroSource
import com.twitter.scalding.{JobTest, TextLine}
import org.hamcrest.CoreMatchers._
import org.hamcrest.core.Is.is
import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnitSuite

class ObservationConverterJobTest extends JUnitSuite {
  @Test
  @throws[Exception]
  def testFlow: Unit = {
    JobTest(new ObservationConverterJob(_))
      .arg("input", "some-input")
      .arg("output", "some-output")
      .source(TextLine("some-input"),
        List(
          "0" -> "357163051901754,116.48379,39.899045,HSPA,2016-04-18 12:06:53,,," //valid
          ,"1" -> "355395045536617,116.4170098,40.0314657,,2016-04-18 12:10:28,,," //valid
          ,"2" -> "355395045536617,116.4170098,40.0314657,,2016-04-18X12:10:28,,," //invalid datetime, still mapped
          ,"3" -> "355395045536617,116.4170098,40.0314657," // invalid, not enough fields, not mapped
        )
      )
      .sink[Observation](PackedAvroSource[Observation]("some-output")) { observations =>
        assertThat(observations.size, is(equalTo(3)))
      }
      .counters { c =>
        assertThat(c("DISCARDED_INVALID_LINES").toInt, is(equalTo(1)))
        assertThat(c("INVALID_DATETIME").toInt, is(equalTo(1)))
        assertThat(c("OBSERVATION_AVRO_PRODUCED").toInt, is(equalTo(3)))
      }
      .run
      .finish
  }
}