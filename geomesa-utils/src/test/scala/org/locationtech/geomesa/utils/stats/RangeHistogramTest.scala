/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RangeHistogramTest extends Specification with StatTestHelper {
  sequential

  "RangeHistogram stat" should {
    "create RangeHistogram stats for" in {
      "dates" in {
        val stat = Stat(sft, "RangeHistogram(dtg,24,2012-01-01T00:00:00.000Z,2012-01-03T00:00:00.000Z)")
        val rh = stat.asInstanceOf[RangeHistogram[Date]]
        val lowerEndpoint = StatHelpers.dateFormat.parseDateTime("2012-01-01T00:00:00.000Z").toDate
        val midPoint = StatHelpers.dateFormat.parseDateTime("2012-01-02T00:00:00.000Z").toDate

        features.foreach { stat.observe }
        rh.histogram.size mustEqual 24
        rh.histogram(lowerEndpoint) mustEqual 4
        rh.histogram(midPoint) mustEqual 0

        "serialize and deserialize" in {
          val packed   = StatSerialization.pack(rh)
          val unpacked = StatSerialization.unpack(packed).asInstanceOf[RangeHistogram[Date]]

          unpacked mustEqual rh
        }

        "combine two RangeHistograms" in {
          val stat2 = Stat(sft, "RangeHistogram(dtg,24,2012-01-01T00:00:00.000Z,2012-01-03T00:00:00.000Z)")
          val rh2 = stat2.asInstanceOf[RangeHistogram[Date]]

          features2.foreach { stat2.observe }

          rh2.histogram.size mustEqual 24
          rh2.histogram(lowerEndpoint) mustEqual 0
          rh2.histogram(midPoint) mustEqual 5

          stat.add(stat2)

          rh.histogram.size mustEqual 24
          rh.histogram(lowerEndpoint) mustEqual 2
          rh.histogram(midPoint) mustEqual 5
          rh2.histogram.size mustEqual 24
          rh2.histogram(lowerEndpoint) mustEqual 0
          rh2.histogram(midPoint) mustEqual 5
        }
      }

//      "integers" in {
//        val stat = Stat(sft, "RangeHistogram(doubleAttr,10,5,15)")
//        val rh = stat.asInstanceOf[RangeHistogram[java.lang.Integer]]
//
//        features.foreach { stat.observe }
//        rh.histogram.size mustEqual 10
//
//        "serialize and deserialize" in {
//
//        }
//
//        "combine two RangeHistograms" in {
//
//        }
//      }
//
//      "longs" in {
//        val stat = Stat(sft, "RangeHistogram(longAttr,10,5,15)")
//        val rh = stat.asInstanceOf[RangeHistogram[java.lang.Long]]
//
//        features.foreach { stat.observe }
//        rh.histogram.size mustEqual 10
//
//        "serialize and deserialize" in {
//
//        }
//
//        "combine two RangeHistograms" in {
//
//        }
//      }
//
//      "doubles" in {
//        val stat = Stat(sft, "RangeHistogram(doubleAttr,10,5,15)")
//        val rh = stat.asInstanceOf[RangeHistogram[java.lang.Double]]
//
//        features.foreach { stat.observe }
//        rh.histogram.size mustEqual 10
//
//        "serialize and deserialize" in {
//
//        }
//
//        "combine two RangeHistograms" in {
//
//        }
//      }
//
//      "floats" in {
//        val stat = Stat(sft, "RangeHistogram(floatAttr,10,5,15)")
//        val rh = stat.asInstanceOf[RangeHistogram[java.lang.Float]]
//
//        features.foreach { stat.observe }
//        rh.histogram.size mustEqual 10
//
//        "serialize and deserialize" in {
//
//        }
//
//        "combine two RangeHistograms" in {
//
//        }
//      }
    }
  }
}
