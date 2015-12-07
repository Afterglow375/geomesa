/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import org.geotools.feature.simple.SimpleFeatureBuilder
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatTest extends Specification {

  sequential

  val sftSpec = "strAttr:String,intAttr:Integer,longAttr:Long,doubleAttr:Double,floatAttr:Float,geom:Geometry:srid=4326,dtg:Date"
  val sft = SimpleFeatureTypes.createType("test", sftSpec)
  val intIndex = sft.indexOf("intAttr")
  val longIndex = sft.indexOf("longAttr")
  val features = (1 to 100).toArray.map {
    i => SimpleFeatureBuilder.build(sft,
      Array("abc", i, i, i, i, "POINT(-77 38)", new DateTime(s"2012-01-01T${i%24}:00:00", DateTimeZone.UTC).toDate).asInstanceOf[Array[AnyRef]],
      i.toString)
  }

  "stats" should {
    "fail for malformed strings" in {
      Stat(sft, "") must throwAn[Exception]
      Stat(sft, "abcd") must throwAn[Exception]
      Stat(sft, "RangeHistogram()") must throwAn[Exception]
      Stat(sft, "RangeHistogram(foo,10,2012-01-01T00:00:00.000Z,2012-02-01T00:00:00.000Z)") must throwAn[Exception]
      Stat(sft, "MinMax()") must throwAn[Exception]
      Stat(sft, "MinMax(abcd)") must throwAn[Exception]
      Stat(sft, "MinMax(geom)") must throwAn[Exception]
    }

    "create MinMax stat" in {
      val stat = Stat(sft, "MinMax(intAttr)")
      val mm = stat.asInstanceOf[MinMax[java.lang.Long]]
      mm.attributeIndex mustEqual intIndex
    }

    "create RangeHistogram stat for" in {
      "dates" in {
        val stat = Stat(sft, "RangeHistogram(dtg,12,2012-01-01T00:00:00.000Z,2012-01-01T23:00:00.000Z)")
        features.foreach { stat.observe }
        val rh = stat.asInstanceOf[RangeHistogram[Date]]
        (rh.histogram must not).beNull
        rh.histogram.size mustEqual 12
      }

      "integers" in {
        val stat = Stat(sft, "RangeHistogram(doubleAttr,10,5,15)")
        features.foreach { stat.observe }
        val rh = stat.asInstanceOf[RangeHistogram[java.lang.Double]]
        (rh.histogram must not).beNull
        rh.histogram.size mustEqual 10
      }

      "longs" in {
        val stat = Stat(sft, "RangeHistogram(longAttr,10,5,15)")
        features.foreach { stat.observe }
        val rh = stat.asInstanceOf[RangeHistogram[Long]]
        (rh.histogram must not).beNull
        rh.histogram.size mustEqual 10
      }

      "doubles" in {
        val stat = Stat(sft, "RangeHistogram(doubleAttr,10,5,15)")
        features.foreach { stat.observe }
        val rh = stat.asInstanceOf[RangeHistogram[Double]]
        (rh.histogram must not).beNull
        rh.histogram.size mustEqual 10
      }

      "floats" in {
        val stat = Stat(sft, "RangeHistogram(floatAttr,10,5,15)")
        features.foreach { stat.observe }
        val rh = stat.asInstanceOf[RangeHistogram[Float]]
        (rh.histogram must not).beNull
        rh.histogram.size mustEqual 10
      }
    }

    "create a sequence of stats" in {
      val stat = Stat(sft, "MinMax(intAttr);MinMax(longAttr);IteratorCount")
      val stats = stat.asInstanceOf[SeqStat].stats

      stats.size mustEqual 3

      stats(0).asInstanceOf[MinMax[java.lang.Integer]].attributeIndex mustEqual intIndex
      stats(1).asInstanceOf[MinMax[java.lang.Long]].attributeIndex mustEqual longIndex
      stats(2) must beAnInstanceOf[IteratorStackCounter]
    }
  }
}
