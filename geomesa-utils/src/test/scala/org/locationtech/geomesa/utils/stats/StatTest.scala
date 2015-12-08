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
  val doubleIndex = sft.indexOf("doubleAttr")
  val floatIndex = sft.indexOf("floatAttr")
  val dateIndex = sft.indexOf("dtg")

  val features = (1 to 100).toArray.map {
    i => SimpleFeatureBuilder.build(sft,
      Array("abc", i, i, i, i, "POINT(-77 38)",
        new DateTime(s"2012-01-01T${i%24}:00:00", DateTimeZone.UTC).toDate).asInstanceOf[Array[AnyRef]], i.toString)
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

    "create MinMax stats for" in {
      "dates" in {
        val stat = Stat(sft, "MinMax(dtg)")
        val minMax = stat.asInstanceOf[MinMax[Date]]

        minMax.attrIndex mustEqual dateIndex
        minMax.attrType mustEqual "java.util.Date"
        minMax.min mustEqual new Date(java.lang.Long.MAX_VALUE)
        minMax.max mustEqual new Date(java.lang.Long.MIN_VALUE)

        features.foreach { stat.observe }

        minMax.min mustEqual StatHelpers.dateFormat.parseDateTime("2012-01-01T00:00:00.000Z").toDate
        minMax.max mustEqual StatHelpers.dateFormat.parseDateTime("2012-01-01T23:00:00.000Z").toDate
      }

      "integers" in {
        val stat = Stat(sft, "MinMax(intAttr)")
        val minMax = stat.asInstanceOf[MinMax[java.lang.Integer]]

        minMax.attrIndex mustEqual intIndex
        minMax.attrType mustEqual "java.lang.Integer"
        minMax.min mustEqual java.lang.Integer.MAX_VALUE
        minMax.max mustEqual java.lang.Integer.MIN_VALUE

        features.foreach { stat.observe }

        minMax.min mustEqual 1
        minMax.max mustEqual 100
      }

      "longs" in {
        val stat = Stat(sft, "MinMax(longAttr)")
        val minMax = stat.asInstanceOf[MinMax[java.lang.Long]]

        minMax.attrIndex mustEqual longIndex
        minMax.attrType mustEqual "java.lang.Long"
        minMax.min mustEqual java.lang.Long.MAX_VALUE
        minMax.max mustEqual java.lang.Long.MIN_VALUE

        features.foreach { stat.observe }

        minMax.min mustEqual 1L
        minMax.max mustEqual 100L
      }

      "doubles" in {
        val stat = Stat(sft, "MinMax(doubleAttr)")
        val minMax = stat.asInstanceOf[MinMax[java.lang.Double]]

        minMax.attrIndex mustEqual doubleIndex
        minMax.attrType mustEqual "java.lang.Double"
        minMax.min mustEqual java.lang.Double.MAX_VALUE
        minMax.max mustEqual java.lang.Double.MIN_VALUE

        features.foreach { stat.observe }

        minMax.min mustEqual 1
        minMax.max mustEqual 100
      }

      "floats" in {
        val stat = Stat(sft, "MinMax(floatAttr)")
        val minMax = stat.asInstanceOf[MinMax[java.lang.Float]]

        minMax.attrIndex mustEqual floatIndex
        minMax.attrType mustEqual "java.lang.Float"
        minMax.min mustEqual java.lang.Float.MAX_VALUE
        minMax.max mustEqual java.lang.Float.MIN_VALUE

        features.foreach { stat.observe }

        minMax.min mustEqual 1
        minMax.max mustEqual 100
      }
    }

    "create IteratoreStackCounter stat" in {
      val stat = Stat(sft, "IteratorStackCounter")
      val isc = stat.asInstanceOf[IteratorStackCounter]

      isc.count mustEqual 1L
    }

    "create EnumerationHistogram stat" in {
      val stat = Stat(sft, "EnumeratedHistogram(doubleAttr)")
      val eh = stat.asInstanceOf[EnumeratedHistogram[java.lang.Double]]

      features.foreach { stat.observe }

      eh.frequencyMap.size mustEqual 100.0
      eh.frequencyMap(1.0) mustEqual 1.0

      features.foreach { stat.observe }

      eh.frequencyMap.size mustEqual 100.0
      eh.frequencyMap(1.0) mustEqual 2.0
    }

    "create RangeHistogram stats for" in {
      "dates" in {
        val stat = Stat(sft, "RangeHistogram(dtg,12,2012-01-01T00:00:00.000Z,2012-01-01T23:00:00.000Z)")
        val rh = stat.asInstanceOf[RangeHistogram[Date]]

        features.foreach { stat.observe }
        rh.histogram.size mustEqual 12
      }

      "integers" in {
        val stat = Stat(sft, "RangeHistogram(doubleAttr,10,5,15)")
        val rh = stat.asInstanceOf[RangeHistogram[java.lang.Double]]

        features.foreach { stat.observe }
        rh.histogram.size mustEqual 10
      }

      "longs" in {
        val stat = Stat(sft, "RangeHistogram(longAttr,10,5,15)")
        val rh = stat.asInstanceOf[RangeHistogram[Long]]

        features.foreach { stat.observe }
        rh.histogram.size mustEqual 10
      }

      "doubles" in {
        val stat = Stat(sft, "RangeHistogram(doubleAttr,10,5,15)")
        val rh = stat.asInstanceOf[RangeHistogram[Double]]

        features.foreach { stat.observe }
        rh.histogram.size mustEqual 10
      }

      "floats" in {
        val stat = Stat(sft, "RangeHistogram(floatAttr,10,5,15)")
        val rh = stat.asInstanceOf[RangeHistogram[Float]]

        features.foreach { stat.observe }
        rh.histogram.size mustEqual 10
      }
    }

    "create a sequence of stats" in {
      val stat = Stat(sft, "MinMax(intAttr);MinMax(longAttr);IteratorStackCounter")
      val stats = stat.asInstanceOf[SeqStat].stats

      stats.size mustEqual 3

      val minMax1 = stats(0).asInstanceOf[MinMax[java.lang.Integer]]
      val minMax2 = stats(1).asInstanceOf[MinMax[java.lang.Long]]
      val isc = stats(2) must beAnInstanceOf[IteratorStackCounter]

      minMax1.attrIndex mustEqual intIndex
      minMax1.attrType mustEqual "java.lang.Integer"
      minMax1.min mustEqual java.lang.Integer.MAX_VALUE
      minMax1.max mustEqual java.lang.Integer.MIN_VALUE

      minMax2.attrIndex mustEqual longIndex
      minMax2.attrType mustEqual "java.lang.Long"
      minMax2.min mustEqual java.lang.Long.MAX_VALUE
      minMax2.max mustEqual java.lang.Long.MIN_VALUE

      features.foreach { stat.observe }

      minMax1.min mustEqual 1
      minMax1.max mustEqual 100

      minMax2.min mustEqual 1
      minMax2.max mustEqual 100

//      isc.count mustEqual 1
    }
  }
}
