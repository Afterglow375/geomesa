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

  val sftSpec = "str:String,int:Integer,long:Long,double:Double,float:Float,geom:Geometry:srid=4326,dtg:Date"
  val sft = SimpleFeatureTypes.createType("test", sftSpec)
  val features = (1 to 100).toArray.map {
    i => SimpleFeatureBuilder.build(sft,
      Array("abc", i, i, i, i, "POINT(-77 38)", new DateTime(s"2012-01-01T${i%24}:00:00", DateTimeZone.UTC).toDate).asInstanceOf[Array[AnyRef]],
      i.toString)
  }

  "stats" should {
    "fail for malformed strings" in {
      Stat("") must throwAn[Exception]
      Stat("abcd") must throwAn[Exception]
      Stat("RangeHistogram()") must throwAn[Exception]
      Stat("RangeHistogram(foo,Date2,10,2012-01-01T00:00:00.000Z,2012-02-01T00:00:00.000Z)") must throwAn[Exception]
      Stat("RangeHistogram(foo,DateTime,10,2012-01-01T00:00:00.000Z,2012-02-01T00:00:00.000Z)") must throwAn[Exception]
      Stat("MinMax()") must throwAn[Exception]
      Stat("MinMax(ab-cd)") must throwAn[Exception]
    }

    "create MinMax stat" in {
      val stats = Stat("MinMax(foo)")
      val stat  = stats.asInstanceOf[SeqStat].stats.head

      val mm = stat.asInstanceOf[MinMax[java.lang.Long]]
      mm.attribute mustEqual "foo"
    }

    "create RangeHistogram stat for" in {
      "dates" in {
        val stats = Stat("RangeHistogram(dtg,Date,12,2012-01-01T00:00:00.000Z,2012-01-01T23:00:00.000Z)")
        val stat  = stats.asInstanceOf[SeqStat].stats.head

        features.foreach { stat.observe }

        val rh = stat.asInstanceOf[RangeHistogram[Date]]
        rh.println(rh.toJson)
        rh.frequency must not beNull
      }

      "integers" in {
        val stats = Stat("RangeHistogram(int,Integer,10,5,15)")
        val stat  = stats.asInstanceOf[SeqStat].stats.head

        features.foreach { stat.observe }

        val rh = stat.asInstanceOf[RangeHistogram[Integer]]
        rh.println(rh.toJson)
        rh.frequency must not beNull
      }

      "longs" in {
        val stats = Stat("RangeHistogram(foo,Long,10,5,15)")
        val stat  = stats.asInstanceOf[SeqStat].stats.head

        val rh = stat.asInstanceOf[RangeHistogram[Long]]
        rh.frequency must not beNull
      }

      "doubles" in {
        val stats = Stat("RangeHistogram(foo,Double,10,5,15)")
        val stat  = stats.asInstanceOf[SeqStat].stats.head

        val rh = stat.asInstanceOf[RangeHistogram[Double]]
        rh.frequency must not beNull
      }

      "floats" in {
        val stats = Stat("RangeHistogram(foo,Float,10,5,15)")
        val stat  = stats.asInstanceOf[SeqStat].stats.head

        val rh = stat.asInstanceOf[RangeHistogram[Float]]
        rh.frequency must not beNull
      }
    }

    "create a sequence of stats" in {
      val stat = Stat("MinMax(foo);MinMax(bar);IteratorCount")
      val stats = stat.asInstanceOf[SeqStat].stats

      stats.size mustEqual 3

      stats(0).asInstanceOf[MinMax[java.lang.Long]].attribute mustEqual "foo"
      stats(1).asInstanceOf[MinMax[java.lang.Long]].attribute mustEqual "bar"
      stats(2) must beAnInstanceOf[IteratorStackCounter]
    }
  }
}
