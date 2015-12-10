/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatTest extends Specification with StatTestHelper {
  sequential

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

    "create a sequence of stats" in {
      val stat11 = Stat(sft, "MinMax(geom)")
      val stat = Stat(sft, "MinMax(intAttr);IteratorStackCounter;EnumeratedHistogram(longAttr);RangeHistogram(doubleAttr,5,1,25)")
      val stats = stat.asInstanceOf[SeqStat].stats

      stats.size mustEqual 4

      val minMax = stats(0).asInstanceOf[MinMax[java.lang.Integer]]
      val isc = stats(1).asInstanceOf[IteratorStackCounter]
      val eh = stats(2).asInstanceOf[EnumeratedHistogram[java.lang.Long]]
      val rh = stats(3).asInstanceOf[RangeHistogram[java.lang.Double]]

      minMax.attrIndex mustEqual intIndex
      minMax.attrType mustEqual "java.lang.Integer"
      minMax.min mustEqual java.lang.Integer.MAX_VALUE
      minMax.max mustEqual java.lang.Integer.MIN_VALUE

      isc.count mustEqual 1

      eh.attrIndex mustEqual longIndex
      eh.attrType mustEqual "java.lang.Long"
      eh.frequencyMap.size mustEqual 0

      rh.attrIndex mustEqual doubleIndex
      rh.attrType mustEqual "java.lang.Double"
      rh.histogram.size mustEqual 0

      features.foreach { stat.observe }

      minMax.min mustEqual 1
      minMax.max mustEqual 100

      isc.count mustEqual 1

      eh.frequencyMap.size mustEqual 100

      rh.histogram.size mustEqual 5

//      "combine two sequences of stats" in {
//        success //TODO
//      }
    }
  }
}
