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
import org.locationtech.geomesa.utils.stats.Stat
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatTest extends Specification  {

  // Stat's apply method should take a SFT and do light validation.
  //val sft = SimpleFeatureTypes.createType()

  "stats DSL" should {
    "fail for malformed strings" in {
      Stat("") must throwAn[Exception]
      Stat("abcd") must throwAn[Exception]
      Stat("RangeHistogram()") must throwAn[Exception]
      Stat("MinMax()") must throwAn[Exception]
      Stat("MinMax(ab-cd)") must throwAn[Exception]
    }

    "create MinMax stat" in {
      val stats = Stat("MinMax(foo)")
      val stat  = stats.asInstanceOf[SeqStat].stats.head

      val mm = stat.asInstanceOf[MinMax[java.lang.Long]]
      mm.attribute mustEqual "foo"
    }

    "create RangeHistogram stat" in {
      val stats = Stat("RangeHistogram(foo,Date,10,2012-01-01T00:00:00.000Z,2012-02-01T00:00:00.000Z)")
      val stat  = stats.asInstanceOf[SeqStat].stats.head

      val rh = stat.asInstanceOf[RangeHistogram[Date]]
      rh.frequency must not beNull
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
