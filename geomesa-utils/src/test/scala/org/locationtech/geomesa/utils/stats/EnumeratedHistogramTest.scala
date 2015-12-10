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
class EnumeratedHistogramTest extends Specification with StatTestHelper {
  sequential

  "EnumeratedHistogram stat" should {
    "should work with" in {
      val stat = Stat(sft, "EnumeratedHistogram(doubleAttr)")
      val eh = stat.asInstanceOf[EnumeratedHistogram[java.lang.Double]]

      features.foreach { stat.observe }

      eh.frequencyMap.size mustEqual 100.0
      eh.frequencyMap(1.0) mustEqual 1.0

      features.foreach { stat.observe }

      eh.frequencyMap.size mustEqual 100.0
      eh.frequencyMap(1.0) mustEqual 2.0
    }
  }
}
