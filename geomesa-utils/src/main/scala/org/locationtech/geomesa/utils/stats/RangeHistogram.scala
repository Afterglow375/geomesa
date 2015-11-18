/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

class RangeHistogram[T](attribute: String, buckets: Int, range: com.google.common.collect.Range[T]) extends Stat {

  override def observe(sf: SimpleFeature): Unit = {
    val sfval = sf.getAttribute(attribute)

    if (sfval != null) {
      sfval match {
        case tval: T =>

      }
    }
  }

  override def toJson(): String = ???

  override def add(other: Stat): Stat = ???
}

