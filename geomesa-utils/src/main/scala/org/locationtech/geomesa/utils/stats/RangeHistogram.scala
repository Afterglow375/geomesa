/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.locationtech.geomesa.utils.stats.StatHelpers._
import org.opengis.feature.simple.SimpleFeature

import scala.util.parsing.json.JSONObject

/**
 * The range histogram's state is stored in a hashmap, where the keys are the bins and the values are the counts
 *
 * @param attributeIndex attribute index for the attribute the histogram is being made for
 * @param numBins number of bins the histogram has
 * @param lowerEndpoint lower end of histogram
 * @param upperEndpoint upper end of histogram
 * @tparam T a comparable type which must have a StatHelperFunctions type class
 */
class RangeHistogram[T : StatHelperFunctions](attributeIndex: Int, numBins: Int, lowerEndpoint: T, upperEndpoint: T) extends Stat {
  val histogram = new collection.mutable.HashMap[T, Long]().withDefaultValue(0)

  val binHelper = implicitly[StatHelperFunctions[T]]
  val binSize = binHelper.getBinSize(numBins, lowerEndpoint, upperEndpoint)

  override def observe(sf: SimpleFeature): Unit = {
    val sfval = sf.getAttribute(attributeIndex).asInstanceOf[T]

    if (sfval != null) {
      val binIndex = binHelper.getBinIndex(sfval, binSize, lowerEndpoint, upperEndpoint)
      histogram(binIndex) += 1
    }
  }

  override def toJson(): String = {
    val jsonMap = histogram.toMap.map { case (k, v) => k.toString -> v }
    new JSONObject(jsonMap).toString()
  }

  override def add(other: Stat): Stat = {
    other match {
      case rangeHistogram: RangeHistogram[T] =>
        for (key <- rangeHistogram.histogram.keySet) {
          histogram(key) += rangeHistogram.histogram.get(key).get
        }
    }

    this
  }
}

