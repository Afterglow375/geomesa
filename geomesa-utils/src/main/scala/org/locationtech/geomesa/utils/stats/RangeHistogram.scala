/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import com.google.common.collect
import org.opengis.feature.simple.SimpleFeature

/**
 * Type class for a BinAble type which has to be a type which you can divide a range into
 * approximately equal sized sub ranges (which is necessary for a histogram).
 */
object Bins {
  trait BinAble[T] {

    /**
     * Computes the appropriate histogram bin that a value falls into
     * @param value an attribute value from a simple feature
     * @return the lower end of the histogram bin that the value falls into
     */
    def getBin(value: T): T
  }

  class BinAbleDate(interval: collect.Range[Date], numBins: Int) extends BinAble[Date] {
    private val binSize = (interval.upperEndpoint.getTime - interval.lowerEndpoint.getTime) / numBins

    override def getBin(value: Date): Date = {
      if (interval.contains(value)) {
        val bucketIndex = (value.getTime - interval.lowerEndpoint.getTime) / binSize
        new Date(interval.lowerEndpoint.getTime + (binSize * bucketIndex))
      } else if (value.getTime > interval.upperEndpoint.getTime) {
        interval.upperEndpoint
      } else {
        interval.lowerEndpoint
      }
    }
  }

  class BinAbleInt(interval: collect.Range[Int], numBins: Int) extends BinAble[Int] {
    private val binSize = (interval.upperEndpoint - interval.lowerEndpoint) / numBins

    override def getBin(value: Int): Int = {
      if (interval.contains(value)) {
        val bucketIndex = (value - interval.lowerEndpoint) / binSize
        interval.lowerEndpoint + (binSize * bucketIndex)
      } else if (value > interval.upperEndpoint) {
        interval.upperEndpoint
      } else {
        interval.lowerEndpoint
      }
    }
  }

  class BinAbleLong(interval: collect.Range[Long], numBins: Int) extends BinAble[Long] {
    private val binSize = (interval.upperEndpoint - interval.lowerEndpoint) / numBins

    override def getBin(value: Long): Long = {
      if (interval.contains(value)) {
        val bucketIndex = (value - interval.lowerEndpoint) / binSize
        interval.lowerEndpoint + (binSize * bucketIndex)
      } else if (value > interval.upperEndpoint) {
        interval.upperEndpoint
      } else {
        interval.lowerEndpoint
      }
    }
  }

  class BinAbleFloat(interval: collect.Range[Float], numBins: Int) extends BinAble[Float] {
    private val binSize = (interval.upperEndpoint - interval.lowerEndpoint) / numBins

    override def getBin(value: Float): Float = {
      if (interval.contains(value)) {
        val bucketIndex = (value - interval.lowerEndpoint) / binSize
        interval.lowerEndpoint + (binSize * bucketIndex.toInt)
      } else if (value > interval.upperEndpoint) {
        interval.upperEndpoint
      } else {
        interval.lowerEndpoint
      }
    }
  }

  class BinAbleDouble(interval: collect.Range[Double], numBins: Int) extends BinAble[Double] {
    private val binSize = (interval.upperEndpoint - interval.lowerEndpoint) / numBins

    override def getBin(value: Double): Double = {
      if (interval.contains(value)) {
        val bucketIndex = (value - interval.lowerEndpoint) / binSize
        interval.lowerEndpoint + (binSize * bucketIndex.toInt)
      } else if (value > interval.upperEndpoint) {
        interval.upperEndpoint
      } else {
        interval.lowerEndpoint
      }
    }
  }
}

import org.locationtech.geomesa.utils.stats.Bins._

class RangeHistogram[T](attribute: String, numBins: Int, lowerEndpoint: T, upperEndpoint: T)(implicit rangeSnap: BinAble[T]) extends Stat {
  private val histogramRange = collect.Ranges.closed(lowerEndpoint, upperEndpoint)

  val histogram = new collection.mutable.HashMap[Long, Long]()

  override def observe(sf: SimpleFeature): Unit = {
    val sfval = sf.getAttribute(attribute)

    if (sfval != null) {
      val binIndex: Long = rangeSnap.getBin(sfval.asInstanceOf[T], numBins, binSize, histogramRange)
      val cur: Long = histogram.getOrElse(binIndex, 0L)
      histogram.put(binIndex, cur + 1L)
    }
  }

  override def toJson(): String = ???

  override def add(other: Stat): Stat = ???
}

object RangeHistogram {
  type HistogramMap = collection.mutable.HashMap[_, Long]
}


