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
import org.joda.time.{DateTime, DateTimeZone, Interval}

/**
 * Type class for a BinAble type which has to be a type which you can divide a range into
 * approximately equal sized sub ranges (which is necessary for a histogram).
 */
object Bins {
  trait BinAble[T] {
    def getBinSize(numBins: Int, lowerEndpoint: T, upperEndpoint: T): Long
    def getBinIndex(value: T, numBins: Int, binSize: Long, range: collect.Range[T]): Int
  }

  class BinAbleDate(interval: collect.Range[Date], numBins: Int) extends BinAble[Date] {
    private val bucketSize: Long = (new DateTime(interval.upperEndpoint.getTime).millis - interval.lowerEndpoint.toMillis) / numBins

    override def getBinSize(numBins: Int, lowerEndpoint: Date, upperEndpoint: Date): Long = ???
    override def getBinIndex(value: Date, numBins: Int, binSize: Long, range: collect.Range[Date]): Int = ???
  }

  class BinAbleInt extends BinAble[Int] {
    override def getBinSize(numBins: Int, lowerEndpoint: Int, upperEndpoint: Int): Long = ???
    override def getBinIndex(value: Int, numBins: Int, binSize: Long, range: collect.Range[Int]): Int = ???
  }

  class BinAbleLong extends BinAble[Long] {
    override def getBinSize(numBins: Int, lowerEndpoint: Long, upperEndpoint: Long): Long = ???
    override def getBinIndex(value: Long, numBins: Int, binSize: Long, range: collect.Range[Long]): Int = ???
  }

  class BinAbleFloat extends BinAble[Float] {
    override def getBinSize(numBins: Int, lowerEndpoint: Float, upperEndpoint: Float): Long = ???
    override def getBinIndex(value: Float, numBins: Int, binSize: Long, range: collect.Range[Float]): Int = ???
  }

  class BinAbleDouble extends BinAble[Double] {
    override def getBinSize(numBins: Int, lowerEndpoint: Double, upperEndpoint: Double): Long = ???
    override def getBinIndex(value: Double, numBins: Int, binSize: Long, range: collect.Range[Double]): Int = ???
  }
}

import org.locationtech.geomesa.utils.stats.Bins._

class RangeHistogram[T](attribute: String, numBins: Int, lowerEndpoint: T, upperEndpoint: T)(implicit rangeSnap: BinAble[T]) extends Stat {
  private val binSize = rangeSnap.getBinSize(numBins, lowerEndpoint, upperEndpoint)
  private val histogramRange = com.google.common.collect.Ranges.closed(lowerEndpoint, upperEndpoint)

  val histogram = new collection.mutable.HashMap[Long, Long]()

  override def observe(sf: SimpleFeature): Unit = {
    val sfval = sf.getAttribute(attribute)

    if (sfval != null) {
      val binIndex: Long = rangeSnap.getBinIndex(sfval.asInstanceOf[T], numBins, binSize, histogramRange)
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


