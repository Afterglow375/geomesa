/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import org.apache.commons.math3.stat.Frequency
import org.opengis.feature.simple.SimpleFeature

import scala.collection.mutable
import scala.util.parsing.json.JSONObject

/**
 * Type classes for a BinHelper type which has to be a type which you can divide a range into
 * approximately equal sized sub ranges (which is necessary for a histogram).
 * Works for dates and any numeric type.
 */
object Bins {
  trait BinHelper[T] {
    def getBinSize(numBins: Int, lowerEndpoint: T, upperEndpoint: T): T
    def frequencyToHistogram(frequency: Frequency, numBins: Int, lowerEndpoint: T, binSize: T): collection.mutable.HashMap[T, Long]
  }

  implicit object BinHelperDate extends BinHelper[Date] {
    override def getBinSize(numBins: Int, lowerEndpoint: Date, upperEndpoint: Date): Date = {
      new Date((lowerEndpoint.getTime - upperEndpoint.getTime) / numBins)
    }

    override def frequencyToHistogram(frequency: Frequency,
                                      numBins: Int,
                                      lowerEndpoint: Date,
                                      binSize: Date): collection.mutable.HashMap[Date, Long] = {
      val histogramMap = new collection.mutable.HashMap[Date, Long]()

      var lowerBin = lowerEndpoint
      var upperBin = new Date(lowerEndpoint.getTime + binSize.getTime)
      for (i <- 0 until numBins) {
        histogramMap.put(lowerBin, frequency.getCumFreq(upperBin) - frequency.getCumFreq(lowerBin))
        lowerBin = upperBin
        upperBin = new Date(upperBin.getTime + binSize.getTime)
      }

      histogramMap
    }
  }

  implicit object BinHelperLong extends BinHelper[java.lang.Long] {
    override def getBinSize(numBins: Int, lowerEndpoint: java.lang.Long, upperEndpoint: java.lang.Long): java.lang.Long = {
      (lowerEndpoint - upperEndpoint) / numBins
    }

    override def frequencyToHistogram(frequency: Frequency, numBins: Int, lowerEndpoint: java.lang.Long, binSize: java.lang.Long): mutable.HashMap[java.lang.Long, Long] = {
      val histogramMap = new collection.mutable.HashMap[java.lang.Long, Long]()

      var lowerBin = lowerEndpoint
      var upperBin = lowerBin + binSize
      for (i <- 0 until numBins) {
        histogramMap.put(lowerBin, frequency.getCumFreq(upperBin) - frequency.getCumFreq(lowerBin))
        lowerBin = upperBin
        upperBin = upperBin + binSize
      }

      histogramMap
    }
  }

  implicit object BinHelperInteger extends BinHelper[java.lang.Integer] {
    override def getBinSize(numBins: Int, lowerEndpoint: java.lang.Integer, upperEndpoint: java.lang.Integer): java.lang.Integer = {
      (lowerEndpoint - upperEndpoint) / numBins
    }

    override def frequencyToHistogram(frequency: Frequency, numBins: Int, lowerEndpoint: java.lang.Integer, binSize: java.lang.Integer): mutable.HashMap[java.lang.Integer, Long] = {
      val histogramMap = new collection.mutable.HashMap[java.lang.Integer, Long]()

      var lowerBin = lowerEndpoint
      var upperBin = lowerBin + binSize
      for (i <- 0 until numBins) {
        histogramMap.put(lowerBin, frequency.getCumFreq(upperBin) - frequency.getCumFreq(lowerBin))
        lowerBin = upperBin
        upperBin = upperBin + binSize
      }

      histogramMap
    }
  }

  implicit object BinHelperDouble extends BinHelper[java.lang.Double] {
    override def getBinSize(numBins: Int, lowerEndpoint: java.lang.Double, upperEndpoint: java.lang.Double): java.lang.Double = {
      (lowerEndpoint - upperEndpoint) / numBins
    }

    override def frequencyToHistogram(frequency: Frequency, numBins: Int, lowerEndpoint: java.lang.Double, binSize: java.lang.Double): mutable.HashMap[java.lang.Double, Long] = {
      val histogramMap = new collection.mutable.HashMap[java.lang.Double, Long]()

      var lowerBin = lowerEndpoint
      var upperBin = lowerBin + binSize
      for (i <- 0 until numBins) {
        histogramMap.put(lowerBin, frequency.getCumFreq(upperBin) - frequency.getCumFreq(lowerBin))
        lowerBin = upperBin
        upperBin = upperBin + binSize
      }

      histogramMap
    }
  }

  implicit object BinHelperFloat extends BinHelper[java.lang.Float] {
    override def getBinSize(numBins: Int, lowerEndpoint: java.lang.Float, upperEndpoint: java.lang.Float): java.lang.Float = {
      (lowerEndpoint - upperEndpoint) / numBins
    }

    override def frequencyToHistogram(frequency: Frequency, numBins: Int, lowerEndpoint: java.lang.Float, binSize: java.lang.Float): mutable.HashMap[java.lang.Float, Long] = {
      val histogramMap = new collection.mutable.HashMap[java.lang.Float, Long]()

      var lowerBin = lowerEndpoint
      var upperBin = lowerBin + binSize
      for (i <- 0 until numBins) {
        histogramMap.put(lowerBin, frequency.getCumFreq(upperBin) - frequency.getCumFreq(lowerBin))
        lowerBin = upperBin
        upperBin = upperBin + binSize
      }

      histogramMap
    }
  }
}

import org.locationtech.geomesa.utils.stats.Bins._

/**
 * The range histogram's state is stored in the Frequency object, though
 * a hashmap is computed at the end for the JSON or serialized output.
 *
 * @param attribute attribute name as a String
 * @param numBins number of bins the histogram has
 * @param lowerEndpoint lower end of histogram
 * @param upperEndpoint upper end of histogram
 * @tparam T a comparable type which must have a BinHelper type class
 */
class RangeHistogram[T : BinHelper](attribute: String, numBins: Int, lowerEndpoint: T, upperEndpoint: T) extends Stat {
  val frequency = new Frequency()

  val binHelper = implicitly[BinHelper[T]]
  val binSize = binHelper.getBinSize(numBins, lowerEndpoint, upperEndpoint)

  override def observe(sf: SimpleFeature): Unit = {
    val sfval: Comparable[T] = sf.getAttribute(attribute).asInstanceOf[Comparable[T]]

    if (sfval != null) {
      frequency.addValue(sfval)
    }
  }

  override def toJson(): String = {
    val histogram = binHelper.frequencyToHistogram(frequency, numBins, lowerEndpoint, binSize)
    val jsonMap = histogram.toMap.map { case (k, v) => k.toString -> v }
    new JSONObject(jsonMap).toString()
  }

  override def add(other: Stat): Stat = {
    other match {
      case rangeHistogram: RangeHistogram[T] =>
        frequency.merge(rangeHistogram.frequency)
    }

    this
  }
}

