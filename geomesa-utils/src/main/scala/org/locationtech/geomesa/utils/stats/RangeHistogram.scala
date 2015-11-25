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

import scala.util.parsing.json.{JSONArray, JSONObject}

/**
 * Type classes for a BinHelper type which has to be a type which you can divide a range into
 * approximately equal sized sub ranges (which is necessary for a histogram).
 * Works for dates and any numeric type.
 */
object Bins {
  trait BinHelper[T] {
    def getBinSize(numBins: Int, lowerEndpoint: T, upperEndpoint: T): T
    def frequencyToHistogram(frequency: Frequency, numBins: Int, lowerEndpoint: T, binSize: T): Array[(T, Long)]
  }

  implicit object BinHelperDate extends BinHelper[Date] {
    override def getBinSize(numBins: Int, lowerEndpoint: Date, upperEndpoint: Date): Date = {
      new Date((upperEndpoint.getTime - lowerEndpoint.getTime) / numBins)
    }

    override def frequencyToHistogram(frequency: Frequency,
                                      numBins: Int,
                                      lowerEndpoint: Date,
                                      binSize: Date): Array[(Date, Long)] = {
      val histogram = new Array[(Date, Long)](numBins)

      var lowerBin = lowerEndpoint
      var upperBin = new Date(lowerEndpoint.getTime + binSize.getTime)
      var lowerCumFreq = frequency.getCumFreq(lowerBin)
      for (i <- 0 until numBins) {
        val upperCumFreq = frequency.getCumFreq(upperBin)
        histogram(i) = (lowerBin, upperCumFreq - lowerCumFreq)
        lowerBin = upperBin
        lowerCumFreq = upperCumFreq
        upperBin = new Date(upperBin.getTime + binSize.getTime)
      }

      histogram
    }
  }

  implicit object BinHelperLong extends BinHelper[java.lang.Long] {
    override def getBinSize(numBins: Int, lowerEndpoint: java.lang.Long, upperEndpoint: java.lang.Long): java.lang.Long = {
      (upperEndpoint - lowerEndpoint) / numBins
    }

    override def frequencyToHistogram(frequency: Frequency,
                                      numBins: Int,
                                      lowerEndpoint: java.lang.Long, 
                                      binSize: java.lang.Long): Array[(java.lang.Long, Long)] = {
      val histogram = new Array[(java.lang.Long, Long)](numBins)

      var lowerBin = lowerEndpoint
      var upperBin = lowerBin + binSize
      var lowerCumFreq = frequency.getCumFreq(lowerBin)
      for (i <- 0 until numBins) {
        val upperCumFreq = frequency.getCumFreq(upperBin)
        histogram(i) = (lowerBin, upperCumFreq - lowerCumFreq)
        lowerBin = upperBin
        lowerCumFreq = upperCumFreq
        upperBin = upperBin + binSize
      }

      histogram
    }
  }

  implicit object BinHelperInteger extends BinHelper[java.lang.Integer] {
    override def getBinSize(numBins: Int, lowerEndpoint: java.lang.Integer, upperEndpoint: java.lang.Integer): java.lang.Integer = {
      (upperEndpoint - lowerEndpoint) / numBins
    }

    override def frequencyToHistogram(frequency: Frequency,
                                      numBins: Int,
                                      lowerEndpoint: java.lang.Integer,
                                      binSize: java.lang.Integer): Array[(java.lang.Integer, Long)] = {
      val histogram = new Array[(java.lang.Integer, Long)](numBins)

      var lowerBin = lowerEndpoint
      var upperBin = lowerBin + binSize
      var lowerCumFreq = frequency.getCumFreq(lowerBin)
      for (i <- 0 until numBins) {
        val upperCumFreq = frequency.getCumFreq(upperBin)
        histogram(i) = (lowerBin, upperCumFreq - lowerCumFreq)
        lowerBin = upperBin
        lowerCumFreq = upperCumFreq
        upperBin = upperBin + binSize
      }

      histogram
    }
  }

  implicit object BinHelperDouble extends BinHelper[java.lang.Double] {
    override def getBinSize(numBins: Int, lowerEndpoint: java.lang.Double, upperEndpoint: java.lang.Double): java.lang.Double = {
      (upperEndpoint - lowerEndpoint) / numBins
    }

    override def frequencyToHistogram(frequency: Frequency,
                                      numBins: Int,
                                      lowerEndpoint: java.lang.Double,
                                      binSize: java.lang.Double): Array[(java.lang.Double, Long)] = {
      val histogram = new Array[(java.lang.Double, Long)](numBins)

      var lowerBin = lowerEndpoint
      var upperBin = lowerBin + binSize
      var lowerCumFreq = frequency.getCumFreq(lowerBin)
      for (i <- 0 until numBins) {
        val upperCumFreq = frequency.getCumFreq(upperBin)
        histogram(i) = (lowerBin, upperCumFreq - lowerCumFreq)
        lowerBin = upperBin
        lowerCumFreq = upperCumFreq
        upperBin = upperBin + binSize
      }

      histogram
    }
  }

  implicit object BinHelperFloat extends BinHelper[java.lang.Float] {
    override def getBinSize(numBins: Int, lowerEndpoint: java.lang.Float, upperEndpoint: java.lang.Float): java.lang.Float = {
      (upperEndpoint - lowerEndpoint) / numBins
    }

    override def frequencyToHistogram(frequency: Frequency,
                                      numBins: Int,
                                      lowerEndpoint: java.lang.Float,
                                      binSize: java.lang.Float): Array[(java.lang.Float, Long)] = {
      val histogram = new Array[(java.lang.Float, Long)](numBins)

      var lowerBin = lowerEndpoint
      var upperBin = lowerBin + binSize
      var lowerCumFreq = frequency.getCumFreq(lowerBin)
      for (i <- 0 until numBins) {
        val upperCumFreq = frequency.getCumFreq(upperBin)
        histogram(i) = (lowerBin, upperCumFreq - lowerCumFreq)
        lowerBin = upperBin
        lowerCumFreq = upperCumFreq
        upperBin = upperBin + binSize
      }

      histogram
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

