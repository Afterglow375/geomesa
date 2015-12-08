/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.lang
import java.util.Date

import org.opengis.feature.simple.SimpleFeature

import scala.util.parsing.json.JSONObject

/**
 * This object provides the type classes used by the range histogram
 */
object BinHelper {
  trait BinAble[T] {
    def getBinSize(numBins: Int, lowerEndpoint: T, upperEndpoint: T): T
    def getBinIndex(attributeValue: T, binSize: T, lowerEndpoint: T, upperEndpoint: T): T
  }

  implicit object BinAbleDate extends BinAble[Date] {
    override def getBinSize(numBins: Int, lowerEndpoint: Date, upperEndpoint: Date): Date = {
      new Date((upperEndpoint.getTime - lowerEndpoint.getTime) / numBins)
    }

    override def getBinIndex(attributeValue: Date,
                             binSize: Date,
                             lowerEndpoint: Date,
                             upperEndpoint: Date): Date = {
      if (attributeValue.getTime >= upperEndpoint.getTime) {
        new Date(upperEndpoint.getTime - binSize.getTime)
      } else if (attributeValue.getTime <= lowerEndpoint.getTime) {
        lowerEndpoint
      } else {
        val bucketIndex = (attributeValue.getTime - lowerEndpoint.getTime) / binSize.getTime
        new Date(lowerEndpoint.getTime + (binSize.getTime * bucketIndex))
      }
    }
  }

  implicit object BinAbleLong extends BinAble[lang.Long] {
    override def getBinSize(numBins: Int, lowerEndpoint: lang.Long, upperEndpoint: lang.Long): lang.Long = {
      (upperEndpoint - lowerEndpoint) / numBins
    }

    override def getBinIndex(attributeValue: lang.Long,
                             binSize: lang.Long,
                             lowerEndpoint: lang.Long,
                             upperEndpoint: lang.Long): lang.Long = {
      if (attributeValue >= upperEndpoint) {
        upperEndpoint - binSize
      } else if (attributeValue <= lowerEndpoint) {
        lowerEndpoint
      } else {
        val bucketIndex = (attributeValue - lowerEndpoint) / binSize
        lowerEndpoint + (binSize * bucketIndex)
      }
    }
  }

  implicit object BinAbleInteger extends BinAble[java.lang.Integer] {
    override def getBinSize(numBins: Int, lowerEndpoint: java.lang.Integer, upperEndpoint: java.lang.Integer): java.lang.Integer = {
      (upperEndpoint - lowerEndpoint) / numBins
    }

    override def getBinIndex(attributeValue: Integer,
                             binSize: Integer,
                             lowerEndpoint: Integer,
                             upperEndpoint: Integer): Integer = {
      if (attributeValue >= upperEndpoint) {
        upperEndpoint - binSize
      } else if (attributeValue <= lowerEndpoint) {
        lowerEndpoint
      } else {
        val bucketIndex = (attributeValue - lowerEndpoint) / binSize
        lowerEndpoint + (binSize * bucketIndex)
      }
    }
  }

  implicit object BinAbleDouble extends BinAble[java.lang.Double] {
    override def getBinSize(numBins: Int, lowerEndpoint: java.lang.Double, upperEndpoint: java.lang.Double): java.lang.Double = {
      (upperEndpoint - lowerEndpoint) / numBins
    }

    override def getBinIndex(attributeValue: lang.Double,
                             binSize: lang.Double,
                             lowerEndpoint: lang.Double,
                             upperEndpoint: lang.Double): lang.Double = {
      if (attributeValue >= upperEndpoint) {
        upperEndpoint - binSize
      } else if (attributeValue <= lowerEndpoint) {
        lowerEndpoint
      } else {
        val bucketIndex = (attributeValue - lowerEndpoint) / binSize
        lowerEndpoint + (binSize * bucketIndex)
      }
    }
  }

  implicit object BinAbleFloat extends BinAble[java.lang.Float] {
    override def getBinSize(numBins: Int, lowerEndpoint: java.lang.Float, upperEndpoint: java.lang.Float): java.lang.Float = {
      (upperEndpoint - lowerEndpoint) / numBins
    }

    override def getBinIndex(attributeValue: lang.Float,
                             binSize: lang.Float,
                             lowerEndpoint: lang.Float,
                             upperEndpoint: lang.Float): lang.Float = {
      if (attributeValue >= upperEndpoint) {
        upperEndpoint - binSize
      } else if (attributeValue <= lowerEndpoint) {
        lowerEndpoint
      } else {
        val bucketIndex = (attributeValue - lowerEndpoint) / binSize
        lowerEndpoint + (binSize * bucketIndex)
      }
    }
  }
}

import org.locationtech.geomesa.utils.stats.BinHelper._

sealed trait RangeHistogram[T] extends Stat {
  def attributeIndex: Int
  def numBins: Int
  def lowerEndpoint: T
  def upperEndpoint: T
  def histogram: collection.mutable.Map[T, Long]
}

object RangeHistogram {
  def apply(attrIndex: Int, attrTypeString: String, numBins: String, lowerEndpoint: String, upperEndpoint: String): RangeHistogram[_] = {
    val attrType = Class.forName(attrTypeString)
    attrType match {
      case _ if attrType == classOf[Date] =>
        new RangeHistogramImpl[Date](attrIndex, attrTypeString, numBins.toInt,
          StatHelpers.dateFormat.parseDateTime(lowerEndpoint).toDate, StatHelpers.dateFormat.parseDateTime(upperEndpoint).toDate)
      case _ if attrType == classOf[java.lang.Integer] =>
        new RangeHistogramImpl[java.lang.Integer](attrIndex, attrTypeString, numBins.toInt, lowerEndpoint.toInt, upperEndpoint.toInt)
      case _ if attrType == classOf[java.lang.Long] =>
        new RangeHistogramImpl[java.lang.Long](attrIndex, attrTypeString, numBins.toInt, lowerEndpoint.toLong, upperEndpoint.toLong)
      case _ if attrType == classOf[java.lang.Double] =>
        new RangeHistogramImpl[java.lang.Double](attrIndex, attrTypeString, numBins.toInt, lowerEndpoint.toDouble, upperEndpoint.toDouble)
      case _ if attrType == classOf[java.lang.Float] =>
        new RangeHistogramImpl[java.lang.Float](attrIndex, attrTypeString, numBins.toInt, lowerEndpoint.toFloat, upperEndpoint.toFloat)
    }
  }

  /**
   * The range histogram's state is stored in a hashmap, where the keys are the bins and the values are the counts
   *
   * @param attributeIndex attribute index for the attribute the histogram is being made for
   * @param numBins number of bins the histogram has
   * @param lowerEndpoint lower end of histogram
   * @param upperEndpoint upper end of histogram
   * @tparam T a comparable type which must have a StatHelperFunctions type class
   */
  private case class RangeHistogramImpl[T : BinAble](attributeIndex: Int,
                                                     attrTypeString: String,
                                                     numBins: Int,
                                                     lowerEndpoint: T,
                                                     upperEndpoint: T) extends RangeHistogram[T] {
    override lazy val histogram = new collection.mutable.HashMap[T, Long]().withDefaultValue(0)

    val binHelper = implicitly[BinAble[T]]
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
}


