/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import org.opengis.feature.simple.SimpleFeature

sealed trait MinMax[T] extends Stat {
  def attributeIndex: Int
  def classType: String
  def min: T
  def max: T
}

object MinMax {
  /**
   * Creates a MinMax object depending on the type of the attribute.
   * Works with dates, integers, longs, doubles, and floats.
   * @param attrIndex attribute index within the SFT
   * @param attrTypeString the type of the attribute (e.g. float, double, int, etc). Necessary for serialization
   * @param min minimum value
   * @param max maximum value
   * @return a MinMax object
   */
  def apply(attrIndex: Int, attrTypeString: String, min: String, max: String): MinMax[_] = {
    val attrType = Class.forName(attrTypeString)
    attrType match {
      case _ if attrType == classOf[Date] =>
        if (min == null && max == null) {
          new MinMaxImpl[Date](attrIndex, attrTypeString, new Date(java.lang.Long.MAX_VALUE), new Date(java.lang.Long.MIN_VALUE))
        } else {
          new MinMaxImpl[Date](attrIndex, attrTypeString,
            StatHelpers.dateFormat.parseDateTime(min).toDate, StatHelpers.dateFormat.parseDateTime(max).toDate)
        }
      case _ if attrType == classOf[java.lang.Integer] =>
        if (min == null && max == null) {
          new MinMaxImpl[java.lang.Integer](attrIndex, attrTypeString, Integer.MAX_VALUE, Integer.MIN_VALUE)
        } else {
          new MinMaxImpl[java.lang.Integer](attrIndex, attrTypeString, min.toInt, max.toInt)
        }
      case _ if attrType == classOf[java.lang.Long] =>
        if (min == null && max == null) {
          new MinMaxImpl[java.lang.Long](attrIndex, attrTypeString, java.lang.Long.MAX_VALUE, java.lang.Long.MIN_VALUE)
        } else {
          new MinMaxImpl[java.lang.Long](attrIndex, attrTypeString, min.toLong, max.toLong)
        }
      case _ if attrType == classOf[java.lang.Float] =>
        if (min == null && max == null) {
          new MinMaxImpl[java.lang.Float](attrIndex, attrTypeString, java.lang.Float.MAX_VALUE, java.lang.Float.MIN_VALUE)
        } else {
          new MinMaxImpl[java.lang.Float](attrIndex, attrTypeString, min.toFloat, max.toFloat)
        }
      case _ if attrType == classOf[java.lang.Double] =>
        if (min == null && max == null) {
          new MinMaxImpl[java.lang.Double](attrIndex, attrTypeString, java.lang.Double.MAX_VALUE, java.lang.Double.MIN_VALUE)
        } else {
          new MinMaxImpl[java.lang.Double](attrIndex, attrTypeString, min.toDouble, max.toDouble)
        }
    }
  }

  /**
   * The MinMax stat merely returns the min/max of an attribute's values.
   * @param attributeIndex attribute index for the attribute the histogram is being made for
   * @param classType class type as a string to make serialization easier
   * @param min minimum value
   * @param max maximum value
   * @tparam T the type of the attribute the stat is targeting (needs to be comparable)
   */
  private case class MinMaxImpl[T <: Comparable[T]](attributeIndex: Int, classType: String, var min: T, var max: T) extends MinMax[T] {
    if (min == null || max == null) {
      throw new Exception("Null min or max encountered when creating MinMax class.") // shouldn't happen, but just to be safe
    }

    override def observe(sf: SimpleFeature): Unit = {
      val sfval = sf.getAttribute(attributeIndex)

      if (sfval != null) {
        updateMin(sfval.asInstanceOf[T])
        updateMax(sfval.asInstanceOf[T])
      }
    }

    override def add(other: Stat): Stat = {
      other match {
        case mm: MinMax[T] =>
          updateMin(mm.min)
          updateMax(mm.max)
      }

      this
    }

    private def updateMin(sfval: T): Unit = {
      if (min.compareTo(sfval) > 0) {
        min = sfval
      }
    }

    private def updateMax(sfval: T): Unit = {
      if (max.compareTo(sfval) < 0) {
        max = sfval
      }
    }

    override def toJson(): String = s"""{ "min": $min, "max": $max }"""
  }
}