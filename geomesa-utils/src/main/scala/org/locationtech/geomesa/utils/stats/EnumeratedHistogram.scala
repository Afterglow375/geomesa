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

import scala.collection.mutable
import scala.util.parsing.json.JSONObject

sealed trait EnumeratedHistogram[T] extends Stat {
  def attrIndex: Int
  def attrType: String
  def frequencyMap: mutable.Map[T, Long]
}

object EnumeratedHistogram {
  def apply(attributeIndex: Int, attrTypeString: String): EnumeratedHistogram[_] = {
    val attrType = Class.forName(attrTypeString)
    attrType match {
      case _ if attrType == classOf[Date] =>
        new EnumeratedHistogramImpl[Date](attributeIndex, attrTypeString)
      case _ if attrType == classOf[java.lang.Integer] =>
        new EnumeratedHistogramImpl[java.lang.Integer](attributeIndex, attrTypeString)
      case _ if attrType == classOf[java.lang.Long] =>
        new EnumeratedHistogramImpl[java.lang.Long](attributeIndex, attrTypeString)
      case _ if attrType == classOf[java.lang.Float] =>
        new EnumeratedHistogramImpl[java.lang.Float](attributeIndex, attrTypeString)
      case _ if attrType == classOf[java.lang.Double] =>
        new EnumeratedHistogramImpl[java.lang.Double](attributeIndex, attrTypeString)
    }
  }

  private case class EnumeratedHistogramImpl[T](attrIndex: Int, attrType: String) extends EnumeratedHistogram[T] {
    override lazy val frequencyMap = mutable.HashMap[T, Long]().withDefaultValue(0)

    override def observe(sf: SimpleFeature): Unit = {
      val sfval = sf.getAttribute(attrIndex)
      if (sfval != null) {
        sfval match {
          case tval: T =>
            frequencyMap(tval) += 1
        }
      }
    }

    override def toJson(): String = {
      val jsonMap = frequencyMap.toMap.map { case (k, v) => k.toString -> v }
      new JSONObject(jsonMap).toString()
    }

    override def add(other: Stat): Stat = {
      other match {
        case eh: EnumeratedHistogram[T] =>
          combine(eh)
          this
      }
    }

    private def combine(other: EnumeratedHistogram[T]): Unit =
      other.frequencyMap.foreach { case (key: T, count: Long) => frequencyMap(key) += count }
  }
}

