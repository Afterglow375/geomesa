/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import org.joda.time.format.DateTimeFormat
import org.opengis.feature.simple.{SimpleFeatureType, SimpleFeature}

import scala.util.parsing.combinator.RegexParsers

/**
 * Stats used by the StatsIterator to gain various statistics for a given query.
 */
trait Stat {
  def observe(sf: SimpleFeature)
  def add(other: Stat): Stat
  def toJson(): String
}

object Stat {
  class StatParser(sft: SimpleFeatureType) extends RegexParsers {
    val attributeNameRegex = """\w+""".r
    val numBinRegex = """[1-9][0-9]*""".r // any non-zero positive int
    val nonEmptyRegex = """[^,)]+""".r
    val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

    def minMaxParser: Parser[MinMax[_]] = {
      "MinMax(" ~> attributeNameRegex <~ ")" ^^ {
        case attribute =>
          val attrIndex = sft.indexOf(attribute)
          if (attrIndex == -1)
            throw new Exception(s"Invalid attribute name in stat string: $attribute")
          new MinMax[java.lang.Long](attrIndex)
      }
    }

    def iteratorStackParser: Parser[IteratorStackCounter] = {
      "IteratorCount" ^^ { case _ => new IteratorStackCounter() }
    }

    def enumeratedHistogramParser[T]: Parser[EnumeratedHistogram[T]] = {
      "EnumeratedHistogram(" ~> attributeNameRegex <~ ")" ^^ {
        case attribute =>
          val attrIndex = sft.indexOf(attribute)
          if (attrIndex == -1)
            throw new Exception(s"Invalid attribute name in stat string: $attribute")
          new EnumeratedHistogram[T](attrIndex)
      }
    }

    def rangeHistogramParser: Parser[RangeHistogram[_]] = {
      "RangeHistogram(" ~> attributeNameRegex ~ "," ~ numBinRegex ~ "," ~ nonEmptyRegex ~ "," ~ nonEmptyRegex <~ ")" ^^ {
        case attribute ~ "," ~ numBins ~ "," ~ lowerEndpoint ~ "," ~ upperEndpoint => {
          val attrIndex = sft.indexOf(attribute)
          if (attrIndex == -1)
            throw new Exception(s"Invalid attribute name in stat string: $attribute")
          sft.getType(attribute).getBinding match {
            case v if v == classOf[Date] =>
              new RangeHistogram[java.util.Date](attrIndex, numBins.toInt, dateFormat.parseDateTime(lowerEndpoint).toDate, dateFormat.parseDateTime(upperEndpoint).toDate)
            case v if v == classOf[java.lang.Integer] =>
              new RangeHistogram[java.lang.Integer](attrIndex, numBins.toInt, lowerEndpoint.toInt, upperEndpoint.toInt)
            case v if v == classOf[java.lang.Long] =>
              new RangeHistogram[java.lang.Long](attrIndex, numBins.toInt, lowerEndpoint.toLong, upperEndpoint.toLong)
            case v if v == classOf[java.lang.Double] =>
              new RangeHistogram[java.lang.Double](attrIndex, numBins.toInt, lowerEndpoint.toDouble, upperEndpoint.toDouble)
            case v if v == classOf[java.lang.Float] =>
              new RangeHistogram[java.lang.Float](attrIndex, numBins.toInt, lowerEndpoint.toFloat, upperEndpoint.toFloat)
          }
        }
      }
    }

    def statParser: Parser[Stat] = {
      minMaxParser |
        iteratorStackParser |
        enumeratedHistogramParser |
        rangeHistogramParser
    }

    def statsParser: Parser[Stat] = {
      rep1sep(statParser, ";") ^^ {
        case statParsers: Seq[Stat] =>
          if (statParsers.length == 1) statParsers.head else new SeqStat(statParsers)
      }
    }

    def parse(s: String): Stat = {
      parseAll(statsParser, s) match {
        case Success(result, _) => result
        case failure: NoSuccess =>
          throw new Exception(s"Could not parse $s")
      }
    }
  }

  def apply(sft: SimpleFeatureType, s: String) = new StatParser(sft).parse(s)
}
