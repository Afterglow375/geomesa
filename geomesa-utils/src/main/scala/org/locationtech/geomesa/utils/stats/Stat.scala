/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.joda.time.format.DateTimeFormat
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.parsing.combinator.RegexParsers

/**
 * Stats used by the StatsIterator to compute various statistics server-side for a given query.
 */
trait Stat {
  /**
   * Compute statistics based upon the given simple feature.
   * This method will be called for every SimpleFeature a query returns.
   * @param sf
   */
  def observe(sf: SimpleFeature)

  /**
   * Meant to be used to combine two Stats of the same subtype.
   * Used in the "reduce" step client-side.
   * @param other
   * @return
   */
  def add(other: Stat): Stat

  /**
   * Serves as serialization needed for storing the computed statistic in a SimpleFeature.
   * @return
   */
  def toJson(): String
}

/**
 * This class contains parsers which dictate how to instantiate a particular Stat.
 * Stats are created by passing a stats string as a query hint (QueryHints.STATS_STRING).
 *
 * A valid stats string should adhere to the parsers here:
 * e.g. "MinMax(attributeName);IteratorCount" or "RangeHistogram(attributeName,10,0,100)"
 * (see tests for more use cases)
 */
object Stat {
  class StatParser(sft: SimpleFeatureType) extends RegexParsers {
    val attributeNameRegex = """\w+""".r
    val numBinRegex = """[1-9][0-9]*""".r // any non-zero positive int
    val nonEmptyRegex = """[^,)]+""".r
    val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

    /**
     * Obtains the index of the attribute within the SFT
     * @param attribute attribute name as a string
     * @return attribute index
     */
    private def getAttrIndex(attribute: String): Int = {
      val attrIndex = sft.indexOf(attribute)
      if (attrIndex == -1)
        throw new Exception(s"Invalid attribute name in stat string: $attribute")
      attrIndex
    }

    def minMaxParser: Parser[MinMax[_]] = {
      "MinMax(" ~> attributeNameRegex <~ ")" ^^ {
        case attribute =>
          val attrIndex = getAttrIndex(attribute)
          val attrTypeString = sft.getType(attribute).getBinding.getName
          MinMax(attrIndex, attrTypeString, null, null)
      }
    }

    def iteratorStackParser: Parser[IteratorStackCounter] = {
      "IteratorStackCounter" ^^ { case _ => IteratorStackCounter() }
    }

    def enumeratedHistogramParser[T]: Parser[EnumeratedHistogram[T]] = {
      "EnumeratedHistogram(" ~> attributeNameRegex <~ ")" ^^ {
        case attribute =>
          val attrIndex = getAttrIndex(attribute)
          val attrTypeString = sft.getType(attribute).getBinding.getName
          EnumeratedHistogram[T](attrIndex, attrTypeString)
      }
    }

    def rangeHistogramParser: Parser[RangeHistogram[_]] = {
      "RangeHistogram(" ~> attributeNameRegex ~ "," ~ numBinRegex ~ "," ~ nonEmptyRegex ~ "," ~ nonEmptyRegex <~ ")" ^^ {
        case attribute ~ "," ~ numBins ~ "," ~ lowerEndpoint ~ "," ~ upperEndpoint =>
          val attrIndex = getAttrIndex(attribute)
          val attrTypeString = sft.getType(attribute).getBinding.getName
          RangeHistogram(attrIndex, attrTypeString, numBins, lowerEndpoint, upperEndpoint)
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
