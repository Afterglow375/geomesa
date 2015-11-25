/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.{Date, UUID}

import org.joda.time.format.DateTimeFormat
import org.opengis.feature.simple.SimpleFeature

import scala.util.parsing.combinator.RegexParsers

trait Stat {
  def observe(sf: SimpleFeature)
  def add(other: Stat): Stat
  def toJson(): String
}

object Stat {
  object StatParser {
    val attributeNameRegex = """\w+""".r
    val alphabetRegex = """[a-zA-Z]+""".r
    val numBinRegex = """[1-9][0-9]*""".r // any non-zero positive int
    val nonEmptyRegex = """[^,)]+""".r
    val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  }

  class StatParser extends RegexParsers {
    import StatParser._

    def minMaxParser: Parser[MinMax[_]] = {
      "MinMax(" ~> attributeNameRegex <~ ")" ^^ {
        case attribute: String => new MinMax[String](attribute)
      }
    }

    def iteratorStackParser: Parser[IteratorStackCounter] = {
      "IteratorCount" ^^ { case _ => new IteratorStackCounter() }
    }

    def enumeratedHistogramParser[T]: Parser[EnumeratedHistogram[T]] = {
      "EnumeratedHistogram(" ~> attributeNameRegex <~ ")" ^^ {
        case attribute: String => new EnumeratedHistogram[T](attribute)
      }
    }

    def rangeHistogramParser: Parser[RangeHistogram[_]] = {
      "RangeHistogram(" ~> attributeNameRegex ~ "," ~ alphabetRegex ~ "," ~ numBinRegex ~ "," ~ nonEmptyRegex ~ "," ~ nonEmptyRegex <~ ")" ^^ {
        case attributeName ~ "," ~ attributeType ~ "," ~ numBins ~ "," ~ lowerEndpoint ~ "," ~ upperEndpoint => {
          attributeType match {
            case "Date" => new RangeHistogram[java.util.Date](attributeName, numBins.toInt, dateFormat.parseDateTime(lowerEndpoint).toDate, dateFormat.parseDateTime(upperEndpoint).toDate)
            case "Integer" => new RangeHistogram[java.lang.Integer](attributeName, numBins.toInt, lowerEndpoint.toInt, upperEndpoint.toInt)
            case "Long" => new RangeHistogram[java.lang.Long](attributeName, numBins.toInt, lowerEndpoint.toLong, upperEndpoint.toLong)
            case "Double" => new RangeHistogram[java.lang.Double](attributeName, numBins.toInt, lowerEndpoint.toDouble, upperEndpoint.toDouble)
            case "Float" => new RangeHistogram[java.lang.Float](attributeName, numBins.toInt, lowerEndpoint.toFloat, upperEndpoint.toFloat)
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
        case statParsers: Seq[Stat] => new SeqStat(statParsers)
      }
    }

    def parse(s: String): Stat = {
      parseAll(statsParser, s) match {
        case Success(result, _) => result
        case failure: NoSuccess =>
          throw new Exception(s"Could not parse $s.")
      }
    }
  }

  // Stat's apply method shoul take a SFT and do light validation.
  def apply(s: String) = new StatParser().parse(s)
}
