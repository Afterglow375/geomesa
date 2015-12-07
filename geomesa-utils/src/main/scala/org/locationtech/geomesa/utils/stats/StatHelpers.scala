package org.locationtech.geomesa.utils.stats

import java.lang
import java.util.Date

import org.joda.time.format.DateTimeFormat
import org.opengis.feature.simple.{SimpleFeatureType, SimpleFeature}

object StatHelpers {

  val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

  /**
   * This trait provides the backbone for type classes used for various functions the stats use
   * @tparam T some type for which there's a corresponding type class implementation
   */
  trait StatHelperFunctions[T] {
    def getBinSize(numBins: Int, lowerEndpoint: T, upperEndpoint: T): T
    def getBinIndex(attributeValue: T, binSize: T, lowerEndpoint: T, upperEndpoint: T): T
    def fromString(value: String): T
  }

  implicit object StatHelperFunctionsDate extends StatHelperFunctions[Date] {
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

    override def fromString(value: String): Date = {
      dateFormat.parseDateTime(value).toDate
    }
  }

  implicit object StatHelperFunctionsLong extends StatHelperFunctions[lang.Long] {
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

    override def fromString(value: String): java.lang.Long = {
      java.lang.Long.parseLong(value)
    }
  }

  implicit object StatHelperFunctionsInteger extends StatHelperFunctions[java.lang.Integer] {
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

    override def fromString(value: String): java.lang.Integer = {
      java.lang.Integer.parseInt(value)
    }
  }

  implicit object StatHelperFunctionsDouble extends StatHelperFunctions[java.lang.Double] {
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

    override def fromString(value: String): java.lang.Double = {
      java.lang.Double.parseDouble(value)
    }
  }

  implicit object StatHelperFunctionsFloat extends StatHelperFunctions[java.lang.Float] {
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

    override def fromString(value: String): java.lang.Float = {
      java.lang.Float.parseFloat(value)
    }
  }
}