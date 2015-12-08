/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.nio.ByteBuffer
import java.util.Date

import com.google.common.primitives.Bytes

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Stats are serialized as a byte array where the first byte indicates which type of stat is present.
 * The next four bits contain the size of the serialized information.
 * A SeqStat is serialized the same way, with each individual stat immediately following the previous in the byte array.
 */
object StatSerialization {
  // bytes indicating the type of stat
  val MINMAX_BYTE: Byte     = '0'
  val ISC_BYTE: Byte        = '1'
  val EH_BYTE: Byte         = '2'
  val RH_BYTE: Byte         = '3'

  private def serializeStat(kind: Byte, bytes: Array[Byte]): Array[Byte] = {
    val size = ByteBuffer.allocate(4).putInt(bytes.length).array
    Bytes.concat(Array(kind), size, bytes)
  }

  protected [stats] def packMinMax(mm: MinMax[_]): Array[Byte] = {
    serializeStat(MINMAX_BYTE, s"${mm.attrIndex};${mm.attrType};${mm.min};${mm.max}".getBytes)
  }

  protected [stats] def unpackMinMax(bytes: Array[Byte]): MinMax[_] = {
    val split = new String(bytes).split(";")
    require(split.size == 4)
    MinMax(java.lang.Integer.parseInt(split(0)), split(1), split(2), split(3))
  }

  protected [stats] def packISC(isc: IteratorStackCounter): Array[Byte] = {
    serializeStat(ISC_BYTE, s"${isc.count}".getBytes)
  }

  protected [stats] def unpackIteratorStackCounter(bytes: Array[Byte]): IteratorStackCounter = {
    val stat = new IteratorStackCounter()
    stat.count = java.lang.Long.parseLong(new String(bytes))
    stat
  }

  protected [stats] def packEnumeratedHistogram(eh: EnumeratedHistogram[_]): Array[Byte] = {
    val sb = new StringBuilder(s"${eh.attrIndex};${eh.attrType};")

    val keyValues = eh.frequencyMap.map { case (key, count) => s"${key.toString}->$count" }.mkString(",")
    sb.append(keyValues)

    serializeStat(EH_BYTE, sb.toString().getBytes)
  }

  protected [stats] def unpackEnumeratedHistogram(bytes: Array[Byte]): EnumeratedHistogram[_] = {
    val split = new String(bytes).split(";")
    require(split.size == 3)

    val attrIndex = split(0).toInt
    val attrTypeString = split(1)
    val keyValues = split(2).split(",")

    val attrType = Class.forName(attrTypeString)
    attrType match {
      case _ if attrType == classOf[Date] =>
        val eh = EnumeratedHistogram(attrIndex, attrTypeString).asInstanceOf[EnumeratedHistogram[Date]]
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            eh.frequencyMap.put(StatHelpers.dateFormat.parseDateTime(splitKeyValuePair(0)).toDate, splitKeyValuePair(1).toLong)
        }
        eh
      case _ if attrType == classOf[Integer] =>
        val eh = EnumeratedHistogram(attrIndex, attrTypeString).asInstanceOf[EnumeratedHistogram[Integer]]
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            eh.frequencyMap.put(splitKeyValuePair(0).toInt, splitKeyValuePair(1).toLong)
        }
        eh
      case _ if attrType == classOf[java.lang.Long] =>
        val eh = EnumeratedHistogram(attrIndex, attrTypeString).asInstanceOf[EnumeratedHistogram[java.lang.Long]]
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            eh.frequencyMap.put(splitKeyValuePair(0).toLong, splitKeyValuePair(1).toLong)
        }
        eh
      case _ if attrType == classOf[java.lang.Float] =>
        val eh = EnumeratedHistogram(attrIndex, attrTypeString).asInstanceOf[EnumeratedHistogram[java.lang.Float]]
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            eh.frequencyMap.put(splitKeyValuePair(0).toFloat, splitKeyValuePair(1).toLong)
        }
        eh
      case _ if attrType == classOf[java.lang.Double] =>
        val eh = EnumeratedHistogram(attrIndex, attrTypeString).asInstanceOf[EnumeratedHistogram[java.lang.Double]]
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            eh.frequencyMap.put(splitKeyValuePair(0).toDouble, splitKeyValuePair(1).toLong)
        }
        eh
    }
  }

  protected [stats] def packRangeHistogram(rh: RangeHistogram[_]): Array[Byte] = {
    val sb = new StringBuilder(s"${rh.attrIndex};${rh.attrType};${rh.numBins};${rh.lowerEndpoint};${rh.upperEndpoint};")

    val keyValues = rh.histogram.map { case (key, count) => s"${key.toString}->$count" }.mkString(",")
    sb.append(keyValues)

    serializeStat(RH_BYTE, sb.toString().getBytes)
  }

  protected [stats] def unpackRangeHistogram(bytes: Array[Byte]): RangeHistogram[_] = {
    val split = new String(bytes).split(";")
    require(split.size == 6)

    val attrIndex = split(0).toInt
    val attrTypeString = split(1)
    val numBins = split(2)
    val lowerEndpoint = split(3)
    val upperEndpoint = split(4)
    val keyValues = split(5).split(",")

    val attrType = Class.forName(attrTypeString)
    attrType match {
      case _ if attrType == classOf[Date] =>
        val rh = RangeHistogram(attrIndex, attrTypeString, numBins, lowerEndpoint, upperEndpoint).asInstanceOf[RangeHistogram[Date]]
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            rh.histogram.put(StatHelpers.dateFormat.parseDateTime(splitKeyValuePair(0)).toDate, splitKeyValuePair(1).toLong)
        }
        rh
      case _ if attrType == classOf[Integer] =>
        val rh = RangeHistogram(attrIndex, attrTypeString, numBins, lowerEndpoint, upperEndpoint).asInstanceOf[RangeHistogram[Integer]]
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            rh.histogram.put(splitKeyValuePair(0).toInt, splitKeyValuePair(1).toLong)
        }
        rh
      case _ if attrType == classOf[java.lang.Long] =>
        val rh = RangeHistogram(attrIndex, attrTypeString, numBins, lowerEndpoint, upperEndpoint).asInstanceOf[RangeHistogram[java.lang.Long]]
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            rh.histogram.put(splitKeyValuePair(0).toLong, splitKeyValuePair(1).toLong)
        }
        rh
      case _ if attrType == classOf[java.lang.Float] =>
        val rh = RangeHistogram(attrIndex, attrTypeString, numBins, lowerEndpoint, upperEndpoint).asInstanceOf[RangeHistogram[java.lang.Float]]
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            rh.histogram.put(splitKeyValuePair(0).toFloat, splitKeyValuePair(1).toLong)
        }
        rh
      case _ if attrType == classOf[java.lang.Double] =>
        val rh = RangeHistogram(attrIndex, attrTypeString, numBins, lowerEndpoint, upperEndpoint).asInstanceOf[RangeHistogram[java.lang.Double]]
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            rh.histogram.put(splitKeyValuePair(0).toDouble, splitKeyValuePair(1).toLong)
        }
        rh
    }
  }

  /**
   * Uses individual stat pack methods to serialize the stat.
   * @param stat the given stat to serialize
   * @return
   */
  def pack(stat: Stat): Array[Byte] = {
    stat match {
      case mm: MinMax[_]                => packMinMax(mm)
      case isc: IteratorStackCounter    => packISC(isc)
      case isc: IteratorStackCounter    => packISC(isc)
      case eh: EnumeratedHistogram[_]   => packEnumeratedHistogram(eh)
      case rh: RangeHistogram[_]        => packRangeHistogram(rh)
      case seq: SeqStat                 => Bytes.concat(seq.stats.map(pack) : _*)
    }
  }

  /**
   * Deserializes the stat.
   * @param bytes the serialized stat
   * @return
   */
  def unpack(bytes: Array[Byte]): Stat = {
    val returnStats: ArrayBuffer[Stat] = new mutable.ArrayBuffer[Stat]()
    val bb = ByteBuffer.wrap(bytes)

    var bytePointer = 0
    while (bytePointer < bytes.length - 1) {
      val statType = bytes(bytePointer)
      val statSize = bb.getInt(bytePointer + 1)
      val statBytes = bytes.slice(bytePointer + 5, bytePointer + 5 + statSize)

      statType match {
        case MINMAX_BYTE =>
          val stat = unpackMinMax(statBytes)
          returnStats += stat
        case ISC_BYTE =>
          val stat = unpackIteratorStackCounter(statBytes)
          returnStats += stat
        case EH_BYTE =>
          val stat = unpackEnumeratedHistogram(statBytes)
          returnStats += stat
        case RH_BYTE =>
          val stat = unpackRangeHistogram(statBytes)
          returnStats += stat
      }

      bytePointer += statSize + 5
    }

    returnStats.size match {
      case 1 => returnStats.head
      case _ => new SeqStat(returnStats.toSeq)
    }
  }
}
