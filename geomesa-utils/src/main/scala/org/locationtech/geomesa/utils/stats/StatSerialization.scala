/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import com.google.common.primitives.Bytes
import org.locationtech.geomesa.utils.stats.StatHelpers._
import org.apache.commons.codec.binary.Base64
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Stats are serialized as a byte array where the first byte indicates which type of stat is present.
 * The next four bits contain the size of the serialized information.
 * A SeqStat is serialized the same way, with each individual stat immediately following the previous in the byte array.
 */
object StatSerialization {
  // bytes indicating the type of stat
  val MINMAX_BYTE: Byte           = '0'
  val ISC_BYTE: Byte              = '1'
  val ENUMERATED_HISTOGRAM: Byte  = '2'
  val RANGE_HISTOGRAM: Byte       = '3'

  private def serializeStat(kind: Byte, bytes: Array[Byte]): Array[Byte] = {
    val size = ByteBuffer.allocate(4).putInt(bytes.length).array
    Bytes.concat(Array(kind), size, bytes)
  }

  protected [stats] def packMinMax(mm: MinMax[_]): Array[Byte] = {
    serializeStat(MINMAX_BYTE, s"${mm.attributeIndex};${mm.classType};${mm.min};${mm.max}".getBytes)
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
    val baos = new ByteArrayOutputStream()
    val os = new DataOutputStream(baos)
    for((key, count) <- eh.map) {
      os.writeBytes(key.toString)
      os.writeLong(count)
    }
    os.flush()
    serializeStat(ENUMERATED_HISTOGRAM, baos.toByteArray)
  }

//  protected [stats] def unpackEnumeratedHistogram(bytes: Array[Byte]): EnumeratedHistogram[_] = {
//
//  }

//  protected [stats] def packRangeHistogram(rh: RangeHistogram[T]): Array[Byte] = {

//  }

  /**
   * Uses individual stat pack methods to serialize the stat.
   * @param stat the given stat to serialize
   * @return
   */
  def pack(stat: Stat): Array[Byte] = {
    stat match {
      case mm: MinMax[_]                => packMinMax(mm)
      case isc: IteratorStackCounter    => packISC(isc)
//      case eh: EnumeratedHistogram[_]   => packEnumerationHistogram(eh)
//      case rh: RangeHistogram[_]        => packRangeHistogram(rh)
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

      statType match {
        case MINMAX_BYTE =>
          val stat = unpackMinMax(bytes.slice(bytePointer + 5, bytePointer + 5 + statSize))
          returnStats += stat
        case ISC_BYTE =>
          val stat = unpackIteratorStackCounter(bytes.slice(bytePointer + 5, bytePointer + 5 + statSize))
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
