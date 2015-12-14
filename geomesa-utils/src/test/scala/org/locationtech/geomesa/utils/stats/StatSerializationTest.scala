/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatSerializationTest extends Specification {

  sequential

  "StatSerialization" should {
    "pack and unpack" in {
      val attributeIndex = 1

      val minMaxLong = new MinMax[java.lang.Long](attributeIndex, "java.lang.Long", -235L, 12345L)

      val isc = IteratorStackCounter()
      isc.count = 987654321L

      val ehDouble = new EnumeratedHistogram[java.lang.Double](attributeIndex, "java.lang.Double")
      ehDouble.frequencyMap(-1.0) += 3
      ehDouble.frequencyMap(0.5) += 5
      ehDouble.frequencyMap(1.0) += 7

      val rhInteger = new RangeHistogram[java.lang.Integer](attributeIndex, "java.lang.Integer", 10, -5, 10)
      rhInteger.histogram(-1) += 3
      rhInteger.histogram(0) += 5
      rhInteger.histogram(1) += 7

      "MinMax stat" in {
        val packed   = StatSerialization.pack(minMaxLong)
        val unpacked = StatSerialization.unpack(packed).asInstanceOf[MinMax[java.lang.Long]]

        unpacked mustEqual minMaxLong
      }

      "IteratorStackCounter stat" in {
        val packed = StatSerialization.pack(isc)
        val unpacked = StatSerialization.unpack(packed).asInstanceOf[IteratorStackCounter]

        unpacked mustEqual isc
      }

      "EnumeratedHistogram stat" in {
        val packed = StatSerialization.pack(ehDouble)
        val unpacked = StatSerialization.unpack(packed).asInstanceOf[EnumeratedHistogram[java.lang.Double]]

        unpacked.frequencyMap.size mustEqual ehDouble.frequencyMap.size
        unpacked.frequencyMap(-1.0) mustEqual ehDouble.frequencyMap(-1.0)
        unpacked.frequencyMap(0.5) mustEqual ehDouble.frequencyMap(0.5)
        unpacked.frequencyMap(1.0) mustEqual ehDouble.frequencyMap(1.0)
        unpacked mustEqual ehDouble
      }

      "RangeHistogram stat" in {
        val packed = StatSerialization.pack(rhInteger)
        val unpacked = StatSerialization.unpack(packed).asInstanceOf[RangeHistogram[java.lang.Integer]]

        unpacked.histogram.size mustEqual rhInteger.histogram.size
        unpacked.histogram(-1) mustEqual rhInteger.histogram(-1)
        unpacked.histogram(0) mustEqual rhInteger.histogram(0)
        unpacked.histogram(1) mustEqual rhInteger.histogram(1)
        unpacked.histogram(2) mustEqual rhInteger.histogram(2)
        unpacked mustEqual rhInteger
      }

      "Sequences of stats" in {
        val stats = new SeqStat(Seq(minMaxLong, isc, ehDouble, rhInteger))

        val packed = StatSerialization.pack(stats)
        val unpacked = StatSerialization.unpack(packed)

        unpacked must anInstanceOf[SeqStat]

        val seqs = unpacked.asInstanceOf[SeqStat].stats
        seqs.size mustEqual 4
        seqs(0) mustEqual minMaxLong
        seqs(1) mustEqual isc
        seqs(2) mustEqual ehDouble
        seqs(3) mustEqual rhInteger
      }
    }
  }
}
