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

  "StatsSerlization" should {
    "pack and unpack" >> {
      val attributeIndex = 1
      val minMax = MinMax(attributeIndex, "java.lang.Long", "-235", "12345")

      val isc = IteratorStackCounter()
      isc.count = 987654321L

//      val enumerationHistogram = EnumerationHistogram(attributeIndex, )

      val rangeHistogram = RangeHistogram(attributeIndex, "java.lang.Integer", "10", "5", "15")

      "MinMax stat" in {
        val packed   = StatSerialization.pack(minMax)
        val unpacked = StatSerialization.unpack(packed).asInstanceOf[MinMax[java.lang.Long]]

        unpacked mustEqual minMax
      }

      "IteratorStackCounter stat" in {
        val packed = StatSerialization.pack(isc)
        val unpacked = StatSerialization.unpack(packed).asInstanceOf[IteratorStackCounter]

        unpacked mustEqual isc
      }

      //TODO: Fill this in.
      "EnumeratedHistogram stat" in {
        success
      }

//      "RangeHistogram stat" in {
//        val packed = StatSerialization.pack(rangeHistogram)
//        val unpacked = StatSerialization.unpack(packed).asInstanceOf[RangeHistogram[java.lang.Integer]]
//
//        unpacked mustEqual rangeHistogram
//      }

      "Sequences of stats" in {
        val stats = new SeqStat(Seq(minMax, isc))

        val packed = StatSerialization.pack(stats)
        val unpacked = StatSerialization.unpack(packed)

        unpacked must anInstanceOf[SeqStat]

        val seqs = unpacked.asInstanceOf[SeqStat].stats
        seqs(0) mustEqual minMax
        seqs(1) mustEqual isc
//        seqs(2) mustEqual rangeHistogram
      }
    }
  }
}
