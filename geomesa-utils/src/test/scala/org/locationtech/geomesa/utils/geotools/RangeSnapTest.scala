///***********************************************************************
//* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
//* All rights reserved. This program and the accompanying materials
//* are made available under the terms of the Apache License, Version 2.0 which
//* accompanies this distribution and is available at
//* http://www.opensource.org/licenses/apache2.0.php.
//*************************************************************************/
//
//package org.locationtech.geomesa.utils.geotools
//
//import com.typesafe.scalalogging.slf4j.Logging
//import org.joda.time.{DateTime, Duration, Interval}
//import org.junit.runner.RunWith
//import org.specs2.mutable.Specification
//import org.specs2.runner.JUnitRunner
//
//@RunWith(classOf[JUnitRunner])
//class RangeSnapTest extends Specification with Logging {
//
//  val buckets = 4
//  val lowerEndpoint: java.lang.Long = 0L
//  val upperEndpoint: java.lang.Long = 10L
//  val interval: com.google.common.collect.Range[java.lang.Long] = com.google.common.collect.Ranges.closed(lowerEndpoint, upperEndpoint)
//  val timeSnap = new RangeSnap(interval, buckets)
//
//
//  "TimeSnap" should {
//    "create a timesnap around a given time interval" in {
//      timeSnap must not beNull
//    }
//
//    "compute correct durations given number of bins wanted" in {
//      val duration = new Duration(interval.toDurationMillis / buckets)
//      timeSnap.dt.equals(duration) must beTrue
//    }
//
//    "compute correct bin index given time" in {
//      var date = new DateTime(1985, 7, 31, 0, 0)
//      var i = timeSnap.getBucket(date)
//      i must beEqualTo(1)
//
//      date = date.withYear(2011)
//      i = timeSnap.getBucket(date)
//      i must beEqualTo(buckets)
//
//      date = date.withYear(1969)
//      i = timeSnap.getBucket(date)
//      i must beEqualTo(-1)
//    }
//
//    "compute correct start time of bin given index" in {
//      timeSnap.t(0) must beEqualTo(startDate)
//      timeSnap.t(buckets) must beEqualTo(endDate)
//
//      val simpleInterval = new Interval(0, 300)
//      val simpleTimeSnap = new RangeSnap(simpleInterval, 3)
//      simpleTimeSnap.t(-1).getMillis must beEqualTo(0)
//      simpleTimeSnap.t(0).getMillis must beEqualTo(0)
//      simpleTimeSnap.t(1).getMillis must beEqualTo(100)
//      simpleTimeSnap.t(2).getMillis must beEqualTo(200)
//      simpleTimeSnap.t(3).getMillis must beEqualTo(300)
//      simpleTimeSnap.t(4).getMillis must beEqualTo(300)
//    }
//  }
//}
