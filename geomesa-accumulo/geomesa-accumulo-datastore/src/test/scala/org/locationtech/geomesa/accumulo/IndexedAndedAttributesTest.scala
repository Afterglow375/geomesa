package org.locationtech.geomesa.accumulo

/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

import org.geotools.data.Query
import org.geotools.filter.text.cql2.CQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IndexedAndedAttributesTest extends Specification with TestWithDataStore {

  override val spec = "id:Integer:index=join,dtg:Date:index=join,*geom:Geometry:srid=4326"
  val geom = WKTUtils.read("POINT(25.0 25.0)")
  val date = "2014-01-10T12:00:00.000Z"

  val attrs1 = Array(new Integer(1), date, geom)
  val sf1 = ScalaSimpleFeatureFactory.buildFeature(sft, attrs1, "1")
  val attrs2 = Array(new Integer(2), date, geom)
  val sf2 = ScalaSimpleFeatureFactory.buildFeature(sft, attrs2, "2")
  val attrs3 = Array(new Integer(3), date, geom)
  val sf3 = ScalaSimpleFeatureFactory.buildFeature(sft, attrs3, "3")
  val attrs4 = Array(new Integer(4), date, geom)
  val sf4 = ScalaSimpleFeatureFactory.buildFeature(sft, attrs4, "4")
  val attrs5 = Array(new Integer(5), date, geom)
  val sf5 = ScalaSimpleFeatureFactory.buildFeature(sft, attrs5, "5")
  val attrs6 = Array(new Integer(6), date, geom)
  val sf6 = ScalaSimpleFeatureFactory.buildFeature(sft, attrs6, "6")

  val features = List(sf1, sf2, sf3, sf4, sf5, sf6)
  addFeatures(features)

  "ANDed queries against an indexed attribute" should {
    "return the correct number of results" in {
      val filter = CQL.toFilter("id < 3 AND id > 5 AND id > 10 AND dtg DURING 2014-01-01T11:45:00.000Z/2014-01-10T12:00:00.000Z")
      println(explain(new Query(sftName, filter)))
      fs.getFeatures(filter).size mustEqual 3

//      val filter2 = CQL.toFilter("id < 3 AND id > 5")
//      println(explain(new Query(sftName, filter2)))
//      fs.getFeatures(filter2).size mustEqual 0
//      success
    }
  }
}
