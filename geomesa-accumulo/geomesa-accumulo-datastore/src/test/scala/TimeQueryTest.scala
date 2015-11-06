/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class TimeQueryTest extends Specification with TestWithDataStore {

  sequential

  override val spec = "id:String,startTime:Date:index=join,endTime:Date,*geom:Geometry:srid=4326"
  override def dtgField: String = "startTime"

  val geom = WKTUtils.read("POINT(45.0 49.0)")
  val startDate = "2014-01-12T12:00:00.000Z"
  val endDate = "2014-01-15T12:00:00.000Z"

  val attrs1 = Array("1", "2014-01-10T12:00:00.000Z", "2014-01-11T12:00:00.000Z", geom)
  val sf1 = ScalaSimpleFeatureFactory.buildFeature(sft, attrs1, "1")
  val attrs2 = Array("2", "2014-01-11T12:00:00.000Z", "2014-01-13T12:00:00.000Z", geom)
  val sf2 = ScalaSimpleFeatureFactory.buildFeature(sft, attrs2, "2")
  val attrs3 = Array("3", "2014-01-11T12:00:00.000Z", "2014-01-16T12:00:00.000Z", geom)
  val sf3 = ScalaSimpleFeatureFactory.buildFeature(sft, attrs3, "3")
  val attrs4 = Array("4", "2014-01-13T12:00:00.000Z", "2014-01-14T12:00:00.000Z", geom)
  val sf4 = ScalaSimpleFeatureFactory.buildFeature(sft, attrs4, "4")
  val attrs5 = Array("5", "2014-01-14T12:00:00.000Z", "2014-01-16T12:00:00.000Z", geom)
  val sf5 = ScalaSimpleFeatureFactory.buildFeature(sft, attrs5, "5")
  val attrs6 = Array("6", "2014-01-16T12:00:00.000Z", "2014-01-17T12:00:00.000Z", geom)
  val sf6 = ScalaSimpleFeatureFactory.buildFeature(sft, attrs6, "6")

  val features = List(sf1, sf2, sf3, sf4, sf5, sf6)

  val ff = CommonFactoryFinder.getFilterFactory2

  val temporalFields = Seq("startTime", "endTime")
  val temporalPreds  = Seq("after", "before")
  val temporalValues = Seq(startDate, endDate)

  val temporalFilterStrings =
    for {
      f <- temporalFields
      p <- temporalPreds
      v <- temporalValues
    } yield { s"$f $p $v" }

  val baseFilters: Seq[Filter] = temporalFilterStrings.map(ECQL.toFilter)
  val andFilters: Seq[Filter] = baseFilters.combinations(2).map(ff.and(_)).toSeq

  val fc: SimpleFeatureCollection = new ListFeatureCollection(sft, features)
  addFeatures(features)

  "temporal queries" should {
    "return the same from a feature collection and from GeoMesa with simple predicates" in {
      println(s"baseFilters size: ${baseFilters.size}")
      forall(baseFilters)(checkFilter)
    }

    "return the same from a feature collection and from GeoMesa with combinations of anded predicates" in {
      println(s"andFilters size: ${andFilters.size}")
      forall(andFilters)(checkFilter)
    }
  }

  def checkFilter(filter: Filter) = {
    val gmCount = fs.getFeatures(filter).size
    val fcCount = fc.subCollection(filter).size()

    println(s"Filter is ${ECQL.toCQL(filter)}, count from geomesa is $gmCount, count from feature collection is $fcCount")

    gmCount mustEqual fcCount
  }
}
