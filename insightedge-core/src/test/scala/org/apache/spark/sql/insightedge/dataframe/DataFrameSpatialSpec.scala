/*
 * Copyright (c) 2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.insightedge.dataframe

import java.sql.{Date, Timestamp}
import java.text.{ParsePosition, SimpleDateFormat}

import com.gigaspaces.document.SpaceDocument
import com.j_spaces.core.client.SQLQuery
import com.j_spaces.jdbc.QueryProcessor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.insightedge.JSpatialData
import org.apache.spark.sql.insightedge.model.{AllClassesSupport, Location, SpatialData, SpatialEmbeddedData}
import org.apache.spark.unsafe.types.CalendarInterval
import org.insightedge.spark.fixture.InsightEdge
import org.insightedge.spark.implicits.all._
import org.insightedge.spark.utils.{JavaSpaceClass, ScalaSpaceClass}
import org.openspaces.spatial.ShapeFactory._
import org.openspaces.spatial.shapes.{Circle, Point, Rectangle}
import org.scalatest.fixture

class DataFrameSpatialSpec extends fixture.FlatSpec with InsightEdge {

  it should "dataframe: find with spatial operations at xap and spark" taggedAs ScalaSpaceClass in { ie =>
    val searchedCircle = circle(point(0, 0), 1.0)
    val searchedRect = rectangle(0, 2, 0, 2)
    val searchedPoint = point(1, 1)
    ie.spaceProxy.write(SpatialData(id = null, routing = 1, searchedCircle, searchedRect, searchedPoint))

    def asserts(df: DataFrame): Unit = {
      assert(df.count() == 1)

      assert(df.filter(df("circle") geoIntersects circle(point(1, 0), 1)).count() == 1)
      assert(df.filter(df("circle") geoIntersects circle(point(3, 0), 1)).count() == 0)
      assert(df.filter(df("circle") geoWithin circle(point(1, 0), 2)).count() == 1)
      assert(df.filter(df("circle") geoWithin circle(point(1, 0), 1)).count() == 0)
      assert(df.filter(df("circle") geoContains circle(point(0, 0), 0.5)).count() == 1)
      assert(df.filter(df("circle") geoContains circle(point(1, 0), 1)).count() == 0)

      assert(df.filter(df("rect") geoIntersects rectangle(1, 3, 1, 3)).count() == 1)
      assert(df.filter(df("rect") geoIntersects rectangle(3, 5, 0, 2)).count() == 0)
      assert(df.filter(df("rect") geoWithin rectangle(-1, 3, -1, 3)).count() == 1)
      assert(df.filter(df("rect") geoWithin rectangle(1, 3, 1, 3)).count() == 0)
      assert(df.filter(df("rect") geoContains rectangle(0.5, 1.5, 0.5, 1.5)).count() == 1)
      assert(df.filter(df("rect") geoContains rectangle(1, 3, 1, 3)).count() == 0)

      assert(df.filter(df("point") geoWithin rectangle(0, 2, 0, 2)).count() == 1)
      assert(df.filter(df("point") geoWithin rectangle(2, 3, 2, 3)).count() == 0)

      // more shapes
      assert(df.filter(df("circle") geoIntersects lineString(point(0, 0), point(0, 2), point(2, 2))).count() == 1)
      assert(df.filter(df("circle") geoIntersects lineString(point(0, 2), point(2, 2), point(2, 0))).count() == 0)
      assert(df.filter(df("circle") geoWithin rectangle(-2, 2, -2, 2)).count() == 1)
      assert(df.filter(df("circle") geoWithin rectangle(0, 2, -2, 2)).count() == 0)
      assert(df.filter(df("circle") geoIntersects polygon(point(0, 0), point(0, 2), point(2, 2), point(0, 0))).count() == 1)
      assert(df.filter(df("circle") geoIntersects polygon(point(2, 2), point(-2, 2), point(2, 4), point(2, 2))).count() == 0)
      assert(df.filter(df("point") geoWithin rectangle(-2, 2, -2, 2)).count() == 1)
      assert(df.filter(df("point") geoWithin rectangle(2, 4, -2, 2)).count() == 0)
    }
    val spark = ie.spark
    // pushed down to XAP
    val df = spark.read.grid[SpatialData]
    df.printSchema()
    asserts(df)

    // executed in expressions on Spark
    val pdf = df.persist()
    asserts(pdf)
  }

  it should "dataframe: find with spatial operations at xap and spark [java]" taggedAs JavaSpaceClass in { ie =>
    ie.spaceProxy.write(new JSpatialData(1L, point(0, 0)))
    val spark = ie.spark
    // pushed down to XAP
    val df = spark.read.grid[JSpatialData]
    df.printSchema()
    zeroPointCheck(df, "point")

    // executed in expressions on Spark
    val pdf = df.persist()
    zeroPointCheck(pdf, "point")
  }

  ignore should "dataframe: support more types." taggedAs ScalaSpaceClass in { ie => // TODO: add support
    val timestamp = System.currentTimeMillis()
    val bigDecimal = new java.math.BigDecimal(42)
    val array = Array[Byte](25.toByte, 66.toByte)

    val date = Date.valueOf("2006-11-12")
    ie.spaceProxy.writeMultiple(Array(
      AllClassesSupport("first", 6, bigDecimal, array, new Timestamp(777), date)
      ,AllClassesSupport("second",6, bigDecimal, array, new Timestamp(999), date)
      ,AllClassesSupport("third",6, bigDecimal, array, new Timestamp(777), date)
      ))

    val spark = ie.spark
    val regularDataFrame = spark.read.grid[AllClassesSupport]

    regularDataFrame.write.grid("collection")
    val dataFrameFromSpace = spark.read.grid("collection")
    regularDataFrame.printSchema()
    dataFrameFromSpace.printSchema()
    println(regularDataFrame.dtypes.equals(dataFrameFromSpace.dtypes))
    println(regularDataFrame.dtypes)
    println(dataFrameFromSpace.dtypes)

    regularDataFrame.show()
    dataFrameFromSpace.show()
    dataFrameFromSpace.count()

    assert(regularDataFrame.filter(regularDataFrame("decimal") equalTo BigDecimal.valueOf(42)).count() == 2)
    assert(dataFrameFromSpace.filter(dataFrameFromSpace("decimal") equalTo bigDecimal).count() == 2)

    assert(regularDataFrame.filter(regularDataFrame("byte") equalTo array).count() == 3)
    assert(dataFrameFromSpace.filter(dataFrameFromSpace("byte") equalTo  array).count() == 3)

    assert(regularDataFrame.filter(regularDataFrame("timeStamp") equalTo new Timestamp(999)).count() == 1)
    assert(dataFrameFromSpace.filter(dataFrameFromSpace("timeStamp") equalTo new Timestamp(999)).count() == 1)

    assert(regularDataFrame.filter(regularDataFrame("date") equalTo new Date(timestamp)).count() == 0)
    assert(dataFrameFromSpace.filter(dataFrameFromSpace("date") equalTo new Date(timestamp)).count() == 0)
  }

  it should "dataframe: work with shapes embedded on second level" taggedAs ScalaSpaceClass in { ie =>
    ie.spaceProxy.write(SpatialEmbeddedData(id = null, Location(point(0, 0))))
    val spark = ie.spark
    // pushed down to XAP
    val df = spark.read.grid[SpatialEmbeddedData]
    df.printSchema()
    zeroPointCheck(df, "location.point")

    // executed in expressions on Spark
    val pdf = df.persist()
    zeroPointCheck(pdf, "location.point")
  }

  it should "dataframe: work with new columns via udf" in { ie =>
    ie.spaceProxy.write(SpatialData(id = null, routing = 1, null, null, point(1, 1)))
    val spark = ie.spark
    val df = spark.read.grid[SpatialData]
    val toPointX = udf((f: Any) => f.asInstanceOf[Point].getX)
    val unwrappedDf = df.withColumn("locationX", toPointX(df("point")))
    unwrappedDf.printSchema()
    val row = unwrappedDf.first()

    assert(row.getAs[Double]("locationX") == 1)
  }

  it should "dataframe: persist shapes as shapes" taggedAs ScalaSpaceClass in { ie =>
    ie.spaceProxy.write(SpatialData(id = null, routing = 1, circle(point(0, 0), 1.0), rectangle(0, 2, 0, 2), point(1, 1)))

    val collectionName = randomString()
    val spark = ie.spark
    spark.read.grid[SpatialData].write.grid(collectionName)

    val data = ie.spaceProxy.read(new SQLQuery[SpaceDocument](collectionName, ""))
    assert(data.getProperty[Any]("routing").isInstanceOf[Long])
    assert(data.getProperty[Any]("circle").isInstanceOf[Circle])
    assert(data.getProperty[Any]("rect").isInstanceOf[Rectangle])
    assert(data.getProperty[Any]("point").isInstanceOf[Point])
  }

  def zeroPointCheck(df: DataFrame, attribute: String) = {
    assert(df.filter(df(attribute) geoWithin rectangle(-1, 1, -1, 1)).count() == 1)
  }

}