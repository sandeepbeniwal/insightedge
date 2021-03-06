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

package org.apache.spark.sql.insightedge.dataset

import org.apache.spark.sql.SaveMode
import org.insightedge.spark.fixture.InsightEdge
import org.insightedge.spark.implicits.all._
import org.insightedge.spark.rdd.{Data, JData}
import org.insightedge.spark.utils.{JavaSpaceClass, ScalaSpaceClass}
import org.scalatest.fixture


class DataSetPersistSpec extends fixture.FlatSpec with InsightEdge {

  it should "persist with simplified syntax" taggedAs ScalaSpaceClass in { f =>
    writeDataSeqToDataGrid(1000)
    val table = randomString()
    val spark = f.spark
    import spark.implicits._
    val ds = spark.read.grid[Data].as[Data]
    ds.filter(o => o.routing > 500).write.mode(SaveMode.Overwrite).grid(table)

    val readDf = spark.read.grid(table)
    val count = readDf.select("routing").count()
    assert(count == 500)

    readDf.printSchema()
  }

  it should "persist with simplified syntax [java]" taggedAs JavaSpaceClass in { ie=>
    writeJDataSeqToDataGrid(1000)
    val table = randomString()
    val spark = ie.spark
    implicit val jDataEncoder = org.apache.spark.sql.Encoders.bean(classOf[JData])
    val ds = spark.read.grid[JData].as[JData]
    ds.filter( o => o.getRouting > 500).write.mode(SaveMode.Overwrite).grid(table)

    val readDf = spark.read.grid(table)
    val count = readDf.select("routing").count()
    assert(count == 500)

    readDf.printSchema()
  }

  it should "persist without implicits" taggedAs ScalaSpaceClass in { ie=>
    writeDataSeqToDataGrid(1000)
    val table = randomString()

    val spark = ie.spark
    import spark.implicits._
    val ds = spark.read
      .format("org.apache.spark.sql.insightedge")
      .option("class", classOf[Data].getName)
      .load()
      .as[Data]
    ds.filter(ds("routing") > 500)
      .write
      .mode(SaveMode.Overwrite)
      .format("org.apache.spark.sql.insightedge")
      .save(table)

    val readDf = spark.read.grid(table)
    val count = readDf.select("routing").count()
    assert(count == 500)

    readDf.printSchema()
  }

  it should "persist without implicits [java]" taggedAs ScalaSpaceClass in { ie=>
    writeJDataSeqToDataGrid(1000)
    val table = randomString()

    val spark = ie.spark
    implicit val jDataEncoder = org.apache.spark.sql.Encoders.bean(classOf[JData])

    val ds = spark.read
      .format("org.apache.spark.sql.insightedge")
      .option("class", classOf[JData].getName)
      .load()
      .as[JData]
    ds.filter(ds("routing") > 500)
      .write
      .mode(SaveMode.Overwrite)
      .format("org.apache.spark.sql.insightedge")
      .save(table)

    val readDf = spark.read.grid(table)
    val count = readDf.select("routing").count()
    assert(count == 500)

    readDf.printSchema()
  }

  it should "fail to persist with ErrorIfExists mode" taggedAs ScalaSpaceClass in { ie=>
    writeDataSeqToDataGrid(1000)
    val table = randomString()
    val spark = ie.spark
    import spark.implicits._
    val ds = spark.read.grid[Data].as[Data]
    ds.filter(ds("routing") > 500).write.mode(SaveMode.ErrorIfExists).grid(table)

    val thrown = intercept[IllegalStateException] {
      ds.filter(ds("routing") < 500).write.mode(SaveMode.ErrorIfExists).grid(table)
    }
    println(thrown.getMessage)
  }

  it should "clear before write with Overwrite mode" taggedAs ScalaSpaceClass in { ie=>
    writeDataSeqToDataGrid(1000)
    val table = randomString()
    val spark = ie.spark
    import spark.implicits._
    val ds = spark.read.grid[Data].as[Data]
    ds.filter(o => o.routing > 500).write.mode(SaveMode.Append).grid(table)
    assert(spark.read.grid(table).count() == 500)

    ds.filter(o => o.routing <= 200).write.mode(SaveMode.Overwrite).grid(table)
    assert(spark.read.grid(table).count() == 200)
  }

  it should "not write with Ignore mode" taggedAs ScalaSpaceClass in { ie=>
    writeDataSeqToDataGrid(1000)
    val table = randomString()
    val spark = ie.spark
    import spark.implicits._
    val ds = spark.read.grid[Data].as[Data]
    ds.filter( o => o.routing > 500).write.mode(SaveMode.Append).grid(table)
    assert(spark.read.grid(table).count() == 500)

    ds.filter( o => o.routing <= 200).write.mode(SaveMode.Ignore).grid(table)
    assert(spark.read.grid(table).count() == 500)
  }

  it should "override document schema" taggedAs ScalaSpaceClass in { ie=>
    writeDataSeqToDataGrid(1000)
    val table = randomString()
    val spark = ie.spark
    import spark.implicits._
    val ds = spark.read.grid[Data].as[Data]
    // persist with modified schema
    ds.select("id", "data").write.grid(table)
    // persist with original schema
    ds.write.mode(SaveMode.Overwrite).grid(table)
    // persist with modified schema again
    ds.select("id").write.mode(SaveMode.Overwrite).grid(table)
  }
}