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

package org.apache.spark.sql.insightedge

import org.apache.spark.sql.types.{Metadata, MetadataBuilder}
import org.apache.spark.sql._
import org.insightedge.internal.utils.SparkSessionUtils
import org.insightedge.spark.implicits.basic._

import scala.reflect._

/**
  * @author Danylo_Hurin.
  */
trait DataFrameImplicits {

  val InsightEdgeFormat = "org.apache.spark.sql.insightedge"

  def nestedClass[R: ClassTag]: Metadata = {
    nestedClassName(classTag[R].runtimeClass.getName)
  }

  def nestedClassName(clazz: String): Metadata = {
    new MetadataBuilder().putString("class", clazz).build()
  }

  implicit class DataFrameReaderWrapper(val reader: DataFrameReader) {

    def grid(collection: String): DataFrame = {
      reader.format(InsightEdgeFormat).load(collection)
    }

    def grid[R: ClassTag]: DataFrame = {
      reader.format(InsightEdgeFormat).option("class", classTag[R].runtimeClass.getName).load()
    }

    def analyticsXtreme(dbtable: String): DataFrame = {
      getDefaultSpaceName() match {
        case Some(spaceName) => analyticsXtreme(dbtable, spaceName)
        case None => throw new IllegalStateException("No active session, or no space name in active session")
      }
    }

    def analyticsXtreme(dbtable: String, spaceName: String): DataFrame = {
      reader.format("jdbc")
        .option("driver", "com.gigaspaces.jdbc.Driver")
        .option("url", s"jdbc:insightedge:spaceName=$spaceName;analyticsXtreme.enabled=true;autoCommit=true")
        .option("dbtable", dbtable)
        .load
    }

    private def getDefaultSpaceName(): Option[String] = {
      SparkSessionUtils.getDefaultSparkSession match {
        case Some(session) => session.getInsightEdgeContext().map(_.ieConfig).map(_.spaceName)
        case None => None
      }
    }
  }

  implicit class DataFrameWriterWrapper(val writer: DataFrameWriter[_]) {

    def grid(collection: String) = {
      writer.format(InsightEdgeFormat).save(collection)
    }
  }

}
