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

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}

import com.gigaspaces.document.{DocumentProperties, SpaceDocument}
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder
import com.j_spaces.core.client.SQLQuery
import org.apache.spark.sql.insightedge.model.{Address, DummyPerson, Person}
import org.apache.spark.sql.insightedge.{JAddress, JPerson}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset}
import org.insightedge.spark.fixture.InsightEdge
import org.insightedge.spark.implicits.all.{nestedClass, _}
import org.insightedge.spark.utils.{JavaSpaceClass, ScalaSpaceClass}
import org.scalatest.fixture

import scala.collection.JavaConversions._

/**
 * Each of these tests showing what's the new InsightEdgeDocumentRelation class implementation doesn't support
 * To use the previous implementation use the 'turnNestedDFFlagOn' method.
 *
 * Note that nested properties were only supported when providing the class instance
 */
class DataFrameUnSupportNestedSpec extends fixture.FlatSpec with InsightEdge {

  it should "Not support nested properties after saving POJO [Scala]" taggedAs ScalaSpaceClass in { ie =>

    ie.sc.parallelize(Seq(
      Person(id = null, name = "Paul", age = 30, address = Address(city = "Columbus", state = "OH")),
      Person(id = null, name = "Mike", age = 25, address = Address(city = "Buffalo", state = "NY")),
      Person(id = null, name = "John", age = 20, address = Address(city = "Charlotte", state = "NC")),
      Person(id = null, name = "Silvia", age = 27, address = Address(city = "Charlotte", state = "NC"))
    )).saveToGrid()

    val collectionName = randomString()
    val spark = ie.spark

    spark.read.grid[Person].write.grid(collectionName)

    val person = ie.spaceProxy.read(new SQLQuery[SpaceDocument](collectionName, ""))
    assert(person.getProperty[Any]("address").isInstanceOf[DocumentProperties])
    assert(person.getProperty[Any]("age").isInstanceOf[Integer])
    assert(person.getProperty[Any]("name").isInstanceOf[String])

    // This shouldn't work
    // We don't support nested properties which was read as Dataframe(without schema) and queried as a collection (.grid("string"))
    // We do support when providing class at spark.read.grid[] or when providing a schema.
    val dataFrameCollection = spark.read.grid(collectionName)
    assert(dataFrameCollection.count() == 4)
    intercept[AnalysisException] { dataFrameCollection.filter(dataFrameCollection("address.state") equalTo "NC").count() == 2 }
    intercept[AnalysisException] { dataFrameCollection.filter(dataFrameCollection("address.city") equalTo "Nowhere").count() == 0 }

    // This should work (provided class person on reading Dataframe)
    val dataFrameClass = spark.read.grid[Person]
    assert(dataFrameClass.count() == 4)
    assert(dataFrameClass.filter(dataFrameClass("address.state") equalTo "NC").count() == 2)
    assert(dataFrameClass.filter(dataFrameClass("address.city") equalTo "Nowhere").count() == 0)
  }

  it should "Not support nested properties after saving [java] (Use DFSchema system property is off)" taggedAs JavaSpaceClass in { ie =>

    parallelizeJavaSeq(ie.sc, () => Seq(
      new JPerson(null, "Paul", 30, new JAddress("Columbus", "OH")),
      new JPerson(null, "Mike", 25, new JAddress("Buffalo", "NY")),
      new JPerson(null, "John", 20, new JAddress("Charlotte", "NC")),
      new JPerson(null, "Silvia", 27, new JAddress("Charlotte", "NC"))
    )).saveToGrid()

    val collectionName = randomString()
    val spark = ie.spark
    spark.read.grid[JPerson].write.grid(collectionName)

    val person = ie.spaceProxy.read(new SQLQuery[SpaceDocument](collectionName, ""))
    assert(person.getProperty[Any]("address").isInstanceOf[DocumentProperties])
    assert(person.getProperty[Any]("age").isInstanceOf[Integer])
    assert(person.getProperty[Any]("name").isInstanceOf[String])

    val dataFrameCollection = spark.read.grid(collectionName)
    assert(dataFrameCollection.count() == 4)
    intercept[AnalysisException] { dataFrameCollection.filter(dataFrameCollection("address.state") equalTo "NC").count() == 2 }
    intercept[AnalysisException] { dataFrameCollection.filter(dataFrameCollection("address.city") equalTo "Nowhere").count() == 0 }
  }

  it should "Not support loading dataframe from existing space documents with provided schema" in { ie =>

    val collectionName = randomString()

    ie.spaceProxy.getTypeManager.registerTypeDescriptor(
      new SpaceTypeDescriptorBuilder(collectionName)
        .idProperty("personId", true)
        .addFixedProperty("personId", "String")
        .routingProperty("name")
        .create()
    )
    ie.spaceProxy.writeMultiple(Array(
      new SpaceDocument(collectionName, Map(
        "personId" -> "222",
        "name" -> "John", "surname" -> "Green", "age" -> Integer.valueOf(40),
        "address" -> Address("Charlotte", "NY"), "jaddress" -> new JAddress("Arad", "NY")))
        ,
      new SpaceDocument(collectionName, Map(
        "personId" -> "222",
        "name" -> "Mike", "surname" -> "Green", "age" -> Integer.valueOf(20),
        "address" -> Address("Charlotte", "NC"), "jaddress" -> new JAddress("Charlotte", "NC")))
    ))

    val dataFrameAsserts = (dataFrame: DataFrame) => {
      assert(dataFrame.count() == 2)
      assert(dataFrame.filter(dataFrame("name") equalTo "John").count() == 1)
      assert(dataFrame.filter(dataFrame("age") < 30).count() == 1)
      assert(dataFrame.filter(dataFrame("address.state") equalTo "NY").count() == 1)
      assert(dataFrame.filter(dataFrame("jaddress.city") equalTo "Charlotte").count() == 1)
    }

    val schemaAsserts = (schema: StructType) => {
      assert(schema.fieldNames.contains("name"))
      assert(schema.fieldNames.contains("surname"))
      assert(schema.fieldNames.contains("age"))
      assert(schema.fieldNames.contains("address"))
      assert(schema.fieldNames.contains("jaddress"))

      assert(schema.get(schema.getFieldIndex("name").get).dataType == StringType)
      assert(schema.get(schema.getFieldIndex("surname").get).dataType == StringType)
      assert(schema.get(schema.getFieldIndex("age").get).dataType == IntegerType)
      assert(schema.get(schema.getFieldIndex("address").get).dataType.isInstanceOf[StructType])
      assert(schema.get(schema.getFieldIndex("jaddress").get).dataType.isInstanceOf[StructType])
    }

    val addressType = StructType(Seq(
      StructField("state", StringType, nullable = true),
      StructField("city", StringType, nullable = true)
    ))
    val spark = ie.spark
    val dataFrameCollectionAndSchema = spark.read
      .schema(StructType(Seq(
        StructField("personId", StringType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("surname", StringType, nullable = true),
        StructField("age", IntegerType, nullable = false),
        StructField("address", addressType.copy(), nullable = true, nestedClass[Address]),
        StructField("jaddress", addressType.copy(), nullable = true, nestedClass[JAddress])
        )))
      .grid(collectionName)
    dataFrameCollectionAndSchema.printSchema()

    // check schema. This should work on both ways
    schemaAsserts(dataFrameCollectionAndSchema.schema)
    // check content. This should work on both ways(System property on/off)
    dataFrameAsserts(dataFrameCollectionAndSchema)

    // Prove that dataframe can't be persisted when providing collection/Document
    val tableName = randomString()
    dataFrameCollectionAndSchema.write.grid(tableName)
    try {
      dataFrameAsserts(spark.read.grid(tableName))
      fail("The expected exception hasn't been thrown")
    } catch {
      case desiredException: AnalysisException => {}
      case other: Object => { fail("The expected exception hasn't been thrown") }
    }
  }

  it should "Not support loading dataset from existing space documents with provided schema" in { ie =>
    val collectionName = randomString()
    val spark = ie.spark
    import spark.implicits._

    ie.spaceProxy.getTypeManager.registerTypeDescriptor(
      new SpaceTypeDescriptorBuilder(collectionName)
        .idProperty("personId")
        .routingProperty("name")
        .create()
    )

    ie.spaceProxy.writeMultiple(Array(
      new SpaceDocument(collectionName, Map(
        "personId" -> "111",
        "name" -> "John",
        "surname" -> "Wind",
        "age" -> Integer.valueOf(32),
        "address" -> Address("New York", "NY")
      )),
      new SpaceDocument(collectionName, Map(
        "personId" -> "222",
        "name" -> "Mike",
        "surname" -> "Green",
        "age" -> Integer.valueOf(20),
        "address" -> Address("Charlotte", "NC")
      ))
    ))

    val dataSetAsserts = (dataSet: Dataset[DummyPerson]) => {
      assert(dataSet.count() == 2)
      assert(dataSet.filter(dataSet("name") equalTo "John").count() == 1)
      assert(dataSet.filter(dataSet("age") < 30).count() == 1)
      assert(dataSet.filter(dataSet("address.state") equalTo "NY").count() == 1)

      assert(dataSet.filter( "age < 30").count() == 1)

      assert(dataSet.filter( o => o.name == "John").count() == 1)
      assert(dataSet.filter( o => o.age < 30 ).count() == 1)
      assert(dataSet.filter( o => o.address.state == "NY").count() == 1)
    }

    val schemaAsserts = (schema: StructType) => {
      assert(schema.fieldNames.contains("name"))
      assert(schema.fieldNames.contains("surname"))
      assert(schema.fieldNames.contains("age"))
      assert(schema.fieldNames.contains("address"))

      assert(schema.get(schema.getFieldIndex("name").get).dataType == StringType)
      assert(schema.get(schema.getFieldIndex("surname").get).dataType == StringType)
      assert(schema.get(schema.getFieldIndex("age").get).dataType == IntegerType)
      assert(schema.get(schema.getFieldIndex("address").get).dataType.isInstanceOf[StructType])
    }

    val addressType = StructType(Seq(
      StructField("state", StringType, nullable = true),
      StructField("city", StringType, nullable = true)
    ))

    val ds = spark.read.schema(
      StructType(Seq(
        StructField("personId", StringType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("surname", StringType, nullable = true),
        StructField("age", IntegerType, nullable = false),
        StructField("address", addressType.copy(), nullable = true, nestedClass[Address])
      ))
    ).grid(collectionName).as[DummyPerson]

    ds.printSchema()

    // check schema
    schemaAsserts(ds.schema)
    // check content
    dataSetAsserts(ds)

    // check if dataframe can be persisted
    val tableName = randomString()
    ds.write.grid(tableName)
    try {
      dataSetAsserts(spark.read.grid(tableName).as[DummyPerson])
      fail("The expected exception hasn't been thrown")
    } catch {
      case desiredException: AnalysisException => {}
      case other: Object => { fail("The expected exception hasn't been thrown") }
    }
  }

  it should "Explain how to support loading dataframe from existing space documents with provided schema" in { ie =>

    val collectionName = randomString()
    ie.spaceProxy.getTypeManager.registerTypeDescriptor(
      new SpaceTypeDescriptorBuilder(collectionName)
        .idProperty("personId", true)
        .addFixedProperty("personId", "String")
        .routingProperty("name")
        .create()
    )
    ie.spaceProxy.writeMultiple(Array(
      new SpaceDocument(collectionName, Map(
        "personId" -> "222",
        "name" -> "John", "surname" -> "Green", "age" -> Integer.valueOf(40),
        "address" -> new DocumentProperties().setProperty("state", "CE").setProperty("city", "Charlotte"),
        "jaddress" -> new DocumentProperties().setProperty("state", "BY").setProperty("city", "arad")
      ))
      ,
      new SpaceDocument(collectionName, Map(
        "personId" -> "222",
        "name" -> "Mike", "surname" -> "Green", "age" -> Integer.valueOf(20),
        "address" -> new DocumentProperties().setProperty("state", "NY").setProperty("city", "Charlotte"),
        "jaddress" -> new DocumentProperties().setProperty("state", "BY").setProperty("city", "Charlotte")
      ))))

    val dataFrameAsserts = (dataFrame: DataFrame) => {
      assert(dataFrame.count() == 2)
      assert(dataFrame.filter(dataFrame("name") equalTo "John").count() == 1)
      assert(dataFrame.filter(dataFrame("age") < 30).count() == 1)
      assert(dataFrame.filter(dataFrame("address.state") equalTo "NY").count() == 1)
      assert(dataFrame.filter(dataFrame("jaddress.city") equalTo "Charlotte").count() == 1)
    }

    val schemaAsserts = (schema: StructType) => {
      assert(schema.fieldNames.contains("name"))
      assert(schema.fieldNames.contains("surname"))
      assert(schema.fieldNames.contains("age"))
      assert(schema.fieldNames.contains("address"))
      assert(schema.fieldNames.contains("jaddress"))

      assert(schema.get(schema.getFieldIndex("name").get).dataType == StringType)
      assert(schema.get(schema.getFieldIndex("surname").get).dataType == StringType)
      assert(schema.get(schema.getFieldIndex("age").get).dataType == IntegerType)
      assert(schema.get(schema.getFieldIndex("address").get).dataType.isInstanceOf[StructType])
      assert(schema.get(schema.getFieldIndex("jaddress").get).dataType.isInstanceOf[StructType])
    }

    val addressType = StructType(Seq(
      StructField("city", StringType, nullable = true),
      StructField("state", StringType, nullable = true)
    ))
    val spark = ie.spark
    val dataFrameCollectionAndSchema = spark.read
      .schema(StructType(Seq(
        StructField("personId", StringType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("surname", StringType, nullable = true),
        StructField("age", IntegerType, nullable = false),
        StructField("address", addressType, nullable = true),
        StructField("jaddress", addressType, nullable = true)
      )))
      .grid(collectionName)
    val tableName = randomString()
    dataFrameCollectionAndSchema.write.grid(tableName)
    dataFrameCollectionAndSchema.printSchema()

    // check schema. This should work on both ways
    schemaAsserts(dataFrameCollectionAndSchema.schema)
    // check content. This should work on both ways(System property on/off)
    dataFrameAsserts(dataFrameCollectionAndSchema)

    // this is what's different from previous tests (the schema).
    val dfWithSchema = spark.read
      .schema(StructType(Seq(
        StructField("personId", StringType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("surname", StringType, nullable = true),
        StructField("age", IntegerType, nullable = false),
        StructField("address", addressType, nullable = true),
        StructField("jaddress", addressType, nullable = true)
      )))
      .grid(tableName)

    dataFrameCollectionAndSchema.printSchema()
    dfWithSchema.printSchema()
    dfWithSchema.show()

    dataFrameAsserts(dfWithSchema)
  }

  it should "Explain how to support nested properties after saving [java] (provide Schema)" taggedAs JavaSpaceClass in { ie =>

    parallelizeJavaSeq(ie.sc, () => Seq(
      new JPerson(null, "Paul", 30, new JAddress("Columbus", "OH")),
      new JPerson(null, "Mike", 25, new JAddress("Buffalo", "NY")),
      new JPerson(null, "John", 20, new JAddress("Charlotte", "NC")),
      new JPerson(null, "Silvia", 27, new JAddress("Charlotte", "NC"))
    )).saveToGrid()

    val collectionName = randomString()
    val spark = ie.spark
    spark.read.grid[JPerson].write.grid(collectionName)

    val person = ie.spaceProxy.read(new SQLQuery[SpaceDocument](collectionName, ""))
    assert(person.getProperty[Any]("address").isInstanceOf[DocumentProperties])
    assert(person.getProperty[Any]("age").isInstanceOf[Integer])
    assert(person.getProperty[Any]("name").isInstanceOf[String])

    val addressType = StructType(Seq(
      StructField("city", StringType, nullable = true),
      StructField("state", StringType, nullable = true)
    ))

    val dataFrameCollection = spark.read
      .schema(
        StructType(Seq(
          StructField("id", StringType, nullable = false),
          StructField("name", StringType, nullable = true),
          StructField("age", IntegerType, nullable = false),
          StructField("address", addressType.copy(), nullable = true)
        ))
      )
      .grid(collectionName)

    assert(dataFrameCollection.count() == 4)
    assert(dataFrameCollection.filter(dataFrameCollection("address.state") equalTo "NC").count() == 2)
    assert(dataFrameCollection.filter(dataFrameCollection("address.city") equalTo "Nowhere").count() == 0)
  }

  it should "Explain how to support nested properties after saving [Scala] (provide Schema)" taggedAs ScalaSpaceClass in { ie =>

    ie.sc.parallelize(Seq(
      Person(id = null, name = "Paul", age = 30, address = Address(city = "Columbus", state = "OH")),
      Person(id = null, name = "Mike", age = 25, address = Address(city = "Buffalo", state = "NY")),
      Person(id = null, name = "John", age = 20, address = Address(city = "Charlotte", state = "NC")),
      Person(id = null, name = "Silvia", age = 27, address = Address(city = "Charlotte", state = "NC"))
    )).saveToGrid()

    val collectionName = randomString()
    val spark = ie.spark

    spark.read.grid[Person].write.grid(collectionName)

    val person = ie.spaceProxy.read(new SQLQuery[SpaceDocument](collectionName, ""))
    assert(person.getProperty[Any]("address").isInstanceOf[DocumentProperties])
    assert(person.getProperty[Any]("age").isInstanceOf[Integer])
    assert(person.getProperty[Any]("name").isInstanceOf[String])

    val addressType = StructType(Seq(
      StructField("city", StringType, nullable = true),
      StructField("state", StringType, nullable = true)
    ))

    val dataFrameClass = spark.read
      .schema(
        StructType(Seq(
          StructField("id", StringType, nullable = false),
          StructField("name", StringType, nullable = true),
          StructField("age", IntegerType, nullable = false),
          StructField("address", addressType.copy(), nullable = true)
        ))
      )
      .grid[Person]
    assert(dataFrameClass.count() == 4)
    assert(dataFrameClass.filter(dataFrameClass("address.state") equalTo "NC").count() == 2)
    assert(dataFrameClass.filter(dataFrameClass("address.city") equalTo "Nowhere").count() == 0)
  }

  it should "Support jdbc querying but without nested properties" in { ie =>

      ie.sc.parallelize(Seq(
        Person(id = null, name = "Paul", age = 30, address = Address(city = "Columbus", state = "OH")),
        Person(id = null, name = "Mike", age = 25, address = Address(city = "Buffalo", state = "NY")),
        Person(id = null, name = "John", age = 20, address = Address(city = "Charlotte", state = "NC")),
        Person(id = null, name = "Silvia", age = 27, address = Address(city = "Charlotte", state = "NC"))
      )).saveToGrid()
      Class.forName("com.j_spaces.jdbc.driver.GDriver")

      val collectionName = randomString()
      val spark = ie.spark

      // Instead of person you can provide a schema
      spark.read.grid[Person].write.grid(collectionName)
      val dataFrame = spark.read.grid(collectionName)

      val space = ie.spaceProxy.getSpace
      val connection = DriverManager.getConnection("jdbc:gigaspaces:url:" + space.getFinderURL.getURL)
      val preparedStatement = connection.prepareStatement("select * from " + collectionName + " where name='Mike'")
      preparedStatement.execute
      val resultSet = preparedStatement.getResultSet
      resultSet.next

      assert(dataFrame.filter(dataFrame("name") equalTo "Mike").count() == 1)
      assert(resultSet.getString("name").equals("Mike"))
      assert(resultSet.getInt("age") == 25)
      assert(resultSet.getString("address").equals("DocumentProperties {city=Buffalo,state=NY}"))

      // Nested properties are not allowed, only their toString of their super type
      intercept[SQLException] { resultSet.getString("address.city") }
    }
}
