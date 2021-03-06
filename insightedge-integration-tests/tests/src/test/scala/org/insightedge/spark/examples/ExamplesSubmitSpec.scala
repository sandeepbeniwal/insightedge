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

package org.insightedge.spark.examples

import org.insightedge.spark.fixture.InsightedgeDemoModeDocker
import org.insightedge.spark.utils.DockerUtils.dockerExec
import org.insightedge.spark.utils.TestUtils.printLnWithTimestamp
import org.scalatest.FlatSpec

/**
  * Verifies that we can submit InsightEdge examples locally (demo mode + submit within docker container)
  *
  * @author Oleksiy_Dyagilev
  */
class ExamplesSubmitSpec extends FlatSpec with InsightedgeDemoModeDocker {

  "insightedge-submit " should "submit examples from insightedge-examples.jar" in {
    val exampleClassNames = Seq(
      "basic.SaveRdd",
      "basic.SaveRddNewContextInitApi",
      "basic.SaveRddNewSessionInitApi",
      "basic.LoadRdd",
      "basic.LoadRddWithSql",
      "basic.PersistDataFrame",
      "basic.PersistDataFrame",
      "mllib.SaveAndLoadMLModel"
    )

    exampleClassNames.foreach { className =>
      val fullClassName = s"org.insightedge.examples.$className"
      val command =
        s"""/opt/gigaspaces-insightedge/insightedge/bin/insightedge-submit
          |--class $fullClassName
          |--master spark://127.0.0.1:7077
          |/opt/gigaspaces-insightedge/insightedge/examples/jars/insightedge-examples.jar""".stripMargin


      printLnWithTimestamp( "command:" + command )
      val exitCode = dockerExec(containerId, command)
      printLnWithTimestamp( "exitCode:" + exitCode )
      assert(exitCode == 0)
    }
  }

  "insightedge-submit " should "fail with wrong space name" in {
    val spaceName = "non-existing-space"
    val command =
      s"""/opt/gigaspaces-insightedge/insightedge/bin/insightedge-submit
          |--class org.insightedge.examples.basic.SaveRdd
          |--master spark://127.0.0.1:7077
          |/opt/gigaspaces-insightedge/insightedge/examples/jars/insightedge-examples.jar
          |spark://127.0.0.1:7077
          |$spaceName""".stripMargin

    printLnWithTimestamp( "command:" + command )
    val exitCode = dockerExec(containerId, command)
    printLnWithTimestamp( "exitCode:" + exitCode )
    assert(exitCode != 0)
  }

  "insightedge-submit " should "submit sf_salaries.py python example" in {
    val command =
      s"""/opt/gigaspaces-insightedge/insightedge/bin/insightedge-submit
          |--master spark://127.0.0.1:7077
          |/opt/gigaspaces-insightedge/insightedge/examples/python/sf_salaries.py""".stripMargin

    printLnWithTimestamp( "command:" + command )
    val exitCode = dockerExec(containerId, command)
    printLnWithTimestamp( "exitCode:" + exitCode )
    assert(exitCode == 0)
  }
}