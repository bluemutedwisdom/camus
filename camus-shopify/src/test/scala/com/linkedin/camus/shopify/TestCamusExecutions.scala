package com.linkedin.camus.shopify

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest.FunSuite


class TestCamusExecutions extends FunSuite {
  val camusHistoryTestFolder = "../camus-shopify/src/test/resources/camus-test-data"
  val camusDestinationPath = "../camus-shopify/src/test/resources/camus-test-data/topics"
  val fs = FileSystem.get(new Configuration())
  val props = new Properties()
  props.setProperty("etl.execution.history.path", camusHistoryTestFolder)
  props.setProperty("etl.destination.path", camusDestinationPath)

  test("topicsAndHoursInWindow") {
    val executions = new CamusExecutions(props, fs)
    val window = 1000 * 60 * 60 * 2 // 2 hours

    val expectedhours =  Map(
      "webrequest_text" -> Vector((2015,10,2,6),(2015,10,2,7)),
      "webrequest_mobile" -> Vector((2015,10,2,6), (2015,10,2,7)),
      "webrequest_upload" -> Vector((2015,10,2,6), (2015,10,2,7)),
      "webrequest_misc" -> Vector((2015,10,2,6), (2015,10,2,7)),
      "webrequest_maps" -> Vector((2015,10,2,5), (2015,10,2,6))
    )

    assertResult(expectedhours) { executions.topicsAndHoursInWindow(window) }
  }

  test("droppedFoldersInWindow") {
    val executions = new CamusExecutions(props, fs)
    val window = 1000 * 60 * 60 * 2 // 2 hours

    val expectedFolder = List(
      camusDestinationPath + "/webrequest.text/2015/10/02/06",
      camusDestinationPath + "/webrequest.text/2015/10/02/07"
    )

    assertResult(expectedFolder) { executions.droppedFoldersInWindow(window) }
  }
}
