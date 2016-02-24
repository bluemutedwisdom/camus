package com.linkedin.camus.shopify

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.scalatest.FunSuite


class TestLateArrivingDataMonitor extends FunSuite {
  val fs = FileSystem.get(new Configuration())
  val window = 1000 * 60 * 60 * 2 // 2 hours
  val camusHistoryTestFolder = "../camus-shopify/src/test/resources/camus-test-data"
  val camusDestinationPath = camusHistoryTestFolder + "/topics"
  val topicsDrop = camusDestinationPath + "/webrequest_text/2015/10/02/06"
  val props = new Properties()
  props.setProperty("etl.execution.history.path", camusHistoryTestFolder)
  props.setProperty("etl.destination.path", camusDestinationPath)
  val flag = new Path(topicsDrop + "/_IMPORTED")
  val data = new Path(topicsDrop + "/data-part-0")

  test("checkDropsInWindow no late-arriving data") {
    fs.setTimes(data, 1000000000000L, 0)
    fs.setTimes(flag, 1300000000000L, 1)

    val violations = LateArrivingDataMonitor.checkDropsInWindow(props, fs, window)
    assert(!violations)
  }

  test("checkDropsInWindow with late-arriving data") {
    fs.setTimes(flag, 1000000000000L, 0)
    fs.setTimes(data, 1300000000000L, 0)

    val violations = LateArrivingDataMonitor.checkDropsInWindow(props, fs, window)
    assert(violations)
  }

}


