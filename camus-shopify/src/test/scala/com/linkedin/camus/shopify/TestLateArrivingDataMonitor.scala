package com.linkedin.camus.shopify

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest.FunSuite


class TestLateArrivingDataMonitor extends FunSuite {
  val fs = FileSystem.get(new Configuration())
  val window = 1000 * 60 * 60 * 2 // 2 hours
  val camusHistoryTestFolder = "../camus-shopify/src/test/resources/camus-test-data"

  test("checkDropsInWindow no late-arriving data") {
    val props = new Properties()
    val camusDestinationPath =
    props.setProperty("etl.execution.history.path", camusHistoryTestFolder)
    props.setProperty("etl.destination.path", "../camus-shopify/src/test/resources/camus-test-data/no_lad")

    val violations = LateArrivingDataMonitor.checkDropsInWindow(props, fs, window)
    assert(!violations)
  }

  test("checkDropsInWindow with late-arriving data") {
    val props = new Properties()
    props.setProperty("etl.execution.history.path", camusHistoryTestFolder)
    props.setProperty("etl.destination.path", "../camus-shopify/src/test/resources/camus-test-data/with_lad")

    val violations = LateArrivingDataMonitor.checkDropsInWindow(props, fs, window)
    assert(violations)
  }

}


