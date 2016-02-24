package com.linkedin.camus.shopify

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.scalatest.FunSuite

class TestCamusHourlyDrop extends FunSuite {
  val camusHistoryTestFolder = "../camus-shopify/src/test/resources/camus-test-data"
  val topicsDrop = camusHistoryTestFolder + "/topics/webrequest_text/2015/10/02/06"
  val unflaggedDrop = camusHistoryTestFolder + "/topics/webrequest_text/2015/10/02/07"
  val unflaggeddDrop = camusHistoryTestFolder + "/hourly_drop_unflagged"
  val flagPath = new Path(topicsDrop + "/_IMPORTED")
  val dataPath = new Path(topicsDrop + "/data-part-0")
  val fs = FileSystem.get(new Configuration())

  test("flagPath") {
    val drop = new CamusHourlyDrop(new Path(topicsDrop), fs)
    assert(drop.flagPath.toString.equals(topicsDrop + "/_IMPORTED"))
  }

  test("hasFlag") {
    val normal = new CamusHourlyDrop(new Path(topicsDrop), fs)
    val unflagged = new CamusHourlyDrop(new Path(unflaggeddDrop), fs)

    assert(normal.hasFlag)
    assert(!unflagged.hasFlag)
  }

  test("flagWrittenAt") {
    fs.setTimes(flagPath, 1000000000000L, 0)
    val drop = new CamusHourlyDrop(new Path(topicsDrop), fs)

    assertResult(1000000000000L) { drop.flagWrittenAt }
  }

  test("lastFileWrittenAt") {
    fs.setTimes(dataPath, 1000000000000L, 0)
    val drop = new CamusHourlyDrop(new Path(topicsDrop), fs)

    assertResult(1000000000000L) { drop.lastFileWrittenAt }
  }

  test("isFlagViolated") {
    val drop = new CamusHourlyDrop(new Path(topicsDrop), fs)

    fs.setTimes(dataPath, 1000000000000L, 0)
    fs.setTimes(flagPath, 1300000000000L, 0)
    assert(!drop.isFlagViolated)

    fs.setTimes(dataPath, 1400000000000L, 0)
    assert(drop.isFlagViolated)
  }
}
