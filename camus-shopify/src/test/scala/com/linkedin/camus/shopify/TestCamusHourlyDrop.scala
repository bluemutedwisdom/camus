package com.linkedin.camus.shopify

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.scalatest.FunSuite

class TestCamusHourlyDrop extends FunSuite {
  val camusHistoryTestFolder = "../camus-shopify/src/test/resources/camus-test-data"
  val stableDrop = camusHistoryTestFolder + "/hourly_drop_stable"
  val ladDrop = camusHistoryTestFolder + "/hourly_drop_lad"
  val unflaggeddDrop = camusHistoryTestFolder + "/hourly_drop_unflagged"
  val fs = FileSystem.get(new Configuration())

  test("flagPath") {
    val drop = new CamusHourlyDrop(new Path(stableDrop), fs)
    assert(drop.flagPath.toString.equals(stableDrop + "/_IMPORTED"))
  }

  test("hasFlag") {
    val normal = new CamusHourlyDrop(new Path(stableDrop), fs)
    val unflagged = new CamusHourlyDrop(new Path(unflaggeddDrop), fs)

    assert(normal.hasFlag)
    assert(!unflagged.hasFlag)
  }

  test("flagWrittenAt") {
    val drop = new CamusHourlyDrop(new Path(stableDrop), fs)

    assertResult(1456343218000L) { drop.flagWrittenAt }
  }

  test("lastFileWrittenAt") {
    val drop = new CamusHourlyDrop(new Path(stableDrop), fs)

    assertResult(1456343218000L) { drop.lastFileWrittenAt }
  }

  test("isFlagViolated") {
    val stable = new CamusHourlyDrop(new Path(stableDrop), fs)
    val violated = new CamusHourlyDrop(new Path(ladDrop), fs)

    assert(violated.isFlagViolated)
    assert(!stable.isFlagViolated)
  }
}
