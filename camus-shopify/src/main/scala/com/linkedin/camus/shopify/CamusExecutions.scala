package com.linkedin.camus.shopify

import java.util.Properties

import com.linkedin.camus.etl.kafka.CamusJob
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.wikimedia.analytics.refinery.job.{CamusPartitionChecker, CamusStatusReader}

import scala.collection.mutable.ListBuffer

class CamusExecutions(properties: Properties, fs: FileSystem) {
  val historyFolder = properties.getProperty(CamusJob.ETL_EXECUTION_HISTORY_PATH)
  val camusReader = new CamusStatusReader(fs)
  val camusRunPath = camusReader.mostRecentRun(new Path(historyFolder))

  def topicsAndHoursInWindow(window: Long): Map[String, Seq[(Int, Int, Int, Int)]] = {
    val currentOffsets = camusReader.readEtlKeys(camusReader.offsetsFiles(camusRunPath))
    val currentTopicsAndTimes = camusReader.topicsAndOldestTimes(currentOffsets)
    val checkWindowTopicsAndTimes = currentTopicsAndTimes.mapValues((currentTime) => currentTime - window)
    checkWindowTopicsAndTimes.foldLeft(Map.empty[String, Seq[(Int, Int, Int, Int)]])(
      (map, checkTopicAndTime) => {
        var returnMap = map
        val (checkTopic, checkTime) = checkTopicAndTime
        val hours = CamusPartitionChecker.finishedHoursInBetween(checkTime, currentTopicsAndTimes.get(checkTopic).get)
        map + (checkTopic -> hours)
      }
    )
  }

  def droppedFoldersInWindow(window: Long): Seq[String] = {
    var list = new ListBuffer[String]
    for ((topic, hours) <- topicsAndHoursInWindow(window)) {
      for ((year, month, day, hour) <- hours) {
        val dir = CamusPartitionChecker.partitionDirectory(
          properties.getProperty(EtlMultiOutputFormat.ETL_DESTINATION_PATH), topic, year, month, day, hour)
        val partitionPath: Path = new Path(dir)
        if (fs.exists(partitionPath) && fs.isDirectory(partitionPath)) {
          list += dir
        }
      }
    }
    list.toList
  }
}
