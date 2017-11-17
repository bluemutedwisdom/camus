package com.linkedin.camus.shopify

import java.util.Properties

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import com.linkedin.camus.etl.kafka.CamusJob
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.wikimedia.analytics.refinery.job.{CamusPartitionChecker, CamusStatusReader}

class CamusExecutions(properties: Properties, fs: FileSystem) {
  val historyFolder = properties.getProperty(CamusJob.ETL_EXECUTION_HISTORY_PATH)
  val camusReader = new CamusStatusReader(fs)

  val log: Logger = Logger.getLogger(classOf[CamusExecutions])

  def topicsAndHoursInWindow(window: Long): Map[String, Seq[(Int, Int, Int, Int)]] = {
    val camusRunPath = camusReader.mostRecentRun(new Path(historyFolder))
    log.info(s"Most recent Camus run folder found: $camusRunPath")
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

  def lastRunDirs(numRuns: Int): List[Path] = camusReader.mostRecentRuns(new Path(historyFolder), numRuns)

  private def isDirsWrittenList(path: Path) = path.getName.startsWith(EtlMultiOutputFormat.PATHS_WRITTEN_PREFIX)

  def droppedFoldersInRuns(camusRunPaths: List[Path]): Seq[String] = {
    val allPathsValid = camusRunPaths.forall(path => fs.listStatus(path).exists(f => isDirsWrittenList(f.getPath)))
    if (! allPathsValid) {
      throw new Exception("Some executions paths do not contain expected dirs lists.")
    }

    val pathsToCheck = mutable.Set[String]()
    // iterate over folders and collect the paths written in the last runs
    camusRunPaths.foreach({
      runPath =>
        log.info(s"Collecting directories written to in run $runPath")
        // for each execution there will be one file per map task
        val dirsWrittenToByTasks = fs.listStatus(runPath).map(f => f.getPath).filter(isDirsWrittenList)
        log.info(s"Found ${dirsWrittenToByTasks.length} files with directory lists in $runPath.")
        // collect all the dirs from all the tasks into a single set
        dirsWrittenToByTasks.foreach({
          dirsWrittenToByTask =>
            val stream = fs.open(dirsWrittenToByTask)
            val readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))
            readLines.takeWhile(_ != null).foreach(line => pathsToCheck.add(line))
            stream.close()
        })
    })
    pathsToCheck.toList.sorted
  }
}
