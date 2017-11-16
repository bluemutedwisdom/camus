package com.linkedin.camus.shopify

import java.io.FileInputStream
import java.util.Properties

import com.linkedin.camus.etl.kafka.reporter.StatsdReporter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.wikimedia.analytics.refinery.job.CamusStatusReader
import scopt.OptionParser

object LateArrivingDataMonitor {
  val log: Logger = Logger.getLogger(LateArrivingDataMonitor.getClass)

  case class Params(camusPropertiesFilePath: String = "",
                    window: Long = 1000 * 60 * 60 * 6) // 6 hours window

  val argsParser = new OptionParser[Params]("com.linkedin.camus.shopify.LateArrivingDataMonitor") {
    head("Camus Late Arriving Data Checker", "")
    note(
      "This Job checks for late arriving data in Camus drops.")
    help("help") text "Prints this usage text"

    opt[String]('c', "camus-properties-file") required() valueName "<path>" action { (x, p) =>
      p.copy(camusPropertiesFilePath = x)
    } text "Camus configuration properties file path."

    opt[Long]('w', "window") valueName "<window>" action { (x, p) =>
      p.copy(window = x)
    } text "Camus configuration properties file path."
  }


  def checkDropsInWindow(properties: Properties, fs: FileSystem, window: Long): Boolean = {
    log.info("Fetching topics and folders to check...")
    val camusExecutions = new CamusExecutions(properties, fs)
    log.info(s"Checking folders in window ($window) to flag violation...")
    val foldersToCheck = camusExecutions.droppedFoldersInWindow(window)
    var violationFound = false
    foldersToCheck.foreach(
      folder => {
        log.info(s"Checking $folder")
        val camusDrop = new CamusHourlyDrop(new Path(folder), fs)

        if (camusDrop.hasFlag && camusDrop.isFlagViolated) {
          violationFound = true
          log.error(s"Violation found in ${camusDrop.path}")
          try {
            val count = camusDrop.violationCount
            log.error(s"=> late-arriving records for dir ${camusDrop.topicDir} in ${camusDrop.path}: $count")
            StatsdReporter.gauge(properties, "late-arriving-data", 1L, s"directory:${camusDrop.topicDir}")
          }
          catch {
            case e: Exception => log.error(s"Could not get count of violations for $folder: $e")
          }
        }
      }
    )
    violationFound
  }

  def main(args: Array[String]): Unit = {
    argsParser.parse(args, Params()) match {
      case Some (params) =>
        log.info("Setting up...")
        val fs = FileSystem.get(new Configuration())
        val camusReader = new CamusStatusReader(fs)
        val properties: Properties = new Properties()
        properties.load(new FileInputStream(params.camusPropertiesFilePath))

        val violationFound = checkDropsInWindow(properties, fs, params.window)

        if (violationFound) {
          log.error("ERROR: One or more folders have late-arriving data.")
          sys.exit(1)
        } else {
          log.info("No late-arriving data found.")
        }
      case None =>
        log.error("missing arguments, use --help")
    }
  }
}
