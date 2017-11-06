package com.linkedin.camus.shopify

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser

case class Params(targetPaths: String = null, backupPath: String = null)

object DedupFolder {
  val log: Logger = Logger.getLogger(DedupFolder.getClass)

  val argsParser = new OptionParser[Params](DedupFolder.getClass.getCanonicalName) {
    head("De-duplicate the contents of a folder")
    help("help") text "Prints this usage text"

    opt[String]('p', "target-paths") required() valueName "<path>" action { (x, p) =>
      p.copy(targetPaths = x)
    } text "The comma separated list of paths to de-duplicate"

    opt[String]('c', "backup-path") required() valueName "<path>" action { (x, p) =>
      p.copy(backupPath = x)
    } text "The path to save the current contents of targetPath"
  }

  def main(args: Array[String]): Unit = {
    argsParser.parse(args, Params()) match {
      case Some(params) =>
        val backupPath = params.backupPath.replace("/$", "") // remove trailing slash
      val targetPaths = params.targetPaths
        if (!backupPath.startsWith("/")) {
          throw new Exception(s"Backup path file location $backupPath must be absolute.")
        }
        val spark = SparkSession.builder().getOrCreate()
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val paths: Array[String] = getPaths(spark, targetPaths)
        DedupFolder.run(spark, fs, paths, backupPath)
      case None =>
        log.error("Missing required arguments, use --help.")
    }
  }

  def getPaths(spark: SparkSession, targetPaths: String): Array[String] = {
    val paths = if (targetPaths.startsWith("file://")) {
      val location = targetPaths.replace("file://", "")
      if (!location.startsWith("/")) {
        throw new Exception(s"Target path file location $location must be absolute.")
      }
      log.info(s"Reading target-paths from file at $location")
      spark.read.text(location).collect().map(_.getString(0))
    } else {
      log.info("Reading comma-separated target-paths from command-line")
      targetPaths.split(",")
    }
    val allAbsolute = paths.iterator.forall(_.startsWith("/"))
    if (!allAbsolute) {
      throw new Exception(s"All target paths must be absolute. Please check your paths.")
    }
    paths
  }

  def run(spark: SparkSession, fs: FileSystem, paths: Array[String], backupPath: String): Unit = {
    // Deduplicate each path
    paths.foreach(path => {
      log.info(s"Processing $path")
      dedup(spark, TextWriter(), fs, path, backupPath)
    })
  }

  def dedup(spark: SparkSession, textWriter: TextWriter, fs: FileSystem, path: String, backupDir: String): Boolean = {
    val data = spark.read.text(path)

    val count = data.count()
    val uniqCount = data.distinct().count()

    if (count != uniqCount) {
      log.info(s"Found ${count - uniqCount} duplicates in $path")

      val backupPath = s"$backupDir$path"

      log.info(s"Moving $path to $backupPath")
      fs.mkdirs(new Path(backupPath))
      val res = fs.rename(new Path(path), new Path(backupPath))

      if (!res) {
        throw new BackupException(s"Couldn't move $path to $backupPath")
      }

      try {
        val partitions = fs.listStatus(new Path(backupPath)).map(_.getPath.getName)
          .count(p => ! (p.startsWith(".") || p.startsWith("_")))
        log.info(s"Writing de-duplicated data to $path")
        val distinctData = spark.read.text(backupPath).distinct().repartition(partitions)
        textWriter.write(distinctData, path)
        log.info(s"Completed de-duplication of $path successfully.")
        true
      } catch {
        case e: Exception =>
          log.error(s"Problem completing writing de-dup data for $path. Rolling back $backupPath to $path.")
          val rollbackRes = fs.rename(new Path(backupPath), new Path(path))

          if (!rollbackRes) {
            throw new RollbackException(s"Couldn't rollback $backupPath back to $path !")
          }
          else throw e
      }
    } else {
      log.info(s"No duplicates found in $path")
      false
    }
  }

}

class TextWriter {
  def write(df: DataFrame, path: String): Unit = {
    df.write.option("compression", "gzip").text(path)
  }
}

object TextWriter {
  def apply(): TextWriter = new TextWriter()
}

class BackupException(val message: String) extends Exception(message)
class RollbackException(val message: String) extends Exception(message)
