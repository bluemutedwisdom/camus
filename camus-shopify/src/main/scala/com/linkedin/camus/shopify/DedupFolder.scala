package com.linkedin.camus.shopify

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object DedupFolder {
  val log: Logger = Logger.getLogger(DedupFolder.getClass)

  case class Params(targetPaths: String = null,
                    backupPath: String = null)

  val argsParser = new OptionParser[Params](DedupFolder.getClass.getCanonicalName) {
    head("De-duplicate the contents of a folder")
    help("help") text "Prints this usage text"

    opt[String]('p', "target-paths") required() valueName "<path>" action { (x,p) =>
      p.copy(targetPaths = x)
    } text "The comma separated list of paths to de-duplicate"

    opt[String]('c', "backup-path") required() valueName "<path>" action { (x,p) =>
      p.copy(backupPath = x)
    } text "The path to save the current contents of targetPath"
  }

  def main(args: Array[String]): Unit = {
    argsParser.parse(args, Params()) match {
      case Some(params) => DedupFolder.run(params)
      case None => log.error("missing required arguments, use --help")
    }
  }

  def baseFolder(fullPath: String): String = {
    fullPath.replaceFirst("""\/\w+\/?$""", "")
  }

  def run(params: Params): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val paths: Array[String] = if (params.targetPaths.startsWith("file://")) {
      val location = params.targetPaths.replace("file://", "")
      log.info(s"reading target-paths from file at $location")
      spark.read.text(location).collect().map(_.getString(0))
    } else {
      log.info("reading target-paths from command-line")
      params.targetPaths.split(",")
    }

    paths.foreach( path => {
      log.info(s"processing $path")
      val data = spark.read.text(path)

      val count = data.count()
      val uniqCount = data.distinct().count()
      if (count != uniqCount) {
        log.info(s"found duplicates in $path")

        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

        val dest = s"${params.backupPath}$path"
        log.info(s"move $path to $dest")
        val destPath = baseFolder(dest)
        fs.mkdirs(new Path(destPath))
        val res = fs.rename(new Path(path), new Path(destPath))
        if (!res) throw new Exception(s"Couldn't move $path to $destPath")

        try {
          log.info(s"writing de-duplicated data to $path")
          val distinctData = spark.read.text(dest).distinct()
          distinctData.write.option("compression", "gzip").text(path)
          log.info(s"completed de-dup of $path successfully")
        } catch {
          case e: Exception =>
            log.error(s"problem completing writing de-dup data for $path. Rolling back")
            val rollbackRes = fs.rename(new Path(dest), new Path(baseFolder(path)))

            if (!rollbackRes) throw new Exception(s"Couldn't rollback $dest back to $path !")
            throw e
        }
      } else {
        log.info(s"no duplicates found in $path")
      }
    })
  }

}