package com.linkedin.camus.shopify

import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat
import org.apache.hadoop.fs.{FileSystem, Path}

class CamusHourlyDrop(folderPath: Path, fileSystem: FileSystem) {
  val path = folderPath
  val fs = fileSystem
  val FLAG_NAME = "_IMPORTED"

  def flagPath: Path = new Path(path.toString + s"/$FLAG_NAME")

  def hasFlag: Boolean = fs.exists(flagPath)

  def flagWrittenAt: Long = fs.getFileStatus(flagPath).getModificationTime

  def lastFileWrittenAt: Long = {
    fs.listStatus(path).
      filter(status => status.getPath.getName != FLAG_NAME).
      map(status => status.getModificationTime).
      max
  }

  def topicDir: String = {
    new Path(path.toString.replace(EtlMultiOutputFormat.ETL_DESTINATION_PATH, ""))
      .getParent.getParent.getParent.getParent.getName
  }

  def isFlagViolated: Boolean = lastFileWrittenAt > flagWrittenAt

  def violationCount: Long = {
    val flagTime = flagWrittenAt
    fs.listStatus(path).
      filter(status => status.getPath.getName != FLAG_NAME && status.getModificationTime > flagTime).
      map(status => status.getPath.getName).
      map(name => name.split("\\.")(3).toLong). // this relies on how we format file names in EtlMultiOutputCommitter.getPartitionedPath
      sum
  }
}
