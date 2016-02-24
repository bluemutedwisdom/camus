package com.linkedin.camus.shopify

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

  def isFlagViolated: Boolean = lastFileWrittenAt > flagWrittenAt
}
