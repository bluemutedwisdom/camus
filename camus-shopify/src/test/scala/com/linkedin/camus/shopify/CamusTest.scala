package com.linkedin.camus.shopify

import java.io.File
import java.nio.file.Files
import java.util.Properties

import scala.reflect.io.Directory

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{path, BeforeAndAfterEach, FunSuite, Matchers}


class CamusTest extends FunSuite with Matchers with BeforeAndAfterEach {
  val fs: FileSystem = FileSystem.get(new Configuration())
  val props: Properties = new Properties()

  val flag: String = "_IMPORTED"
  private var tmpDataDir: File = null
  private var tmpWatermarkDir: File = null


  def camusDataPath: Path = new Path(tmpDataDir.getAbsolutePath)
  def camusWatermarkPath: Path = new Path(tmpWatermarkDir.getAbsolutePath)

  override def beforeEach(): Unit = {
    tmpDataDir = Files.createTempDirectory("camus_test_data").toFile
    tmpWatermarkDir = Files.createTempDirectory("camus_test_wm").toFile

    props.setProperty("etl.destination.path", camusDataPath.toString)
    props.setProperty("etl.watermark.path", camusWatermarkPath.toString)

  }

  private def recursivelyDelete(f: File): Unit = {
    if (f != null && f.exists()) {
      val d = new Directory(f)
      d.deleteRecursively()
    }
  }

  override def afterEach(): Unit = {
    recursivelyDelete(tmpDataDir)
    recursivelyDelete(tmpWatermarkDir)
  }

  private def createFile(path: String): Unit = {
    val newFile = new File(path)
    newFile.createNewFile()
  }

  private def createDir(path: String): Unit = {
    val newDir = new Directory(new File(path))
    newDir.createDirectory()
  }

  def topicsPartitionAbsolutePath(topicPartition: String): Path  = {
    new Path(camusDataPath.toString, topicPartition)
  }

  def topicsWatermarkDirAbsolutePath(topic: String): Path  = {
    new Path(camusWatermarkPath.toString, topic)
  }

  def dataFileAbsolutePath(topicPartition: String, filename: String): Path  = {
    new Path(topicsPartitionAbsolutePath(topicPartition).toString, filename)
  }

  def flagAbsolutePath(topicPartition: String): Path  = {
    new Path(topicsPartitionAbsolutePath(topicPartition).toString, flag)
  }

  def setFlag(topicPartition: String): Path = {
    val path = flagAbsolutePath(topicPartition)
    createFile(path.toString)
    path
  }

  def removeFlag(topicPartition: String): Unit = {
    val flagFile = new File(flagAbsolutePath(topicPartition).toString)
    flagFile.delete()
  }

  def isFlagged(topicPartition: String): Boolean = {
    val flagFile = new File(flagAbsolutePath(topicPartition).toString)
    flagFile.exists()
  }

  def createTopicPartition(topicPartition: String): Path = {
    val path = topicsPartitionAbsolutePath(topicPartition)
    createDir(path.toString)
    path
  }

  def writeDataFile(topicPartition: String, fileName: String): Path = {
    val path = dataFileAbsolutePath(topicPartition, fileName)
    createFile(path.toString)
    path
  }

  def createWatermarkDir(topic: String): Path = {
    val path = topicsWatermarkDirAbsolutePath(topic)
    createDir(path.toString)
    path
  }

  def createWatermark(topic: String, t: Long): Path = {
    val path = new Path(topicsWatermarkDirAbsolutePath(topic).toString, t.toString)
    createFile(path.toString)
    path
  }

  def getWatermarkForTopic(topic: String): Long = {
    val wmFolder = new File(topicsWatermarkDirAbsolutePath(topic).toString)
    val fileList = wmFolder.listFiles().filterNot(_.getName.endsWith(".crc"))
    assert(fileList.length == 1)
    fileList.head.getName.toLong
  }

}
