package com.linkedin.camus.shopify

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.mockito.ArgumentMatchers.{eq => eqTo, _}
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import org.scalatest.mockito.MockitoSugar


class TestDedupFolder extends FunSuite with BeforeAndAfter with BeforeAndAfterAll with MockitoSugar {
  var mockSpark: SparkSession = _
  var fs: FileSystem = _
  var sc: SparkContext = _
  var dataFrameReader: DataFrameReader = _
  var textWriter: TextWriter = _

  val path = "/tmp/_camus_tests/data"
  val backupDir = "/tmp/_camus_tests/backup"

  val conf: SparkConf = new SparkConf().setAppName("camustest").setMaster("local[1]")

  override def beforeAll() {
    sc = new SparkContext(conf)
    super.beforeAll()
  }

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
    sc = null
    super.afterAll()
  }

  val schema = StructType(
    Array(
      StructField("string", StringType, nullable = false)
    ))

  before {
    mockSpark = mock[SparkSession]
    fs = mock[FileSystem]
    dataFrameReader = mock[DataFrameReader]
    textWriter = mock[TextWriter]
    when(mockSpark.read).thenReturn(dataFrameReader)
  }

  test("throws an error if target paths come from a file and path to that file is not absolute") {
    assertThrows[Exception](DedupFolder.getPaths(mockSpark, "file://foo/bar"))
  }

  test("parses comma-separated paths and throws an error if not all paths absolute") {
    assertThrows[Exception](DedupFolder.getPaths(mockSpark, "/foo/bar,/foo/bar2,foo/barr"))
  }

  test("parses comma-separated paths") {
    val paths = DedupFolder.getPaths(mockSpark, "/foo/bar,/foo/bar2,/foo/bar3")

    assert(paths sameElements Array("/foo/bar", "/foo/bar2", "/foo/bar3"))
  }

  test("reads paths from file") {
    val paths = Seq("/foo/bar", "/foo/bar2", "/foo/bar3")
    when(dataFrameReader.text(anyString())).thenReturn(toDF(paths.map(p => Row(p)), schema))

    val result = DedupFolder.getPaths(mockSpark, "file:///list")
    assert(result sameElements paths)
  }

  test("reads paths from file and throws an error if not all paths are absolute") {
    val paths = Seq("/foo/bar", "/foo/bar2", "foo/bar3")
    when(dataFrameReader.text(anyString())).thenReturn(toDF(paths.map(p => Row(p)), schema))

    assertThrows[Exception](DedupFolder.getPaths(mockSpark, "file:///list"))
  }

  test("does nothing if there are no duplicates") {
    val data = Seq("a", "b", "c").map(x => Row(x))
    when(dataFrameReader.text(path)).thenReturn(toDF(data, schema))

    assert(!DedupFolder.dedup(mockSpark, mock[TextWriter], fs, path, backupDir))
    verify(fs, never()).rename(new Path(path), new Path(s"$backupDir$path"))
    verify(fs, never()).rename(new Path(s"$backupDir$path"), new Path(path))
  }

  test("backs up the data and deduplicates correctly") {
    val data = toDF(Seq("a", "b", "c", "b").map(x => Row(x)), schema)
    when(dataFrameReader.text(path)).thenReturn(data)
    when(dataFrameReader.text(s"$backupDir$path")).thenReturn(data)
    // so we can back up
    when(fs.rename(new Path(path), new Path(s"$backupDir$path"))).thenReturn(true)
    // for partitioning
    when(fs.listStatus(any[Path]())).thenReturn(
      Array[FileStatus](new FileStatus(0, false, 0, 0, 0, new Path("p.gz"))))

    val deduped = DedupFolder.dedup(mockSpark, textWriter, fs, path, backupDir)

    verify(fs).rename(new Path(path), new Path(s"$backupDir$path"))
    verify(textWriter).write(any(), eqTo(path))
    assert(deduped)
  }

  test("throws an error if can not backup directory before deduplicating") {
    val data = toDF(Seq("a", "b", "c", "b").map(x => Row(x)), schema)
    when(dataFrameReader.text(path)).thenReturn(data)
    when(dataFrameReader.text(s"$backupDir$path")).thenReturn(data)
    when(fs.rename(new Path(path), new Path(s"$backupDir$path"))).thenReturn(false)
    assertThrows[BackupException](DedupFolder.dedup(mockSpark, textWriter, fs, path, backupDir))
  }

  test("rolls back the data if can not write deduplicated, throws an error") {
    val data = toDF(Seq("a", "b", "c", "b").map(x => Row(x)), schema)
    when(dataFrameReader.text(path)).thenReturn(data)
    when(dataFrameReader.text(s"$backupDir$path")).thenReturn(data)
    when(fs.rename(new Path(path), new Path(s"$backupDir$path"))).thenReturn(true)
    when(fs.rename(new Path(s"$backupDir$path"), new Path(path))).thenReturn(true)
    when(textWriter.write(any(), any())).thenThrow(new IllegalArgumentException("boom"))

    assertThrows[Exception](DedupFolder.dedup(mockSpark, textWriter, fs, path, backupDir))
    verify(fs).rename(new Path(path), new Path(s"$backupDir$path"))
    verify(fs).rename(new Path(s"$backupDir$path"), new Path(path))
  }

  test("throws an error if can not roll back the directory") {
    val data = toDF(Seq("a", "b", "c", "b").map(x => Row(x)), schema)
    when(dataFrameReader.text(any())).thenReturn(data)
    when(fs.rename(new Path(path), new Path(s"$backupDir$path"))).thenReturn(true)
    when(fs.rename(new Path(s"$backupDir$path"), new Path(path))).thenReturn(false)
    when(textWriter.write(any(), any())).thenThrow(new IllegalArgumentException("boom"))

    assertThrows[RollbackException](DedupFolder.dedup(mockSpark, textWriter, fs, path, backupDir))
    verify(fs).rename(new Path(s"$backupDir$path"), new Path(path))
  }

  def toDF(seq: Seq[Row], schema: StructType): DataFrame = {
    val extractRDD = sc.makeRDD(seq)
    val spark = SparkSession.builder().appName("camustest").master("local").getOrCreate()
    spark.createDataFrame(extractRDD, schema)
  }

  test("Integration test: reads and writes deduplicated data correctly") {
    val dir = s"$path/duplicates"
    val spark = SparkSession.builder().appName("camustest").master("local[1]").getOrCreate()

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // reset the dir state
    fs.delete(new Path(dir))
    fs.delete(new Path(s"$backupDir$dir"))

    fs.mkdirs(new Path(path))
    fs.mkdirs(new Path(backupDir))

    // write test data
    val data = toDF(Seq(Row("a"), Row("b"), Row("c"), Row("b")), schema)
    data.repartition(1).write.option("compression", "gzip").text(dir)

    // deduplicate
    DedupFolder.dedup(spark, new TextWriter(), fs, dir, backupDir)

    // test the data was deduplicated
    val deduplicated = spark.read.text(dir)
    assert(deduplicated.count() == 3)
    assert(deduplicated.collect().map(_.getAs[String](0)).toList.sorted == List("a", "b", "c"))

    // only 1 file was written
    assert(fs.listStatus(new Path(dir)).map(p => p.getPath.getName)
      .count(p => ! (p.startsWith(".") || p.startsWith("_"))) == 1)
  }

}
