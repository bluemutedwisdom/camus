package com.linkedin.camus.shopify

import org.wikimedia.analytics.refinery.job.CamusPartitionChecker


class TestWatermarking extends CamusTest {

  override def beforeEach(): Unit = {
    super.beforeEach()
    // patch the properties with our temp paths
    CamusPartitionChecker.props.setProperty("etl.destination.path", props.getProperty("etl.destination.path"))
    CamusPartitionChecker.props.setProperty("etl.watermark.path", props.getProperty("etl.watermark.path"))
  }

  // Testing watermark writing
  test("write the latest watermark for a topic that never had a watermark") {
    val topic = "test_topic"
    CamusPartitionChecker.writeHighWatermarkByTopic(Map(topic -> Seq((2014, 5, 13, 16))))
    assert(getWatermarkForTopic(topic) == 1399996800000L) // May 13, 2014 16:00:00
  }

  test("correctly writes the highest watermark") {
    val topic = "test_topic"
    CamusPartitionChecker.writeHighWatermarkByTopic(Map(topic -> Seq((2014, 5, 13, 14), (2014, 5, 13, 16), (2014, 5, 13, 15))))
    assert(getWatermarkForTopic(topic) == 1399996800000L) // May 13, 2014 16:00:00
  }

  test("write the latest watermark when watermark dir exists, but there's not watermark") {
    val topic = "test_topic"
    createWatermarkDir(topic)
    CamusPartitionChecker.writeHighWatermarkByTopic(Map(topic -> Seq((2014, 5, 13, 16))))
    assert(getWatermarkForTopic(topic) == 1399996800000L) // May 13, 2014 16:00:00
  }

  test("update the latest watermark for a directory") {
    val topic = "test_topic"
    createWatermarkDir(topic)
    createWatermark(topic, 1399989600000L) // May 13, 2014 14:00:00
    CamusPartitionChecker.writeHighWatermarkByTopic(Map(topic -> Seq((2014, 5, 13, 16))))
    assert(getWatermarkForTopic(topic) == 1399996800000L) // May 13, 2014 16:00:00
  }

  test("do not update the latest watermark for a directory if watermarking has not advanced") {
    val topic = "test_topic"
    createWatermarkDir(topic)
    val wm = 1399996800000L  // May 13, 2014 16:00:00
    createWatermark(topic, wm)
    CamusPartitionChecker.writeHighWatermarkByTopic(Map(topic -> Seq((2014, 5, 13, 14))))
    assert(getWatermarkForTopic(topic) == wm)
  }

  test("write the latest watermarks for topics correctly") {
    val watermarkedDirs = Map(
      "topic1" -> Seq((2014, 5, 13, 15), (2014, 5, 13, 16), (2014, 5, 13, 14)),
      "topic2" -> Seq((2015, 5, 13, 15), (2015, 5, 13, 16), (2015, 5, 13, 14)),
      "topic3" -> Seq((2016, 5, 13, 15), (2016, 5, 13, 16), (2016, 5, 13, 14))
    )

    val expectedWatermarks = Map(
      "topic1" -> 1399996800000L, // May 13, 2014 16:00:00
      "topic2" -> 1431532800000L, // May 13, 2015 16:00:00
      "topic3" -> 1463155200000L  // May 13, 2016 16:00:00
    )

    CamusPartitionChecker.writeHighWatermarkByTopic(watermarkedDirs)
    assert(getWatermarkForTopic("topic1") == expectedWatermarks("topic1"))
    assert(getWatermarkForTopic("topic2") == expectedWatermarks("topic2"))
    assert(getWatermarkForTopic("topic3") == expectedWatermarks("topic3"))
  }

  test("write the file flag for a given partition hour") {
    val topicPartition = "testtopic/2015/10/02/08"
    createTopicPartition(topicPartition)
    CamusPartitionChecker.flagFullyImportedPartitions(flag, false, Map("testtopic" -> Seq((2015, 10, 2, 8))))
    assert(isFlagged(topicPartition))
  }

  test("write the file flags for given partition hours") {
    val topic = "testtopic"

    val partitionsAndHours = List(
      ("2015/10/02/08", (2015, 10, 2, 8)),
      ("2015/10/02/09", (2015, 10, 2, 9))
    )

    partitionsAndHours.map(_._1).foreach({
      ph =>
        createTopicPartition(s"$topic/$ph")
        assert(!isFlagged(s"$topic/$ph"))
    })

    CamusPartitionChecker.flagFullyImportedPartitions(flag, false, Map("testtopic" -> partitionsAndHours.map(_._2)))

    assert(isFlagged(s"$topic/2015/10/02/08"))
    assert(isFlagged(s"$topic/2015/10/02/09"))
  }

  test("not overwrite the existing file flag for a given partition hour") {
    val topic = "testtopic"
    val topicPartition = s"$topic/2015/10/02/08"
    createTopicPartition(topicPartition)
    setFlag(topicPartition)
    assert(isFlagged(topicPartition))
    fs.setTimes(flagAbsolutePath(topicPartition), 1400000000000L, 0) // May 13, 2014 4:53:20 PM

    CamusPartitionChecker.flagFullyImportedPartitions(flag, false, Map(topic -> Seq((2015, 10, 2, 8))))
    assert(isFlagged(topicPartition))
    assert(fs.getFileStatus(flagAbsolutePath(topicPartition)).getModificationTime == 1400000000000L)
  }

  test("creates an empty partition directory if the given partition hour folder doesn't exist") {
    val topic = "testtopic"
    val topicPartition = s"$topic/2015/10/02/08"
    val topicPartitionToBeCreated = s"$topic/2015/10/02/07"

    createTopicPartition(topicPartition)
    assert(!isFlagged(topicPartition))
    assert(!isFlagged(topicPartitionToBeCreated))

    CamusPartitionChecker.flagFullyImportedPartitions(flag, false, Map(topic -> Seq((2015, 10, 2, 8), (2015, 10, 2, 7))))

    assert(isFlagged(topicPartition))
    assert(isFlagged(topicsPartitionAbsolutePath(topicPartitionToBeCreated).toString))
  }

}
