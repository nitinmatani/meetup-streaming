package com.reuthlinger.meetup

import com.johnsnowlabs.nlp.annotator.{Normalizer, Tokenizer}
import com.johnsnowlabs.nlp.base.{DocumentAssembler, Finisher}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

/**
  * Nice class to get Meetup's RSVP information transformed into the trending N topics per day.
  */
object MeetupExperiment {

  private val spark: SparkSession = SparkSession.builder()
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  spark.sparkContext.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  case class Config(
                     batchFileDataPath: String = "data/meetup.json",
                     topN: Int = 10
                   )

  /**
    * Extract the JVM call parameters and parse them into a [[Config]] object.
    *
    * @param args JVM call arguments.
    * @return Defined config object.
    */
  private def parseArgs(args: Array[String]): Config = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("TaxiExperiment")

      opt[String]("data-file-path")
        .optional
        .action((x, c) => c.copy(batchFileDataPath = x))
        .text("(relative) path to the batch data file containing one rsvp per line")

      opt[Int]("top-N")
        .optional
        .action((x, c) => c.copy(topN = x))
        .text("how many of the top trending topics shall be provided as result")

    }

    val parsedConfig = parser.parse(args, Config()) match {
      case Some(config) => config
      case None =>
        println("Wrong arguments specified. Please see the usage for more details.")
        sys.exit(1)
    }
    parsedConfig
  }

  /**
    * Read the basic Json RSVP messages from a file containing one RSVP Json entry per line. Incorrect Json elements
    * will be dropped.
    *
    * @param config Spark job's config.
    * @return [[DataFrame]] containing all valid RSVP entries read from file.
    */
  private def readRsvpDataFromFile(config: Config): DataFrame = {
    val meetupRsvpDataFrame: DataFrame = spark
      .read.json(config.batchFileDataPath)
      .filter("_corrupt_record is null")
      .drop("_corrupt_record")
    meetupRsvpDataFrame
  }

  /**
    * Extract the basic information fields from the Meetup's RSVP information.
    *
    * @param meetupRsvpDf [[DataFrame]] created by [[MeetupExperiment.readRsvpDataFromFile()]]
    * @return [[Dataset]] of [[MeetupTopicInformation]] objects
    */
  private def extractTopicInformationFromDf(meetupRsvpDf: DataFrame): Dataset[MeetupTopicInformation] = {
    import spark.implicits._
    val meetupTopics: Dataset[MeetupTopicInformation] = meetupRsvpDf.map(row => {
      val rsvpId: Long = row.getLong(row.fieldIndex("rsvp_id"))
      val rsvpModificationTime: Long = row.getLong(row.fieldIndex("mtime"))
      val event: Row = row.getStruct(row.fieldIndex("event"))
      val eventName: String = event.getString(event.fieldIndex("event_name"))
      val group: Row = row.getStruct(row.fieldIndex("group"))
      val groupName: String = group.getString(group.fieldIndex("group_name"))
      val groupTopics: mutable.WrappedArray[Row] = group.getAs[mutable.WrappedArray[Row]]("group_topics")
      var topics: Array[(String, String)] = Array[(String, String)]()
      groupTopics.foreach(topicsRow => {
        val topicName = topicsRow.getString(topicsRow.fieldIndex("topic_name"))
        val topicUrl = topicsRow.getString(topicsRow.fieldIndex("urlkey"))
        topics = topics ++ Array[(String, String)]((topicName, topicUrl))
      })
      val guests: Long = row.getLong(row.fieldIndex("guests"))
      val response: String = row.getString(row.fieldIndex("response"))

      MeetupTopicInformation(
        rsvpId,
        rsvpModificationTime,
        response,
        guests,
        groupName,
        topics,
        eventName
      )
    })
    meetupTopics
  }

  /**
    * Extract the complete (concatenated) textual information (that might be interesting for finding trending topics)
    * from the basic [[DataFrame]] gathered from reading Json information. Choose this format for doing NLP.
    *
    * @param meetupRsvpDf [[DataFrame]] created by [[MeetupExperiment.readRsvpDataFromFile()]]
    * @return [[Dataset]] of [[MeetupWholeTextInformation]] objects
    */
  private def extractWholeTextFromDf(meetupRsvpDf: DataFrame): Dataset[MeetupWholeTextInformation] = {
    import spark.implicits._
    val meetupTexts: Dataset[MeetupWholeTextInformation] = meetupRsvpDf.map(row => {
      val rsvpId: Long = row.getLong(row.fieldIndex("rsvp_id"))
      val rsvpModificationTime: Long = row.getLong(row.fieldIndex("mtime"))
      val event: Row = row.getStruct(row.fieldIndex("event"))
      val eventName: String = event.getString(event.fieldIndex("event_name"))
      val group: Row = row.getStruct(row.fieldIndex("group"))
      val groupName: String = group.getString(group.fieldIndex("group_name"))
      val groupTopics: mutable.WrappedArray[Row] = group.getAs[mutable.WrappedArray[Row]]("group_topics")
      var concatenatedTopics: String = ""
      groupTopics.foreach(topicsRow => {
        concatenatedTopics += s" ${topicsRow.getString(topicsRow.fieldIndex("topic_name"))} "
      })
      val guests: Long = row.getLong(row.fieldIndex("guests"))
      val response: String = row.getString(row.fieldIndex("response"))
      val wholeTextInformation: String = s"$groupName $concatenatedTopics $eventName"

      MeetupWholeTextInformation(
        rsvpId,
        rsvpModificationTime,
        response,
        guests,
        wholeTextInformation
      )
    })
    meetupTexts
  }

  /**
    * Transform the extracted [[MeetupWholeTextInformation]]'s concatenated text piece using NLP functions into a
    * list of relevant topic tokens (duplicates removed).
    *
    * @param inputDs [[Dataset]] of [[MeetupWholeTextInformation]] created by
    *                [[MeetupExperiment.extractWholeTextFromDf()]].
    * @return [[Dataset]] of [[MeetupCleansedTopicsInformation]] to analyze trending topics.
    */
  private def transformTextsUsingNlp(inputDs: Dataset[MeetupWholeTextInformation])
  : Dataset[MeetupCleansedTopicsInformation] = {
    import spark.implicits._
    val input: DocumentAssembler = new DocumentAssembler()
      .setInputCol("wholeTextInformation")
      .setOutputCol("document")

    val token: Tokenizer = new Tokenizer()
      .setInputCols("document")
      .setOutputCol("token")

    val normalizer: Normalizer = new Normalizer()
      .setInputCols("token")
      .setOutputCol("normal")

    val finisher: Finisher = new Finisher()
      .setInputCols("normal")

    val remover: StopWordsRemover = new StopWordsRemover()
      .setInputCol("finished_normal")
      .setOutputCol("filtered")

    val pipeline: Pipeline = new Pipeline().setStages(Array(input, token, normalizer, finisher, remover))
    val nlpResult: DataFrame = pipeline.fit(Seq.empty[String].toDS
      .toDF("wholeTextInformation")).transform(inputDs)

    val result: Dataset[MeetupCleansedTopicsInformation] = nlpResult
      .selectExpr("rsvpId", "rsvpModificationDatetime", "response", "guests", "explode(filtered) as topic")
      .filter("length(topic) >= 3") // still contained some bad words, that were even not found by stop word filter...
      .filter("topic != 'and'") // and this maybe does also not interest us too much
      .selectExpr("rsvpId", "rsvpModificationDatetime", "response", "guests",
      "initcap(lower(topic)) as topic") // remove caps words and correct initial upper case letter
      .dropDuplicates("rsvpId", "rsvpModificationDatetime", "response", "guests", "topic")
      .groupBy("rsvpId", "rsvpModificationDatetime", "response", "guests")
      .agg(collect_list("topic").as("topics")).as[MeetupCleansedTopicsInformation]

    result
  }

  /**
    * Calculate the value of this RSVP message (based on sign up or off and number of guests) for later aggregation
    * of trending topics.
    * This calculation will also remove some fields that were used for the calculation and will explode the array of
    * topics into multiple lines. By this the aggregation is very simple.
    *
    * @param inputDs [[Dataset]] of [[MeetupCleansedTopicsInformation]] created by
    *                [[MeetupExperiment.transformTextsUsingNlp]]
    * @return [[Dataset]] of [[MeetupTopicValueInformation]]
    */
  private def calculateRsvpTopicValue(inputDs: Dataset[MeetupCleansedTopicsInformation])
  : Dataset[MeetupTopicValueInformation] = {
    import spark.implicits._

    val resultDs: Dataset[MeetupTopicValueInformation] = inputDs
      .selectExpr("cast(to_date(from_unixtime(rsvpModificationDatetime / 1000)) as string) as date", "rsvpId",
        "rsvpModificationDatetime", "response", "guests", "topics")
      .withColumn("value",
        when(col("response") === lit("yes"),
          lit("1")).otherwise(lit("-1")))
      .withColumn("totalValue",
        when(col("guests") > lit("0"),
          lit("1") + col("guests") * col("value"))
          .otherwise(col("value")))
      .drop("response", "guests", "value")
      .selectExpr("rsvpId", "rsvpModificationDatetime", "explode(topics) as topic", "date",
        "cast(totalValue as double) as value")
      .as[MeetupTopicValueInformation]

    resultDs
  }

  /**
    * Calculate the trending topics by summing up the values per day.
    *
    * @param inputDs [[Dataset]] of [[MeetupTopicValueInformation]] created by
    *                [[MeetupExperiment.calculateRsvpTopicValue()]]
    * @return [[Dataset]] of [[TrendingTopic]]s
    */
  private def getTrendingTopics(inputDs: Dataset[MeetupTopicValueInformation]): Dataset[TrendingTopic] = {
    import spark.implicits._
    inputDs
      .groupBy("date", "topic")
      .agg(sum("value").as("value"))
      .as[TrendingTopic]
  }

  /**
    * Simple main function, here all method calls are chained up.
    *
    * @param args Arguments are parsed, see [[MeetupExperiment.Config]].
    */
  def main(args: Array[String]): Unit = {
    val config: Config = parseArgs(args)

    // pre-process meetup data to enable easy trend analysis
    val meetupRsvpDf: DataFrame = readRsvpDataFromFile(config)
    val meetupTexts: Dataset[MeetupWholeTextInformation] = extractWholeTextFromDf(meetupRsvpDf)
    val meetupTokenizedTexts: Dataset[MeetupCleansedTopicsInformation] = transformTextsUsingNlp(meetupTexts).cache()
    val meetupRsvpValue: Dataset[MeetupTopicValueInformation] = calculateRsvpTopicValue(meetupTokenizedTexts)
    val trendingTopics: Dataset[TrendingTopic] = getTrendingTopics(meetupRsvpValue)

    // now finally we take the N topics from the whole list and store / show the results
    val trendingNTopics: Array[TrendingTopic] = trendingTopics.sort(desc("value")).head(config.topN)
    val trendingTopicsRDD: RDD[TrendingTopic] = spark.sparkContext.parallelize(trendingNTopics)
    val trendingTopicsDf: DataFrame = spark.createDataFrame(trendingTopicsRDD)
    trendingTopicsDf.sort(desc("value")).show
  }
}
