package com.reuthlinger.meetup

/*
This script shows the code I developed exploratively to get the solution running while using spark-shell

Run:
spark-shell \
--driver-memory 2G --executor-memory 8G \
--conf "spark.driver.memoryOverhead=2G spark.executor.memoryOverhead=2G
spark.serializer=org.apache.spark.serializer.KryoSerializer spark.kryoserializer.buffer.max=200M" \
--jars libs/spark-nlp-assembly-1.6.3.jar

And copy paste the lines below and type Enter to create the object, then type "MeetupScript" to execute all commands
 */

import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.base._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

object MeetupScript {

  val spark: SparkSession = SparkSession.builder().getOrCreate()

  val meetupFile: String = "data/meetup.json"
  val N: Int = 10

  val meetup: DataFrame = (spark.read.json(meetupFile)
    .filter("_corrupt_record is null")
    .drop("_corrupt_record")
    .cache())

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val meetupTopics: Dataset[MeetupTopicInformation] = meetup.map(row => {
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
  }).cache()
  meetupTopics.show

  meetupTopics.selectExpr(
    "rsvpId",
    "rsvpModificationDatetime",
    "response",
    "guests",
    "groupName",
    "explode(groupTopics) as topic",
    "eventName"
  ).sort("topic").show

  /* but topic seems to be free text..., so like this not useful
+----------+------------------------+--------+------+--------------------+--------------------+--------------------+
|    rsvpId|rsvpModificationDatetime|response|guests|           groupName|               topic|           eventName|
+----------+------------------------+--------+------+--------------------+--------------------+--------------------+
|1658878647|           1489926422115|     yes|     0|Cloud Native Appl...|["Cloud Computing...|Creating Your Fir...|
|1658877514|           1489925582888|     yes|     0|Studenckie Grupy ...|      [.NET, dotnet]|Akademia C# Spotk...|
|1658877966|           1489925946982|     yes|     0|Sitecore User Gro...|      [.NET, dotnet]|Session #11 - SUG...|
|1658878033|           1489926004440|     yes|     1|Enterprise Develo...|      [.NET, dotnet]|21. Meetup: Azure...|
|1658877705|           1489925734817|     yes|     0|MSDEV Serbia User...|      [.NET, dotnet]|Visual Studio 201...|
|1658878254|           1489926158367|     yes|     0|Tokyo .NET Develo...|      [.NET, dotnet]|Tokyo .NET Develo...|
|1658878254|           1489926158000|     yes|     0|Tokyo .NET Develo...|      [.NET, dotnet]|Tokyo .NET Develo...|
|1658878349|           1489926210946|     yes|     0|MCT Community Pak...|      [.NET, dotnet]|Data Modeling wit...|
|1658878831|           1489926546003|     yes|     0|Studenckie Grupy ...|      [.NET, dotnet]|Akademia C# Spotk...|
|1658878386|           1489926233712|     yes|     0|Studenckie Grupy ...|      [.NET, dotnet]|Akademia C# Spotk...|
|1658877966|           1489925946982|     yes|     0|Sitecore User Gro...|[.NET Content Man...|Session #11 - SUG...|
|1658878703|           1489926465241|      no|     0|ACA Adult Childen...|[12 Step Recovery...|         ACA MEETING|
|1658878292|           1489926183300|     yes|     0|Free and Affordab...|[20's  30's  40's...|Official Launch P...|
|1658878292|           1489926183000|     yes|     0|Free and Affordab...|[20's  30's  40's...|Official Launch P...|
|1657317641|           1489925535661|      no|     0|20s and 30s Socia...|[20's & 30's Soci...|Yoga @ LaBelle Wi...|
|1658877505|           1489925577495|     yes|     0|   Debates in London|[20's & 30's Soci...|House of Commons ...|
|1658877943|           1489925932652|     yes|     0|The Clarendon Tea...|[20's & 30's Soci...|Tea Time & Shit H...|
|1655195034|           1489925707753|      no|     0|       Real New York|[20's & 30's Soci...|French and Italia...|
|1658878126|           1489926077734|      no|     0|Dogs & Drinks Meetup|[20's & 30's Soci...|Sunday Afternoon ...|
|1658800672|           1489926080971|      no|     0|Cape Town 20s & E...|[20's & 30's Soci...|The Charles Café ...|
+----------+------------------------+--------+------+--------------------+--------------------+--------------------+
    */

  val meetupTexts: Dataset[MeetupWholeTextInformation] = meetup.map(row => {
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
  }).cache()
  meetupTexts.show

  val input: DocumentAssembler = (new DocumentAssembler()
    .setInputCol("wholeTextInformation")
    .setOutputCol("document"))

  val token: Tokenizer = (new Tokenizer()
    .setInputCols("document")
    .setOutputCol("token"))

  val normalizer: Normalizer = (new Normalizer()
    .setInputCols("token")
    .setOutputCol("normal"))

  val finisher: Finisher = (new Finisher()
    .setInputCols("normal"))

  val remover: StopWordsRemover = (new StopWordsRemover()
    .setInputCol("finished_normal")
    .setOutputCol("filtered"))

  val pipeline: Pipeline = new Pipeline().setStages(Array(input, token, normalizer, finisher, remover))
  val nlpResult: DataFrame = (pipeline.fit(Seq.empty[String].toDS
    .toDF("wholeTextInformation")).transform(meetupTexts))

  val result: Dataset[MeetupCleansedTopicsInformation] = (nlpResult
    .selectExpr("rsvpId", "rsvpModificationDatetime", "response", "guests", "explode(filtered) as topic")
    .filter("length(topic) >= 3") // still contained some bad words, that were even not found by stop word filter...
    .filter("topic != 'and'") // and this maybe does also not interest us too much
    .selectExpr("rsvpId", "rsvpModificationDatetime", "response", "guests",
    "initcap(lower(topic)) as topic") // remove caps words and correct initial upper case letter
    .dropDuplicates("rsvpId", "rsvpModificationDatetime", "response", "guests", "topic")
    .groupBy("rsvpId", "rsvpModificationDatetime", "response", "guests")
    .agg(collect_list("topic").as("topics")).as[MeetupCleansedTopicsInformation])
  result.show(truncate = false)

  // start analyzing
  val valuedResult: Dataset[MeetupTopicValueInformation] = (result
    .selectExpr("cast(to_date(from_unixtime(rsvpModificationDatetime / 1000)) as string) as date", "rsvpId",
      "rsvpModificationDatetime", "response", "guests", "topics")
    .withColumn("value",
      when(col("response") === lit("yes"),
        lit("1")).otherwise(lit("-1"))
    )
    .withColumn("totalValue",
      when(col("guests") > lit("0"),
        lit("1") + col("guests") * col("value"))
        .otherwise(col("value")))
    .drop("response", "guests", "value")
    .selectExpr("rsvpId", "rsvpModificationDatetime", "explode(topics) as topic", "date",
      "cast(totalValue as double) as value")
    .as[MeetupTopicValueInformation]
    )

  val trendingTopics: Dataset[TrendingTopic] = (valuedResult
    .groupBy("date", "topic")
    .agg(sum("value").as("value"))
    .sort(desc("value")).as[TrendingTopic])

  val trendingNTopics: Array[TrendingTopic] = trendingTopics.sort(desc("value")).head(10)
  val trendingTopicsRDD: RDD[TrendingTopic] = spark.sparkContext.parallelize(trendingNTopics)
  val trendingTopicsDf: DataFrame = spark.createDataFrame(trendingTopicsRDD)
  (trendingTopicsDf.coalesce(1).sort(desc("value"))
    .write.format("csv").option("header", "true").save("trending-N-topics"))
  trendingTopicsDf.sort(desc("value")).show
}
