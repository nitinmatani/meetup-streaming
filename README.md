# Meetup Streaming

## Goal

Calculate the trending N topics from the RSVP information objects that Meetup.com provides via API as stream (but in this case simple batching multiple entries from a file was choosen).

## Prerequisites

You will need to install:

**For DataBricks cluster:**
1. Spin a cluster on Databricks community edition(10.4 LTS ML (includes Apache Spark 3.2.1, Scala 2.12))
2. Install **com.johnsnowlabs.nlp:spark-nlp_2.12:3.4.4** on the cluster once its up

**For Creating a Project on Intellij:**
1. JDK (for IDE like IntelliJ, you will also need Scala 2.11.8)
2. Maven 2.3.*
3. Spark 2.3.2 (download and extract the tar archive to a folder is enough)


## Run it

**For Running on Databricks cluster:**

1. Run each cell within the notebook to see output at each step.The complete output of notebook is in the file [src/main/scala/com/nitin/meetup/MeetupTrendAnalysis.html](src/main/scala/com/nitin/meetup/MeetupTrendAnalysis.html)

**For Running locally the jar using spark-submit:**

1. Do call ``` mvn package ``` from the root folder of this project
2. With the jar file  generated in previous step inside the extracted folder call ``` ./run.sh  ``` 

At the end of the Spark program it will display the resulting trending 10 topics that were found in the data of this one day.

## Details on Implementation

1. I have implemented this case study on Databricks community edition cluster for Demo purpose since it has handy notebook api for Spark where you can focus more on logic and the output at each step rather than cluster administration.The Source file html [src/main/scala/com/nitin/meetup/MeetupTrendAnalysis.html](src/main/scala/com/nitin/meetup/MeetupTrendAnalysis.html) contains the step wise process I applied using databricks notebook for iteratively working with spark

2. The implementation for running via spark-submit is provided by [src/main/scala/com/nitin/meetup/MeetupStreaming.scala](src/main/scala/com/nitin/meetup/MeetupStreaming.scala).The project folder ``` MeetupStraming ``` which is created will be actually used in production for creating project jars and deploying the same on Production cluster.Due to time constraint I could not completely execute the code by packaging the code in a jar and running through spark-submit locally but the layout of the project is ready and I have shared the same. 

3. I have also implemented the case study using Spark-Structured Streaming to replicate the events directly coming from live Meetup RSVP stream.I have implemented the same by installing kafka on Databricks clsuter and using it as a Producer and the spark-structred stream  as a consumer.We can apply the logic implemented in step 1 on this streaming data to get the ``` Trending topics ``` in real time.We could also implement a window function, to get the Tending topics ``` Trending topics ``` for a particular window interval like for e.g hour. The notebook html file for Spark-Structured Streaming is in [src/main/scala/com/nitin/meetup/MeetupStreamingTrendAnalysis.html](src/main/scala/com/nitin/meetup/MeetupStreamingTrendAnalysis.html)

4. During iteratively working with the data on Databricks I found that the group's topics are very unclearly set by the groups themselves. So it happen when just taking the defined ``` topic_name ``` text, we cannot see that similar topics might be trending, but are defined differently and we see discrepancies in output for Trending Topic. Because of this I did choose to not use the ``` topic_name ``` directly but to go with some NLP approach, trying to extract the valuable information from each RSVP. But I need to say that I am new to using NLP .This is the first instance in my career I had to explore NLP by doing some research So I maybe didn't come up with the most sophisticated approach using the NLP functions to the max.Nevertheless it is a good start and would love to dive more into NLP

But in the end, the result shows some topics that were really caught as trending:


```
scala>   trendingTopicsDf.sort(desc("value")).show
+----------+-----------+-----+
|      date|      topic|value|
+----------+-----------+-----+
|2017-03-19|     Social|321.0|
|2017-03-19|        New|263.0|
|2017-03-19| Networking|239.0|
|2017-03-19|     Meetup|190.0|
|2017-03-19|       Town|180.0|
|2017-03-19|      Group|154.0|
|2017-03-19|    Culture|146.0|
|2017-03-19|Development|145.0|
|2017-03-19|        Fun|140.0|
|2017-03-19| Technology|118.0|
+----------+-----------+-----+
```

Here you can see, that also some words were still caught that I didn't exclude providing maybe another dictionary to the NLP functions.
So, e.g. "New", "Group", "Town" and "Meetup" could be excluded, as they don't show any useful direction of the topic.Also using some Pretrained NLP models might imporve the output.

## Assumptions for Calculating Trending Topics:
1. The usage of bringing some guests involved in the groups topics also accounted for better calculation of Trending topics.
2. The response value ``` no ``` is also taken into account for lowering the value for calculation of Trending topics

In general having better values for the group topics, or even some labels for each event might help a lot providing better results.
Also the event's text description could be used with the NLP (and using more advanced functions of it) to extract the real background of the event to get maybe even more useful information then the topic sometimes provides.


## Improvements and extensions

- Directly attaching to live API from Meetup.com since it was mentioned there was some authentication issue,but I have replicated this case using Spark-Structured Streaming on Databricks cluster
- Using a window function, since it would not make much sense for the smaller amount of data of the batch for a timeframe of a day. Maybe selecting an hour would make sense.The Pesudo code is available in the html file but it was difficult to replicate the output since there was no real time streams available and windowing is based on eventtime.
- Maximizing spell checking and avoidance of any other unwanted symbols that are not filtered by the NLP functions. There could still be improvements using advanced NLP functions, but those would not influence the top topics much.
- Geo-taging and filtering could be implemented as next step though execution of trending topics based on cityname is available in my databricks notebook.
- Implementing unit tests using ```scala-test ``` apis could help in testdriven approach of development, but since this implementation uses mostly simpler functions there is not much to go wrong.


