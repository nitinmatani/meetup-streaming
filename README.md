# Meetup Streaming

## Goal

Calculate the trending N topics from the RSVP information objects that Meetup.com provides via API as stream (but in this case simple batching multiple entries from a file was choosen).
## Prerequisits

You will need to install:
1. JDK (for IDE like IntelliJ, you will also need Scala 2.11.8)
2. Maven 2.3.*
3. Spark 2.3.2 (download and extract the tar archive to a folder is enough)

You will need to provide the Spark's root folder to the script, see next chapter.

## Run it

1. Do call ``` mvn package ``` from the root folder of this project
2. Find the created archive ``` meetup-experiment-1.0.0-bin.zip ``` and extract contents to your desired location.
3. Inside the extracted folder call ``` ./run.sh <your spark home path> ``` (see printed message when not providing parameters for an example)

At the end of the Spark programm it will display the resulting trending 10 topics that were found in the data of this one day.

## Details on Implementation

1.I have implemented this case study on Databricks community edition cluster for Demo purpose since it has handy notebook api for Spark where you can focus more on logic and the output at each step rather than cluster administration

The source file [src/main/scala/com/reuthlinger/meetup/MeetupScript.scala](src/main/scala/com/reuthlinger/meetup/MeetupScript.scala) contains the step wise process I applied using Spark's shell for iteratively working with Spark.
The implementation for running via spark-submit is provided by [src/main/scala/com/reuthlinger/meetup/MeetupExperiment.scala](src/main/scala/com/reuthlinger/meetup/MeetupExperiment.scala).

During iteratively working with the data I found (and documented in the script) that the group's topics are very unclearly set by the groups themselves. So it might happen when just taking the defined topic's text, we cannot see that similar topics might be trending, but are defined differently.
Because of this I did choose to not use the topic directly but to go with some NLP approach, trying to extract the valuable information from each RSVP. But I need to say that I am new to using NLP (besides heard theory about different things). So I maybe didn't come up with the most sophisticated approach using the NLP functions to the max.
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
So, e.g. "New", "Group", "Town" and "Meetup" could be excluded, as they don't show any useful direction of the topic.
I was also playing around a bit with NGram, but since I am not experienced in using all this, I found it hard to get to reasonable results.


Something I tried to add the calculating the value of trending topics: I guess the usage of bringing some guests (well, it is not used much on the platform) could make a better value.
Bug also when someone was signing off (response = no), I  took this into account lowering the value by this.


In general I need to admit that having better values for the group topics, or even some labels for each event might help a lot providing better results.
Also the event's text description could be used with the NLP (and using more advanced functions of it) to extract the real background of the event to get maybe even more useful information then the topic sometimes provides.


## Improvements and extensions

Because of lack of time doing more nice features inside this implementation, I have choosen to not implement:
- Directly attacking to live API from Meetup.com
- Using a window function, since it would not make much sense for the smaller amount of data of the batch for a timeframe of a day. Maybe selecting an hour would make sense, but I didn't try if the results might be valueable.
- I didn't go for maximizing spell checking and avoidance of any other weird symbols that are not filtered by the NLP functions. There could still be improvements, but those would not influence the top topics much.
- Geo-taging and filtering (I didn't work with something like that much)
- I also didn't go for unit tests, but since this implementation uses mostly simpler functions there is not much to go wrong. When using unit tests, I would consider writing a data generator, like spark-testing-base project of Holden Karau would suggest, but this is also very time consuming to get it done and from my point of view not valueable for this short implementation.

But I am happy to discuss further things, if you are interested.
