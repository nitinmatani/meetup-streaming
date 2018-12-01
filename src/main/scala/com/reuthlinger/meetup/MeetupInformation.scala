package com.reuthlinger.meetup

import java.util.Date

import org.joda.time.DateTime


/**
  * Basic information object containing some raw ID and time information about an RSVP (without any further
  * information).
  *
  * @param rsvpId                   ID of the RSVP for the attendee
  * @param rsvpModificationDatetime Datetime as [[Long]] Epoch timestamp what the RSVP was modified last
  * @param response                 Response (yes/no) of the attendee (if attendee is taking part or not)
  * @param guests                   Number of guests the attendee wants to bring
  */
abstract class BasicRsvpInformation(
                                     rsvpId: Long,
                                     rsvpModificationDatetime: Long,
                                     response: String,
                                     guests: Long
                                   )

/**
  * Meetup topic information object that contains an extract from the original RSVP object, where only relevant
  * text fields and the list of group's topics are extracted.
  *
  * @param rsvpId                   ID of the RSVP for the attendee
  * @param rsvpModificationDatetime Datetime as [[Long]] Epoch timestamp what the RSVP was modified last
  * @param response                 Response (yes/no) of the attendee (if attendee is taking part or not)
  * @param guests                   Number of guests the attendee wants to bring
  * @param groupName                Meetup group name
  * @param groupTopics              Array of meetup group's topics (what topics the group is about)
  * @param eventName                Name of the event this RSVP was sent for
  *
  */
case class MeetupTopicInformation(
                                   rsvpId: Long,
                                   rsvpModificationDatetime: Long,
                                   response: String,
                                   guests: Long,
                                   groupName: String,
                                   groupTopics: Array[(String, String)],
                                   eventName: String
                                 ) extends BasicRsvpInformation(rsvpId, rsvpModificationDatetime, response, guests)

/**
  * Meetup topic information object that contains all extracted textual information concatenated together for doing
  * NLP term extraction.
  *
  * @param rsvpId                   ID of the RSVP for the attendee
  * @param rsvpModificationDatetime Datetime as [[Long]] Epoch timestamp what the RSVP was modified last
  * @param response                 Response (yes/no) of the attendee (if attendee is taking part or not)
  * @param guests                   Number of guests the attendee wants to bring
  * @param wholeTextInformation     All concatenated textual information
  *
  */
case class MeetupWholeTextInformation(
                                       rsvpId: Long,
                                       rsvpModificationDatetime: Long,
                                       response: String,
                                       guests: Long,
                                       wholeTextInformation: String
                                     ) extends BasicRsvpInformation(rsvpId, rsvpModificationDatetime, response, guests)

/**
  * Meetup topic information object that contains all extracted textual information concatenated together, cleansed
  * using NLP functions for identification of trending topics.
  *
  * @param rsvpId                   ID of the RSVP for the attendee
  * @param rsvpModificationDatetime Datetime as [[Long]] Epoch timestamp what the RSVP was modified last
  * @param response                 Response (yes/no) of the attendee (if attendee is taking part or not)
  * @param guests                   Number of guests the attendee wants to bring
  * @param topics                   List of topics identified from varios texts of the RSVP object.
  *
  */
case class MeetupCleansedTopicsInformation(
                                            rsvpId: Long,
                                            rsvpModificationDatetime: Long,
                                            response: String,
                                            guests: Long,
                                            topics: List[String]
                                          )
  extends BasicRsvpInformation(rsvpId, rsvpModificationDatetime, response, guests)

/**
  * Meetup topic information object that contains all extracted textual information concatenated together, cleansed
  * using NLP functions for identification of trending topics.
  *
  * @param rsvpId                   ID of the RSVP for the attendee
  * @param rsvpModificationDatetime Datetime as [[Long]] Epoch timestamp what the RSVP was modified last
  * @param topic                    Topic (exploded) identified from varios texts of the RSVP object.
  * @param date                     The date, extracted from the RSVP modification timestamp.
  * @param value                    A calculatory value of this RSVP information, based on whether the attendee
  *                                 signed up or off in combination with the number of guests to bring
  */
case class MeetupTopicValueInformation(
                                        rsvpId: Long,
                                        rsvpModificationDatetime: Long,
                                        topic: String,
                                        date: String,
                                        value: Double
                                      )

/**
  * Meetup topic information object that contains all extracted textual information concatenated together, cleansed
  * using NLP functions for identification of trending topics.
  *
  * @param date  The date, extracted from the RSVP modification timestamp.
  * @param topic Topic identified from varios texts of the RSVP object.
  * @param value Sum of the trending topics daily value.
  */
case class TrendingTopic(
                          date: String,
                          topic: String,
                          value: Double
                        )
