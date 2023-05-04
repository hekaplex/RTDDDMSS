// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC # ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)  3/ GOLD table: extract the sessions
// MAGIC <div style="float:right" ><img src="https://quentin-demo-resources.s3.eu-west-3.amazonaws.com/images/sessionization/session_diagram.png" style="height: 280px; margin:0px 0px 0px 10px"/></div>
// MAGIC
// MAGIC **Scala version:** This notebook implement the same logic as [the python]($../03-Delta-session-GOLD), but using Scala. As you'll see, the function signature is slightly different as we do not receive an iterator of Pandas Dataframe, but the logic remains identical.
// MAGIC
// MAGIC ### Why is this a challenge?
// MAGIC Because we don't have any event to flag the user disconnection, detecting the end of the session is hard. After 10 minutes without any events, we want to be notified that the session has ended.
// MAGIC However, spark will only react on event, not the absence of event.
// MAGIC
// MAGIC Thanksfully, Spark Structured Streaming has the concept of timeout. 
// MAGIC
// MAGIC **We can set a 10 minutes timeout in the state engine** and be notified 10 minutes later in order to close the session
// MAGIC
// MAGIC <!-- tracking, please Collect usage data (view). Remove it to disable collection. View README for more details.  -->
// MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=4994874874414839&aip=1&t=event&ec=dbdemos&ea=VIEW&dp=%2F_dbdemos%2Fdata-engineering%2Fstreaming-sessionization%2Fscala%2F03-Delta-session-GOLD-scala&uid=6165859300088079">

// COMMAND ----------

// MAGIC %run ../_resources/00-setup-scala $reset_all_data=false

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ### Implementing the aggregation function to update our Session
// MAGIC
// MAGIC In this simple example, we'll just be counting the number of click in the session.

// COMMAND ----------

import java.sql.Timestamp

waitForTable("events") // Wait until the previous table is created to avoid error if all notebooks are started at once

//Event (from the silver table)
case class ClickEvent(user_id: String, event_id: String, event_datetime: Timestamp, event_date: Long, platform: String, action: String, uri: String)
//Session (from the gold table)
case class UserSession(
  user_id: String, 
  var click_count: Integer = 0, 
  var start_time: Timestamp = Timestamp.valueOf("9999-12-31 23:59:29"), 
  var end_time: Timestamp = new Timestamp(0L), 
  var status: String = "online"
)

// COMMAND ----------

// MAGIC %md
// MAGIC The function `updateState` will be called for each user with a list of events for this user.

// COMMAND ----------

import org.apache.spark.sql.streaming.{ GroupState, GroupStateTimeout, OutputMode }


val MaxSessionDuration = 30000

def updateState(user_id: String, events: Iterator[ClickEvent], state: GroupState[UserSession]): Iterator[UserSession] = {
  val curState = state.getOption.getOrElse { UserSession(user_id) } // get previous state or instantiate new with default
  if (state.hasTimedOut) {
    state.remove()
    Iterator(curState)
  } else {
    val updatedState = events.foldLeft(curState){ updateStateWithEvent }
    updatedState.status = "offline" // next iteration will be a timeout or restart
    state.update(updatedState)
    state.setTimeoutTimestamp(MaxSessionDuration)
    Iterator(updatedState)
  }
}

def updateStateWithEvent(state: UserSession, input: ClickEvent): UserSession = {
  state.status = "online"
  state.click_count += 1
  //Update then begining and end of our session
  if (input.event_datetime.after(state.end_time)) {
    state.end_time = input.event_datetime
  }
  if (input.event_datetime.before(state.start_time)) {
    state.start_time = input.event_datetime
  }
  state
}

val sessions = spark
  .readStream
  .format("delta")
  .table("events")  
  .as[ClickEvent]
  .groupByKey(_.user_id)
  .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.EventTimeTimeout)(updateState)
  .toDF

display(sessions)

// COMMAND ----------

// MAGIC %md
// MAGIC # Updating the session table with number of clicks and end/start time
// MAGIC
// MAGIC We want to have the session information in real time for each user. 
// MAGIC
// MAGIC To do that, we'll create a Session table. Everytime we update the state, we'll UPSERT the session information:
// MAGIC
// MAGIC - if the session doesn't exist, we add it
// MAGIC - if it exists, we update it with the new count and potential new status
// MAGIC
// MAGIC This can easily be done with a MERGE operation using Delta and calling `foreachBatch`

// COMMAND ----------

import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame

def updateSessions(df: DataFrame, epochId: Long): Unit = {
  // Create the table if it doesn't exist (we need it to be able to perform the merge)
  if (!spark.catalog.tableExists("sessions")) {
    df.limit(0).write.option("mergeSchema", "true").mode("append").saveAsTable("sessions")
  }

  DeltaTable.forName(spark, "sessions").alias("s")
    .merge(source = df.alias("u"), condition = "s.user_id = u.user_id")
    .whenMatched().updateAll()
    .whenNotMatched().insertAll()
    .execute()
}

sessions
  .writeStream
  .option("checkpointLocation", s"$cloudStoragePath/checkpoints/sessions")
  .foreachBatch(updateSessions _)
  .start()

waitForTable("sessions")

// COMMAND ----------

// MAGIC %sql SELECT * FROM sessions

// COMMAND ----------

// MAGIC %sql SELECT CAST(avg(end_time - start_time) as INT) average_session_duration FROM sessions

// COMMAND ----------

// DBTITLE 1,Stop all the streams 
stopAllStreams(sleepTime=120)

// COMMAND ----------

// MAGIC %md
// MAGIC ### We now have our sessions stream running!
// MAGIC
// MAGIC We can set the output of this streaming job to a SQL database or another queuing system.
// MAGIC
// MAGIC We'll be able to automatically detect cart abandonments in our website and send an email to our customers, our maybe just give them a call asking if they need some help! 