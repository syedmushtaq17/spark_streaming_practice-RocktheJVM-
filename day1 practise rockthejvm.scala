// Databricks notebook source
spark

// COMMAND ----------

import org.apache.spark.sql.functions.{col,sum,avg,from_json,expr,window, row_number,count}  // import window form Functions
import org.apache.spark.sql.expressions.Window  // used inside rank functions but row_number is used form functions 
// in Python  ==>  from pyspark.sql.window import Window
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StructType, StructField, StringType, TimestampType}



// COMMAND ----------

"userId":"rirani",

"jobTitleName":"Developer",

"firstName":"Romin",

"lastName":"Irani",

"preferredFullName":"Romin Irani",

"employeeCode":"E1",

"region":"CA",

"phoneNumber":"408-1234567",

"emailAddress":"romin.k.irani@gmail.com",
"Time": "2022-11-01T11:01:28.107"

// COMMAND ----------

val empSchema = new StructType()
                  .add(StructField("userId",StringType))
                  .add(StructField("jobTitleName",StringType))
                  .add(StructField("firstName",StringType))
                  .add(StructField("lastName",StringType))
                  .add(StructField("preferredFullName",StringType))
                  .add(StructField("employeeCode",StringType))
                  .add(StructField("region",StringType))
                  .add(StructField("phoneNumber",StringType))
                  .add(StructField("emailAddress",StringType))
                  .add(StructField("Time",TimestampType))
                  

// COMMAND ----------

val empSchema2 = new StructType(Array(
StructField("userId",StringType)
                  ,StructField("jobTitleName",StringType)
                  ,StructField("firstName",StringType)
                  ,StructField("lastName",StringType)
                  ,StructField("preferredFullName",StringType)
                  ,StructField("employeeCode",StringType)
                  ,StructField("region",StringType)
                  ,StructField("phoneNumber",StringType)
                  ,StructField("emailAddress",StringType)
                  ,StructField("Time",TimestampType)


))
                  

val empSchema3 = new StructType(Array(
StructField("userId",StringType)                  
                  ,StructField("Time",TimestampType)


))
                  
                  
                  

// COMMAND ----------

val InpDf = spark.readStream.format("socket").option("host","localhost").option("port",12345).load()

// COMMAND ----------

InpDf.writeStream.trigger(Trigger.ProcessingTime("2 seconds") ).format("console").outputMode("update").start().awaitTermination()
println("Syed")


// COMMAND ----------

InpDf.selectExpr("count(value) as count_value").writeStream.format("console").outputMode("complete").start().awaitTermination()
println("Syed")

// COMMAND ----------

InpDf.writeStream.trigger(Trigger.ProcessingTime("2 seconds") ).format("console").outputMode("update").start().awaitTermination()
println("Syed")

// COMMAND ----------

val names = InpDf.select(col("value").as("number")).groupByKey(l => l).agg(avg("number"), sum("number"))

// COMMAND ----------

names.writeStream.format("console").outputMode("complete").start().awaitTermination()

// COMMAND ----------

val empStreamDf = spark.readStream.format("socket").option("host","localhost").option("port",12345).load()
                  .select(col("value"),from_json(col("value"), empSchema3).as("act_val"))
                  .select(expr("act_val.*"))

// COMMAND ----------

empStreamDf.select(expr("count(1)")).writeStream.trigger(Trigger.ProcessingTime("1 seconds") ).format("console").outputMode("update").start().awaitTermination()
// Update and complete mode works for count(* ) but not 

println("Syed")

// COMMAND ----------

empStreamDf.select(expr("count(*)")).writeStream.trigger(Trigger.ProcessingTime("1 seconds") ).format("console").outputMode("update").start().awaitTermination()
println("Syed")

// COMMAND ----------

// Aggregating Query  Tumble Window working Fine for evey 5 mins window base don passing records we are getting count
/* {"userId":"rirani","Time":"2022-11-01T11:01:28.107"}
{"userId":"thanks","Time":"2022-11-01T11:09:28.107"}
{"userId":"nirani","Time":"2022-11-01T11:03:28.107"}
*/
empStreamDf.groupBy(window(col("Time"), "5 minutes").as("empWindow")).agg(count("userId").as("cnt_users"))
.selectExpr(
  "empWindow.start as start ", "empWindow.end as end","cnt_users"
)

.writeStream.trigger(Trigger.ProcessingTime("1 seconds") ).format("console").outputMode("update").start().awaitTermination()
// Update and complete mode works for count(* ) but not 

println("Syed")

// COMMAND ----------

{"userId":"rirani","Time":"2022-11-01T11:01:28.107"}
{"userId":"thanks","Time":"2022-11-01T11:09:28.107"}
{"userId":"nirani","Time":"2022-11-01T11:03:28.107"}



{"userId":"rirani","Time":"2022-11-01T11:01:28.107"}
{"userId":"nirani","Time":"2022-11-01T11:03:28.107"}



{"userId":"thanks","Time":"2022-11-01T11:29:28.107"}





// COMMAND ----------

// Aggregating Query  Sliding Window working Fine for evey 5 mins window base done passing records we are getting Agg count
/* {"userId":"rirani","Time":"2022-11-01T11:01:28.107"}
{"userId":"thanks","Time":"2022-11-01T11:09:28.107"}
{"userId":"nirani","Time":"2022-11-01T11:03:28.107"}
*/
empStreamDf.groupBy(window(col("Time"), "5 minutes","1 minutes" ).as("empWindow")).agg(count("userId").as("cnt_users"))
.selectExpr(
  "empWindow.start as start ", "empWindow.end as end","cnt_users"
) // .orderBy(col("start").asc, col("end").asc)

.writeStream.trigger(Trigger.ProcessingTime("1 seconds") ).format("console").outputMode("update").start().awaitTermination()
// Update and complete mode works for count(* ) but not 

println("Syed")

// COMMAND ----------




Practical observations

empStreamDf.groupBy(window(col("Time"), "5 minutes","1 minute" ).as("empWindow")).agg(count("userId").as("cnt_users"))
.selectExpr(
  "empWindow.start as start ", "empWindow.end as end","cnt_users"
).orderBy(col("start").asc, col("end").asc)


For user {"userId":"rirani","Time":"2022-11-01T11:01:28.107"}

So technically 
Starting from rounding to nearest next sliding interval duration which is 2 minutes in this case and going back to 5 mins previous 10.57 
Increasing to every minute (sliding interval) uptill the last 5 minute window which can have the above data which is 11.01 to 11.06
Cause 11.02 - 11.07 cannot contain this data so only till this is the end point


+-------------------+-------------------+---------+
|              start|                end|cnt_users|
+-------------------+-------------------+---------+
|2022-11-01 10:57:00|2022-11-01 11:02:00|        1|
|2022-11-01 10:58:00|2022-11-01 11:03:00|        1|
|2022-11-01 10:59:00|2022-11-01 11:04:00|        1|
|2022-11-01 11:00:00|2022-11-01 11:05:00|        1|
|2022-11-01 11:01:00|2022-11-01 11:06:00|        1|
+-------------------+-------------------+---------+


For same above query window. == > window(col("Time"), "3 minutes","30 seconds" )
Rounding to nearest 30 seconds which is 11.01.30 and 3 minutes back 10.58.30 
Uphill 30 seconds increment
Start ==> 58.30 to 59.00 then 59.30…..last window which can include this one is rounding 11.01.28 to floor (prev) sliding window == 11.01.00

+-------------------+-------------------+---------+
|              start|                end|cnt_users|
+-------------------+-------------------+---------+
|2022-11-01 10:58:30|2022-11-01 11:01:30|        1|
|2022-11-01 10:59:00|2022-11-01 11:02:00|        1|
|2022-11-01 10:59:30|2022-11-01 11:02:30|        1|
|2022-11-01 11:00:00|2022-11-01 11:03:00|        1|
|2022-11-01 11:00:30|2022-11-01 11:03:30|        1|
|2022-11-01 11:01:00|2022-11-01 11:04:00|        1|
+-------------------+-------------------+---------+



Window 5 minutes and 1 minute sliding 

Complete mode

+-------------------+-------------------+---------+
|              start|                end|cnt_users|
+-------------------+-------------------+---------+
|2022-11-01 10:57:00|2022-11-01 11:02:00|        1|
|2022-11-01 10:58:00|2022-11-01 11:03:00|        1|
|2022-11-01 10:59:00|2022-11-01 11:04:00|        2|
|2022-11-01 11:00:00|2022-11-01 11:05:00|        2|
|2022-11-01 11:01:00|2022-11-01 11:06:00|        2|
|2022-11-01 11:02:00|2022-11-01 11:07:00|        1|
|2022-11-01 11:03:00|2022-11-01 11:08:00|        1|
|2022-11-01 11:05:00|2022-11-01 11:10:00|        1|
|2022-11-01 11:06:00|2022-11-01 11:11:00|        1|
|2022-11-01 11:07:00|2022-11-01 11:12:00|        1|
|2022-11-01 11:08:00|2022-11-01 11:13:00|        1|
|2022-11-01 11:09:00|2022-11-01 11:14:00|        1|
|2022-11-01 11:25:00|2022-11-01 11:30:00|        1|. ==> here after 11.09 we have 11.25 as in-between there is no intervals so just getting data which is in between intervals if
|2022-11-01 11:26:00|2022-11-01 11:31:00|        1|.  There is no count or no interval then it is not considered.
|2022-11-01 11:27:00|2022-11-01 11:32:00|        1|
|2022-11-01 11:28:00|2022-11-01 11:33:00|        1|
|2022-11-01 11:29:00|2022-11-01 11:34:00|        1|


{"userId":"rirani","Time":"2022-11-01T11:01:28.107"}
{"userId":"thanks","Time":"2022-11-01T11:09:28.107"}
{"userId":"nirani","Time":"2022-11-01T11:03:28.107"}


Also it doesn’t matter if the event time isn’t sorted it works the same way for sorted and. Unsorted data 

NOTE::  .orderBy(col("start").asc, col("end").asc)
Sorting is not supported on streaming DataFrames/Datasets, unless it is on aggregated DataFrame/Dataset in Complete output mode;

Since data keeps on changing in update mode and we cannot have fixed sort 

In complete Mode if the start and end intervals match then even data from previous batch will be aggregated and will be shown as total value inside cnt_users, but in append mode, only new data and its batch and agg count is shown, in complete mode even if previous batch All the non-matching windows and matching updated (prev+curr ) agg values.


Append Mode : Just current batch data’s window Range and Aggeragation of Current data result only
Update Mode : just current batch data’s window Range  but if some Window Range is matching with previous batches window then Agg= current data agg + prev data agg for the same window
Complete Mode: All previous data from beginning batch and if any Window is matching then Agg its count  with the Matching Window from current data

So complete mode ~~= Update + Append (need removing existing windows from append mode + update mode)



UPDATE MODE

First {"userId":"rirani","Time":"2022-11-01T11:01:28.107"}
+-------------------+-------------------+---------+
|              start|                end|cnt_users|
+-------------------+-------------------+---------+
|2022-11-01 10:59:00|2022-11-01 11:04:00|        1|
|2022-11-01 11:01:00|2022-11-01 11:06:00|        1|
|2022-11-01 10:58:00|2022-11-01 11:03:00|        1|
|2022-11-01 10:57:00|2022-11-01 11:02:00|        1|
|2022-11-01 11:00:00|2022-11-01 11:05:00|        1|
+-------------------+-------------------+---------+

-------------------------------------------
Batch: 2
-------------------------------------------
When added   {"userId":"nirani","Time":"2022-11-01T11:03:28.107”}. To above update data query we can see

For this the range starts from 2022-11-01 10:59:00 and if any previous batches has data in same window then it gets agg
And gave result as 2
As the highlighted window are present in both the batches so only those window from previous window taken but final window range will depends on only current batches Time range
Means  2022-11-01T11:03:28. So 10.59 t0 11.05 only can be the batches. And matching windows from previous batches is taken



+-------------------+-------------------+---------+
|              start|                end|cnt_users|
+-------------------+-------------------+---------+
|2022-11-01 10:59:00|2022-11-01 11:04:00|        2|
|2022-11-01 11:02:00|2022-11-01 11:07:00|        1|
|2022-11-01 11:01:00|2022-11-01 11:06:00|        2|
|2022-11-01 11:03:00|2022-11-01 11:08:00|        1|
|2022-11-01 11:00:00|2022-11-01 11:05:00|        2|





// COMMAND ----------


