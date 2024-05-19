import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions.desc

val spark = SparkSession.builder.appName("Crime Data Analysis").getOrCreate()



val rawData = spark.read.option("header", "true").csv("hdfs:///user/yz5835_nyu_edu/proj/ArrestData.csv")

// Select columns
val necessaryColumns = Seq("ARREST_DATE", "PERP_SEX", "PERP_RACE", "AGE_GROUP", "LAW_CAT_CD","PD_DESC", "PD_CD", "ARREST_BORO")
val dataWithSelectedColumns = rawData.select(necessaryColumns.map(col): _*)

// Drop rows with NULL
val cleanedData = dataWithSelectedColumns.na.drop()



// Convert ARREST_DATE from String to Date
val dataWithDate = cleanedData.withColumn("ARREST_DATE", to_date(col("ARREST_DATE"), "MM/dd/yyyy"))

// Check for null
// dataWithDate.filter(col("ARREST_DATE").isNull).show()

// Extract year and month
val dataWithMonthYear = dataWithDate.withColumn("YEAR_MONTH", date_format(col("ARREST_DATE"), "yyyy-MM"))

// Group by YEAR_MONTH and LAW_CAT_CD
val monthlyCrimeStats = dataWithMonthYear.groupBy("YEAR_MONTH", "LAW_CAT_CD")
  .agg(count("LAW_CAT_CD").alias("COUNT"))
  .groupBy("YEAR_MONTH")
  .pivot("LAW_CAT_CD", Seq("F", "M", "V"))
  .sum("COUNT")
  .withColumn("TOTAL", col("F") + col("M") + col("V")) 

// Show monthly result
monthlyCrimeStats.orderBy("YEAR_MONTH").show()



// Convert month to season
val monthToSeason = udf((month: Int) => month match {
  case 12 | 1 | 2 => "Winter"
  case 3 | 4 | 5 => "Spring"
  case 6 | 7 | 8 => "Summer"
  case 9 | 10 | 11 => "Autumn"
})

val dataWithSeason = dataWithDate.withColumn("MONTH", month(col("ARREST_DATE")))
                                 .withColumn("SEASON", monthToSeason(col("MONTH")))

val seasonalCrimeStats = dataWithSeason.groupBy("SEASON", "LAW_CAT_CD")
  .agg(count("LAW_CAT_CD").alias("COUNT"))
  .groupBy("SEASON")
  .pivot("LAW_CAT_CD", Seq("F", "M", "V"))
  .sum("COUNT")
  .withColumn("TOTAL", col("F") + col("M") + col("V"))  

seasonalCrimeStats.orderBy("SEASON").show()





val monthlyCrimeStatsWithPercentage = monthlyCrimeStats
  .withColumn("PERCENT_F", round(col("F") / col("TOTAL") * 100, 2))
  .withColumn("PERCENT_M", round(col("M") / col("TOTAL") * 100, 2))
  .withColumn("PERCENT_V", round(col("V") / col("TOTAL") * 100, 2))

monthlyCrimeStatsWithPercentage.select("YEAR_MONTH", "PERCENT_F", "PERCENT_M", "PERCENT_V").orderBy("YEAR_MONTH").show()




val seasonalCrimeStatsWithPercentage = seasonalCrimeStats
  .withColumn("PERCENT_F", round(col("F") / col("TOTAL") * 100, 2))
  .withColumn("PERCENT_M", round(col("M") / col("TOTAL") * 100, 2))
  .withColumn("PERCENT_V", round(col("V") / col("TOTAL") * 100, 2))

seasonalCrimeStatsWithPercentage.select("SEASON", "PERCENT_F", "PERCENT_M", "PERCENT_V").orderBy("SEASON").show()




// Classify each date as "Weekend" or "Weekday"
val isWeekend = udf((dayOfWeek: Int) => dayOfWeek match {
  case 6 | 7 => "Weekend"  
  case _ => "Weekday"
})

val dataWithWeekday = dataWithDate
  .withColumn("DAY_OF_WEEK", dayofweek(col("ARREST_DATE")))
  .withColumn("WEEKDAY", isWeekend(col("DAY_OF_WEEK")))

val weekdayCrimeStats = dataWithWeekday.groupBy("WEEKDAY", "LAW_CAT_CD")
  .agg(count("LAW_CAT_CD").alias("COUNT"))
  .groupBy("WEEKDAY")
  .pivot("LAW_CAT_CD", Seq("F", "M", "V"))
  .sum("COUNT")
  .withColumn("TOTAL", col("F") + col("M") + col("V"))

weekdayCrimeStats.orderBy("WEEKDAY").show()



// Calculate average crimes per day
val averageCrimeStats = weekdayCrimeStats.withColumn("AVG_F", round(col("F") / when(col("WEEKDAY") === "Weekend", 2).otherwise(5), 2))
  .withColumn("AVG_M", round(col("M") / when(col("WEEKDAY") === "Weekend", 2).otherwise(5), 2))
  .withColumn("AVG_V", round(col("V") / when(col("WEEKDAY") === "Weekend", 2).otherwise(5), 2))
  .withColumn("AVG_TOTAL", round(col("TOTAL") / when(col("WEEKDAY") === "Weekend", 2).otherwise(5), 2))
  .select("WEEKDAY", "AVG_F", "AVG_M", "AVG_V", "AVG_TOTAL")

averageCrimeStats.orderBy("WEEKDAY").show()
