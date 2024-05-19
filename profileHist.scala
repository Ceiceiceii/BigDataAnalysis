import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DateType

val spark = SparkSession.builder()
  .appName("NYPD Arrest Data Profiling")
  .config("spark.master", "yarn")
  .getOrCreate()

// Load data
val dataframe = spark.read.format("csv").option("header", "true").load("hdfs:///user/yz5835_nyu_edu/proj/arrest.csv")

// Convert ARREST_DATE to date type and extract year and month
val dfWithDate = dataframe.withColumn("ARREST_DATE", to_date(col("ARREST_DATE"), "MM/dd/yyyy"))
  .withColumn("ARREST_YEAR", year(col("ARREST_DATE")))
  .withColumn("ARREST_MONTH", month(col("ARREST_DATE")))

// Analyze distribution of arrests by borough
val boroughDistribution = dfWithDate.groupBy("ARREST_BORO")
  .count()
  .orderBy(desc("count"))
boroughDistribution.show()

// Analyze distribution of arrests by age group, sex, race, and crime category
Seq("AGE_GROUP", "PERP_SEX", "PERP_RACE", "ARREST_BORO", "Community Districts", "LAW_CAT_CD").foreach { column =>
  dfWithDate.groupBy(column).count().orderBy(desc("count")).show()
}

// Analyze arrest trends over time by race
val raceTrends = dfWithDate.groupBy("ARREST_YEAR", "PERP_RACE")
  .count()
  .orderBy("ARREST_YEAR", "PERP_RACE")
raceTrends.show()

// Analyze arrest distribution by community district and race
val communityRaceDistribution = dfWithDate.groupBy("Community Districts", "PERP_RACE")
  .count()
  .orderBy(desc("count"))
communityRaceDistribution.show()

// Stop Spark session
spark.stop()
