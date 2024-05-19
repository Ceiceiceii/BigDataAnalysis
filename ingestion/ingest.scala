import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
        .appName("Unified Data Ingestion")
        .config("spark.master", "yarn")
        .getOrCreate()

val temp = spark

val rawData1 = temp.read
        .option("header", "true")
        .csv("hdfs:///user/yz5835_nyu_edu/proj/ArrestData.csv")

val necessaryColumns1 = Seq("ARREST_DATE", "PERP_SEX", "PERP_RACE", "AGE_GROUP", "LAW_CAT_CD", "PD_DESC", "PD_CD", "ARREST_BORO")
val processedData1 = rawData1.select(necessaryColumns1.map(col): _*)
        .na.drop()
        .withColumn("ARREST_DATE", to_date(col("ARREST_DATE"), "MM/dd/yyyy"))
        .filter(year(col("ARREST_DATE")) === 2021)

processedData1.write
        .option("header", "true")
        .csv("hdfs:///user/yz5835_nyu_edu/proj/cleanedArrestData2021/")

spark.stop()

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DateType

val spark = SparkSession.builder()
  .appName("Example App")
  .config("spark.master", "yarn")
  .getOrCreate()

val temp = spark

val dataframe = temp.read.format("csv").option("header", "true").load("hdfs:///user/yz5835_nyu_edu/proj/arrest.csv")

val cleanedData = dataframe.na.drop()

val processedData1 = dataframe.select(necessaryColumns1.map(col): _*)
        .na.drop()
        .withColumn("ARREST_DATE", to_date(col("ARREST_DATE"), "MM/dd/yyyy"))
        .filter(year(col("ARREST_DATE")) === 2021)

processedData1.write
  .option("header", "true")
  .csv("hdfs:///user/yz5835_nyu_edu/proj/cleanedArrestData2021")


spark.stop()
