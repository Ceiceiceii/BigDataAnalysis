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

cleanedData.write
  .option("header", "true")
  .csv("hdfs:///user/yz5835_nyu_edu/proj/cleanedData.csv")


spark.stop()
