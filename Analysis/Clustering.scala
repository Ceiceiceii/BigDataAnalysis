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

val cleanedData = temp.read.format("csv").option("header", "true").load("hdfs:///user/yz5835_nyu_edu/proj/cleanedData.csv")

Seq("AGE_GROUP", "PERP_SEX", "PERP_RACE", "ARREST_BORO","Community Districts","LAW_CAT_CD").foreach { column =>
  cleanedData.groupBy(column).count().orderBy(desc("count")).show()
}


val dataWithYear = cleanedData.withColumn("ARREST_YEAR", year(col("ARREST_DATE")))

Seq("LAW_CAT_CD", "PERP_RACE", "AGE_GROUP").foreach { column =>
  dataWithYear.groupBy("ARREST_YEAR", column).count()
    .orderBy(col("ARREST_YEAR"), desc("count"))
    .show()
}


dataWithYear.groupBy("Community Districts", "LAW_CAT_CD")
  .count()
  .orderBy(col("Community Districts"), desc("count"))
  .show()


val indexerOptions = Seq("AGE_GROUP", "PERP_SEX", "PERP_RACE", "ARREST_BORO", "Community Districts", "LAW_CAT_CD").map { columnName =>
  new StringIndexer()
    .setInputCol(columnName)
    .setOutputCol(columnName + "_Index")
    .setHandleInvalid("keep")
    .fit(cleanedData)
}

val indexedData = indexerOptions.foldLeft(cleanedData)((tempData, indexer) => indexer.transform(tempData))

val encoder = new OneHotEncoder()
  .setInputCols(indexerOptions.map(_.getOutputCol).toArray)
  .setOutputCols(indexerOptions.map(_.getOutputCol.replace("Index", "Vec")).toArray)

val encodedData = encoder.fit(indexedData).transform(indexedData)

val assembler = new VectorAssembler()
  .setInputCols(encoder.getOutputCols)
  .setOutputCol("features")

val finalData = assembler.transform(encodedData)


val evaluator = new ClusteringEvaluator()
val silhouetteScores = ArrayBuffer[(Int, Double)]()
val clustersRange = 2 to 10
clustersRange.foreach { k =>
  val kmeans = new KMeans().setK(k).setSeed(1L).setFeaturesCol("features")
  val model = kmeans.fit(finalData)
  val predictions = model.transform(finalData)
  val score = evaluator.evaluate(predictions)
  silhouetteScores += ((k, score))
  println(s"Silhouette score for k=$k is $score")
}



val (optimalK, optimalScore) = silhouetteScores.maxBy(_._2)
println(s"Optimal number of clusters: $optimalK with silhouette score: $optimalScore")


val optimalKMeans = new KMeans().setK(optimalK).setSeed(1L).setFeaturesCol("features")
val optimalModel = optimalKMeans.fit(finalData)
val optimalPredictions = optimalModel.transform(finalData).withColumnRenamed("prediction", "cluster")


def analyzeCluster(data: DataFrame, groupCols: Seq[String]): Unit = {
  groupCols.foreach { col =>
    data.groupBy("cluster", col)
      .count()
      .orderBy(desc("count")).show()
  }
}


analyzeCluster(optimalPredictions, Seq("PERP_RACE", "AGE_GROUP", "PERP_SEX", "Community Districts", "LAW_CAT_CD"))



spark.stop()
