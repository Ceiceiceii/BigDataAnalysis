import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


val spark = SparkSession.builder().appName("Crime Data Analysis").getOrCreate()


val rawData = spark.read.option("header", "true").csv("hdfs:///user/yz5835_nyu_edu/proj/cleanedData.csv")


val dataWithDate = rawData.withColumn("ARREST_DATE", to_date(col("ARREST_DATE"), "MM/dd/yyyy"))
                          .withColumn("YEAR", year(col("ARREST_DATE")))


val selectedColumns = Seq("YEAR", "ARREST_BORO", "PERP_RACE", "Community Districts")
val data = dataWithDate.select(selectedColumns.map(col): _*)


val cleanedData = data.na.drop()


val yearWindow = Window.partitionBy("YEAR").orderBy(col("count").desc)


val distributionByDistrictRace = cleanedData.groupBy("YEAR", "Community Districts", "PERP_RACE")
                                            .count()
                                            .withColumn("rank", rank().over(yearWindow))
                                            .filter(col("rank") <= 3)
                                            .orderBy("YEAR", "rank")

distributionByDistrictRace.show()


val distributionByBoroDistrict = cleanedData.groupBy("YEAR", "ARREST_BORO", "Community Districts")
                                            .count()
                                            .withColumn("rank", rank().over(yearWindow))
                                            .filter(col("rank") <= 3)
                                            .orderBy("YEAR", "rank")

distributionByBoroDistrict.show()

// Stop Spark session
spark.stop()
