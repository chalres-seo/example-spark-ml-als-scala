package com.example.apps

import com.example.spark.SparkFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

object ExapleZScoreAppMain {
  private val spark = SparkFactory.getSparkSession("local[*]")
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val data:Seq[Double] = Seq(13, 1.5, 8, 132, 4, 12, 3, 11, 14, 18, 137.5, 2, 15, 5, 1, 16, 9, 17)

    val df: Dataset[Double] = spark.createDataset(data)

    val rdd: RDD[Double] = spark.sparkContext.makeRDD(data)
    val stat = rdd.stats()

    val count = stat.count
    val mean = stat.mean
    val stdev = stat.stdev
    val max = stat.max
    val min = stat.min
    val variance = stat.variance

    val statAll = (count, mean, stdev, max, min, variance)


    val pdf = df
      .withColumn("dev", $"value" - mean)
      .withColumn("stdev", lit(mean))
      .withColumn("mean", lit(mean))
      .withColumn("min", lit(min))
      .withColumn("max", lit(max))
      .withColumn("count", lit(count))
      .withColumn("z-score", ($"value" - mean) / stdev)

    val zStat = pdf.select($"z-score").rdd.map(_.getDouble(0)).stats()

    val pdfWithT = pdf
      .withColumn("z-dev", $"z-score" - zStat.mean)
      .withColumn("z-stdev", $"z-score" - zStat.stdev)
      .withColumn("z-mean", $"z-score" - zStat.mean)
      .withColumn("z-max", $"z-score" - zStat.max)
      .withColumn("z-min", $"z-score" - zStat.min)
      .withColumn("t-value", $"z-score" * 10 + 50)

    pdfWithT.show

    pdf.withColumn("abs-v", (abs($"z-score") * 10).cast("int"))
      .groupBy("abs-v").count().show

  }
}
