package com.example.spark.sql

import com.example.spark.SparkFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.annotation.tailrec

import com.databricks.spark.avro._

object DataFrameUtils extends LazyLogging {
  private val spark: SparkSession = SparkFactory.getSparkSession

  def trimColumnName(df: DataFrame): DataFrame = {
    this.replaceAllColumnName(df)(_.replace(" ", ""))
  }

  def replaceAllColumnName(df: DataFrame)(replaceRule: String => String): DataFrame = {
    val columnNameList = df.schema.fields.map(_.name).toVector

    @tailrec
    def loop(df: DataFrame, counter: Int): DataFrame = {
      if (counter < 0) {
        df
      } else {
        val currentColumnName = columnNameList(counter)
        val newColumnName = replaceRule(currentColumnName)
        loop(df.withColumnRenamed(currentColumnName, newColumnName), counter - 1)
      }
    }

    loop(df, columnNameList.length - 1)
  }

  def saveOrc(df: DataFrame, path: String): Unit = {
    logger.debug(s"save to orc. path: ${path}")
    df.write
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .orc(path)
  }

  def saveAvro(df: DataFrame, path: String): Unit = {
    logger.debug(s"save to avro. path: ${path}")

    df.write
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .avro(path)
  }

  def saveParquet(df: DataFrame, path: String): Unit = {
    logger.debug(s"save to parquet. path: ${path}")

    df.write
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .parquet(path)
  }

  def saveJson(df: DataFrame, path: String): Unit = {
    logger.debug(s"save to json. path: ${path}")

    df.write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .json(path)
  }

  def registerTempTable(df: DataFrame, tempTableName: String): Unit = {
    df.createOrReplaceTempView(tempTableName)
  }
}
