package com.example.spark

import com.example.utils.AppConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkFactory extends LazyLogging {

  private val sparkConf = new SparkConf()
    .set("spark.sql.avro.compression.codec", "snappy")
    .set("apache.spark.executor.memory", "10g")
    .setAll(AppConfig.sparkConfig)

  private val sparkSessionBuilder = SparkSession.builder()
    .appName(AppConfig.sparkAppName)
    .config(sparkConf)
    .enableHiveSupport()

  def getSparkSession: SparkSession = {
    logger.debug("create or get spark session.")

    sparkSessionBuilder
      .getOrCreate()
  }

  def getSparkSession(master: String): SparkSession = {
    logger.debug(s"create or get spark session. master: $master" )

    sparkSessionBuilder
      .master(master)
      .getOrCreate()
  }
}
