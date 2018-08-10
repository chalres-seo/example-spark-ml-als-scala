package com.example.apps

import com.example.spark.SparkFactory
import com.typesafe.scalalogging.LazyLogging

object AppMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("app main")

    val spark = SparkFactory.getSparkSession
  }
}
