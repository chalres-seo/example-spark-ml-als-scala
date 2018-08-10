package com.example.utils

import java.io.{File, FileInputStream}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.collection.mutable

object AppConfig extends LazyLogging {
  // read application.conf
  private val conf: Config = ConfigFactory.parseFile(new File("conf/application.conf")).resolve()

  // read spark.conf
  private val sparkProps: Properties = new Properties()
  sparkProps.load(new FileInputStream("conf/spark.conf"))

  // spark config
  val sparkAppName: String = conf.getString("spark.appName")
  val sparkConfig: mutable.Map[String, String] = sparkProps.asScala
  val sparkPalleleLevel: Int = Runtime.getRuntime.availableProcessors() * 2
}