package com.example.apps.reommand

import com.example.spark.SparkFactory
import com.example.utils.AppConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

object ArtistData extends LazyLogging {
  private val spark = SparkFactory.getSparkSession
  private val sc = spark.sparkContext
  import spark.implicits._

  // tap delimiter text file. user_id, artist_id, play_count
  private val userArtistDataPath = "tmp/sample_data/user_artist_data.txt"
  // tap delimiter text file. artist_id, artist_name
  private val artistDataPath = "tmp/sample_data/artist_data.txt"
  // tap delimiter text file. bad_artist_id, good_artist_id
  private val artistAliasPath = "tmp/sample_data/artist_alias.txt"

  private val rawArtistData = sc.textFile(artistDataPath, AppConfig.sparkPalleleLevel)
  private val rawArtistAlias = sc.textFile(artistAliasPath, AppConfig.sparkPalleleLevel)
  private val rawUserArtistData = sc.textFile(userArtistDataPath, AppConfig.sparkPalleleLevel)

  private val broadCastedArtistAlias: Broadcast[collection.Map[Int, Int]] = sc.broadcast(this.getArtistAliasInfo)

  def getArtistName(artistID: Int): Set[String] = {
    this.getArtistByID.lookup(artistID).toSet
  }

  def getArtistNames(artistIDList: Set[Int]): Array[String] = {
    this.getArtistByID.filter {
      case (id, _) =>
        artistIDList.contains(id)
    }.values.collect()
  }

  def getArtistForUser(searchUserID: Int): Array[String] = {
    val existArtistSet = this.rawUserArtistData
      .map(_.split(' '))
      .filter {
        case Array(user, _, _) =>
          user.toInt == searchUserID
      }
      .map {
        case Array(_, artist, _) =>
          artist.toInt
      }.collect().toSet

    this.getArtistNames(existArtistSet)
  }

  def getBroadCastedArtistAliasInfo: Broadcast[collection.Map[Int, Int]] = broadCastedArtistAlias

  def getArtistAliasInfo: collection.Map[Int, Int] = {
    this.rawArtistAlias
      .flatMap { line =>
        val tokens = line.split("\t")
        if (tokens(0).isEmpty) {
          None
        } else {
          Some((tokens(0).toInt, tokens(1).toInt))
        }
      }.collectAsMap()
  }

  def getArtistByID: RDD[(Int, String)] = {
    rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case e: NumberFormatException =>
            logger.warn(s"failed parse artist id. skipped line: $line")
            None
        }
      }
    }
  }

  def getRawUserArtistData: RDD[String] = this.rawUserArtistData

  def showAllData():Unit = {
    spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", " ")
      .csv(userArtistDataPath)
      .withColumnRenamed("_c0", "user_id")
      .withColumnRenamed("_c1", "artist_id")
      .withColumnRenamed("_c2", "play_count")
      .repartition(AppConfig.sparkPalleleLevel)
      .show
    logger.info(userArtistDataPath)

    spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", "\t")
      .csv(artistDataPath)
      .withColumnRenamed("_c0", "artist_id")
      .withColumnRenamed("_c1", "artist_name")
      .repartition(AppConfig.sparkPalleleLevel)
      .show
    logger.info(artistDataPath)

    spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", "\t")
      .csv(artistAliasPath)
      .withColumnRenamed("_c0", "bad_artist_id")
      .withColumnRenamed("_c1", "good_artist_id")
      .repartition(AppConfig.sparkPalleleLevel)
      .show
    logger.info(artistAliasPath)
  }
}
