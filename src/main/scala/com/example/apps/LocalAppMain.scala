package com.example.apps

import breeze.linalg.rank
import com.example.apps.reommand.{ArtistData, RecommandArtist, Validation}
import com.example.spark.SparkFactory
import com.example.utils.AppConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

import scala.io.StdIn
import scala.util.Try

object LocalAppMain extends LazyLogging {
  private val spark = SparkFactory.getSparkSession("local[*]")
  private val sc = spark.sparkContext
  import spark.implicits._

  private val sparkUIUrl: Option[String] = spark.sparkContext.uiWebUrl

  private val modelPath = "tmp/model/recommend_artist"

  def main(args: Array[String]): Unit = {
    logger.info("Start LocalAppMain.")
    logger.info(s"Spark Web UI: ${sparkUIUrl.getOrElse("null")}")

    this.saveModel(modelPath)

    val model = Try(
      MatrixFactorizationModel.load(sc, modelPath)
    ).getOrElse(MatrixFactorizationModel.load(sc, modelPath))

    val users = RecommandArtist.getTrainData.map(_.user).distinct().take(100)
    val recommendations = users.map(model.recommendProducts(_, 5))

    val recommendationsInfo = recommendations.map(recommendation =>
      recommendation.head.user + " -> " + recommendation.map(_.product).mkString(",")
    )

    recommendationsInfo.foreach(println)

    // hold on for debug web UI
    this.waitForDebugWebUI()
  }

  def saveModel(path: String): MatrixFactorizationModel = {
    val allData = RecommandArtist.getTrainData
    val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))

    trainData.cache()
    cvData.cache()

    val allItemIDs = allData.map(_.product).distinct().collect()
    val bAllItemIDs = sc.broadcast(allItemIDs)


    val model: MatrixFactorizationModel = ALS.trainImplicit(trainData, 50, 5, 1.0, 40.0)

    model.save(sc, path)

    val auc: Double = Validation.areaUnderCurve(cvData, bAllItemIDs, model.predict)
    val aucNotModeling: Double = Validation.areaUnderCurve(cvData, bAllItemIDs, Validation.predictMostListened(trainData))

    logger.info(s"modeling auc: $auc)")
    logger.info(s"not modeling auc: $aucNotModeling)")
    model
  }

  def findParameter(): Unit = {
    //    default parameter rank: 10, lambda: 0,01, alpha: 1.0
    //    ((50,1.0,40.0),0.9774368615460968)
    //    ((10,1.0,40.0),0.9769954264990749)
    //    ((50,1.0E-4,40.0),0.9764532961819038)
    //    ((10,1.0E-4,40.0),0.9764173537965675)
    //    ((10,1.0,1.0),0.9691622476674887)
    //    ((50,1.0,1.0),0.9671921790113309)
    //    ((10,1.0E-4,1.0),0.9637633354075307)
    //    ((50,1.0E-4,1.0),0.9538044250579388)

    Validation.evaluations
      .sortBy(_._2)
      .reverse
      .foreach { result =>
        val ((rank, lambda, alpha), auc) = result
        println(s"rank: $rank, lambda: $lambda, alpha: $alpha, auc: $auc")
      }
  }

  def simpleAUCValidation(): Unit = {
    val allData = RecommandArtist.getTrainData
    val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))

    trainData.cache()
    cvData.cache()

    val allItemIDs = allData.map(_.product).distinct().collect()
    val bAllItemIDs = sc.broadcast(allItemIDs)


    val model: MatrixFactorizationModel = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)

    println(model.userFeatures.getNumPartitions)
    println(model.productFeatures.getNumPartitions)

    val auc: Double = Validation.areaUnderCurve(cvData, bAllItemIDs, model.predict)
    val aucNotModeling: Double = Validation.areaUnderCurve(cvData, bAllItemIDs, Validation.predictMostListened(trainData))

    println(s"modeling auc: $auc)")
    println(s"not modeling auc: $aucNotModeling)")
  }

  private def waitForDebugWebUI(): Unit = {
    println
    println(s">>> Hold on for debug web UI (${sparkUIUrl.getOrElse("null")})")
    println(">>> Press ENTER to exit <<<")
    try StdIn.readLine()
    finally System.exit(0)
  }
}
