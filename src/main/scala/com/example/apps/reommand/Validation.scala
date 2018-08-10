package com.example.apps.reommand

import java.util.Random

import com.example.apps.LocalAppMain.sc
import com.example.spark.SparkFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object Validation {
  private val spark = SparkFactory.getSparkSession
  private val sc = spark.sparkContext
  import spark.implicits._

  // Imported
  def areaUnderCurve(positiveData: RDD[Rating],
                     bAllItemIDs: Broadcast[Array[Int]],
                     predictFunction: RDD[(Int, Int)] => RDD[Rating]): Double = {
    // What this actually computes is AUC, per user. The result is actually something
    // that might be called "mean AUC".

    // Take held-out data as the "positive", and map to tuples
    val positiveUserProducts = positiveData.map(r => (r.user, r.product))
    // Make predictions for each of them, including a numeric score, and gather by user
    val positivePredictions = predictFunction(positiveUserProducts).groupBy(_.user)

    // BinaryClassificationMetrics.areaUnderROC is not used here since there are really lots of
    // small AUC problems, and it would be inefficient, when a direct computation is available.

    // Create a set of "negative" products for each user. These are randomly chosen
    // from among all of the other items, excluding those that are "positive" for the user.
    val negativeUserProducts = positiveUserProducts.groupByKey().mapPartitions {
      // mapPartitions operates on many (user,positive-items) pairs at once
      userIDAndPosItemIDs => {
        // Init an RNG and the item IDs set once for partition
        val random = new Random()
        val allItemIDs = bAllItemIDs.value
        userIDAndPosItemIDs.map { case (userID, posItemIDs) =>
          val posItemIDSet = posItemIDs.toSet
          val negative = new ArrayBuffer[Int]()
          var i = 0
          // Keep about as many negative examples per user as positive.
          // Duplicates are OK
          while (i < allItemIDs.length && negative.size < posItemIDSet.size) {
            val itemID = allItemIDs(random.nextInt(allItemIDs.length))
            if (!posItemIDSet.contains(itemID)) {
              negative += itemID
            }
            i += 1
          }
          // Result is a collection of (user,negative-item) tuples
          negative.map(itemID => (userID, itemID))
        }
      }
    }.flatMap(t => t)
    // flatMap breaks the collections above down into one big set of tuples

    // Make predictions on the rest:
    val negativePredictions = predictFunction(negativeUserProducts).groupBy(_.user)

    // Join positive and negative by user
    positivePredictions.join(negativePredictions).values.map {
      case (positiveRatings, negativeRatings) =>
        // AUC may be viewed as the probability that a random positive item scores
        // higher than a random negative one. Here the proportion of all positive-negative
        // pairs that are correctly ranked is computed. The result is equal to the AUC metric.
        var correct = 0L
        var total = 0L
        // For each pairing,
        for (positive <- positiveRatings;
             negative <- negativeRatings) {
          // Count the correctly-ranked pairs
          if (positive.rating > negative.rating) {
            correct += 1
          }
          total += 1
        }
        // Return AUC: fraction of pairs ranked correctly
        correct.toDouble / total
    }.mean() // Return mean AUC over users
  }

  def predictMostListened(train: RDD[Rating])(allData: RDD[(Int, Int)]): RDD[Rating] = {
    val bListenCount: Broadcast[collection.Map[Int, Double]] = sc.broadcast(
      train.map(r => (r.product, r.rating)).reduceByKey(_ + _).collectAsMap()
    )

    allData.map {
      case (user, product) =>
        Rating(user, product, bListenCount.value.getOrElse(product, 0.0))
    }
  }

  def evaluations: Array[((Int, Double, Double), Double)] = {
    val allData = RecommandArtist.getTrainData
    val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))

    trainData.cache()
    cvData.cache()

    val allItemIDs = allData.map(_.product).distinct().collect()
    val bAllItemIDs = sc.broadcast(allItemIDs)

    for (
      rank <- Array(10, 50);
      lambda <- Array(1.0, 0.0001);
      alpha <- Array(1.0, 40.0)
    ) yield {
      val model = ALS.trainImplicit(trainData, rank, 10, lambda, alpha)
      val auc = areaUnderCurve(cvData, bAllItemIDs, model.predict)
      ((rank, lambda, alpha), auc)
    }
  }
}