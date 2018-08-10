package com.example.apps.reommand

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

object RecommandArtist extends LazyLogging {
  def getModel(trainData: RDD[Rating], rank: Int, iterations: Int, lambda: Double, alpha: Double): MatrixFactorizationModel = {
    ALS.trainImplicit(this.getTrainData, rank, iterations, lambda, alpha)
  }

  private def getRating: RDD[Rating] = {
    ArtistData.getRawUserArtistData.map{ line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID = ArtistData.getBroadCastedArtistAliasInfo.value.getOrElse(artistID, artistID)
      Rating(userID, finalArtistID, count)
    }
  }

  def getTrainData: RDD[Rating] = {
    getRating.cache()
  }


}
