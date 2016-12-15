package com.objectfrontier.health.analytics.algorithms


import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.regression.LabeledPoint
import com.objectfrontier.health.analytics.constants.ApplicationConstants
import org.apache.spark.rdd.RDD
import scala.math.BigDecimal

class RandomForestAlgorithm {


	def getRandomForestDataModel(trainingData: org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint], 
                               numClasses: Int,
                               categoricalFeaturesInfo: Map[Int,Int],
                               numTrees: Int,
                               featureSubsetStrategy: String, 
                               impurity: String, 
                               maxDepth: Int, 
                               maxBins: Int) = {

		  val randomForestModel = RandomForest.trainClassifier(trainingData,
                                               numClasses, 
				                                       categoricalFeaturesInfo,
                                               numTrees, 
				                                       featureSubsetStrategy, 
                                               impurity, 
                                               maxDepth, 
                                               maxBins)

		 randomForestModel
    
	} // end - getRandomForestDataModel function
  
  def calculateRandomForestPredictionAccuracy(testData             : org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint], 
                                              randomForestModel    : org.apache.spark.mllib.tree.model.RandomForestModel) = {

      val labelAndPreds = testData.map { point =>
                                        val prediction = randomForestModel.predict(point.features)
                                        (point.label, prediction)
                                        }

      val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()

      val prediction_Accuracy = BigDecimal(100-testErr*100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
       
      println()
      println("////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////")
      println()
      println("RandomForestAlgorithm Prediction Accuracy = " + prediction_Accuracy + "%")
      println()
      println("////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////")
      println()
          
  } // end - calculateRandomForestPredictionAccuracy function
  
   
  // Save and load model
  def saveModel(randomForestModel    : org.apache.spark.mllib.tree.model.RandomForestModel,
                               sc    : org.apache.spark.SparkContext) = {
    
          randomForestModel.save(sc, ApplicationConstants.RANDOM_FOREST_MODEL_PATH)
  
  } // end saveModel
  
  
} // end - RandomForestAlgorithm class
