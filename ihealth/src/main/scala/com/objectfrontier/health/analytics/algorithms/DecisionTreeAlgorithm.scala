package com.objectfrontier.health.analytics.algorithms


import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.regression.LabeledPoint
import com.objectfrontier.health.analytics.constants.ApplicationConstants
import org.apache.spark.rdd.RDD
import scala.math.BigDecimal

class DecisionTreeAlgorithm {


	def getDecisionTreeDataModel(trainingData: org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint], 
                               numClasses: Int,
                               categoricalFeaturesInfo: Map[Int,Int],
                               impurity: String, 
                               maxDepth: Int, 
                               maxBins: Int) = {
    
        val decisionTreeModel = DecisionTree.trainClassifier(trainingData, 
                                                 numClasses, 
                                                 categoricalFeaturesInfo, 
                                                 impurity, 
                                                 maxDepth, 
                                                 maxBins)

		    decisionTreeModel
	} // end - getDecisionTreeDataModel function
  
  def calculateDecisionTreePredictionAccuracy(testData             : org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint], 
                                              decisionTreeModel    : org.apache.spark.mllib.tree.model.DecisionTreeModel) = {

          val labelAndPreds = testData.map { point =>
                                             val prediction = decisionTreeModel.predict(point.features)
                                             (point.label, prediction)
                                           }

          val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()

          val prediction_Accuracy = BigDecimal(100-testErr*100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

          println()
          println("////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////")
          println()
          println("DecisionTreeAlgorithm Prediction Accuracy = " + prediction_Accuracy + "%")
          println()
          println("////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////")
          println()
 
  } // end - calculateDecisionTreePredictionAccuracy function
  
  // Save and load model
  def saveModel(decisionTreeModel    : org.apache.spark.mllib.tree.model.DecisionTreeModel,
                               sc    : org.apache.spark.SparkContext) = {
    
          decisionTreeModel.save(sc, ApplicationConstants.DECISION_TREE_MODEL_PATH)
  
  } // end saveModel
  
}// end - DecisionTreeAlgorithm Class