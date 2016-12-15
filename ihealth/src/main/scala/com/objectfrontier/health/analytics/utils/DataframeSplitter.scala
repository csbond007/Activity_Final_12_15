package com.objectfrontier.health.analytics.utils

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.DataFrame
import com.objectfrontier.health.analytics.constants.ApplicationConstants

class DataframeSplitter {
  
  def getTrainingAndTestingData(df_prepared : org.apache.spark.sql.DataFrame)
                                             : (org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint],
                                                org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint]) = {
    
           val splits = df_prepared.randomSplit(Array(ApplicationConstants.TRAINING_SPLIT_SIZE, 
                                                      ApplicationConstants.TESTING_SPLIT_SIZE))

           val (training_split, test_split) = (splits(0), splits(1))

           val trainingData = training_split.rdd.map(row => LabeledPoint(
                                                                         row.getAs[Double]("label"),
                                                                         row.getAs[org.apache.spark.mllib.linalg.Vector]("features")
                                                                        ))

           val testData = test_split.rdd.map(row => LabeledPoint(
                                                                 row.getAs[Double]("label"),
                                                                 row.getAs[org.apache.spark.mllib.linalg.Vector]("features")
                                                                ))

          (trainingData, testData)         
    } // end get_Training_Testing_Data function
  
}