package com.objectfrontier.health.analytics.constants


import com.typesafe.config.ConfigFactory

object ApplicationConstants {
  
  val Config = ConfigFactory.load()
  
  // Cassandra Host
  val CASSANDRA_CONNECTION_HOST = Config.getString("cassandra.connection.host")
  
  // Mesos Master
  val MESO_MASTER = Config.getString("mesos.master")
  
  // Keyspace
  val KEYSPACE_UCI = Config.getString("keyspace.uci")
  
  // Table-name
  val UCI_PROTOCOLSUBJECT109 = Config.getString("uci.protocolsubject.109")
  
  // Decision Tree and Ram Forest Common Parameters
  val NUM_CLASSES = Config.getInt("numClasses")
  
  val CATEGORICAL_FEATURES_INFO = Map[Int, Int]()
  
  val IMPURITY = Config.getString("impurity")
  
  val MAX_DEPTH = Config.getInt("maxDepth")
  
  val MAX_BINS = Config.getInt("maxBins")
  
  // Extra Parameters for Random Forest
  val NUM_TREES = Config.getInt("numTrees")
  
  val FEATURE_SUBSET_STRATEGY = Config.getString("featureSubsetStrategy") 
  
  // Algorithm Choice
  val ALGORITM_CHOICE = Config.getString("algorithm.choice") 
  
  // Training and Testing Splits
  val TRAINING_SPLIT_SIZE = Config.getDouble("training.split.size")
    
  val TESTING_SPLIT_SIZE  = Config.getDouble("testing.split.size")
  
  // Decision Tree Model Path
  val DECISION_TREE_MODEL_PATH = Config.getString("decisionTreeModelPath")
    
  // Random Forest Model Path
  val RANDOM_FOREST_MODEL_PATH = Config.getString("randomForestModelPath")
 
}