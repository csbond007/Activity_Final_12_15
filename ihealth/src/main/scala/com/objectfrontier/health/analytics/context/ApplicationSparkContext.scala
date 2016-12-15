package com.objectfrontier.health.analytics.context

//// Basic Spark Library
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import com.objectfrontier.health.analytics.constants.ApplicationConstants


object ApplicationSparkContext {

	val conf = new SparkConf(true)
	                              .set("spark.cassandra.connection.host", ApplicationConstants.CASSANDRA_CONNECTION_HOST)
	                              .setAppName(this.getClass.getSimpleName)
	                            //.setMaster("spark://10.10.40.138:7077")
	                            //.setMaster(ApplicationConstants.MESO_MASTER)

	val sparkContext = new SparkContext(conf)  

	def getApplicationSparkContext() = sparkContext

}