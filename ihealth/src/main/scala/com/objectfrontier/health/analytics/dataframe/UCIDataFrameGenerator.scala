package com.objectfrontier.health.analytics.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import com.google.common.collect.ImmutableMap
import org.apache.spark.sql.functions.udf
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import com.typesafe.config.ConfigFactory
import com.objectfrontier.health.analytics.constants.ApplicationConstants

import scala.math.BigDecimal
class UCIDataFrameGenerator(sqlCtx:SQLContext,keyspaceName :String, tableName: String) {

    val df = sqlCtx.read
			             .format("org.apache.spark.sql.cassandra")
			             .options(Map( "table" -> tableName, "keyspace" -> keyspaceName))
		             	 .load()

    def getUCIImputedDataframe() = {
		
        // Remove Time_Stamp Column here as it is non-important attribute
        val df_minus_timestamp = df.drop("time_stamp")

				//Fill the empty column with "NA"
				// val df1 = df_minus_timestamp.na.replace("heart_rate", ImmutableMap.of("NA", "0"));

		    //Filter Heart Rate
		    // val df_imputed = df1.filter("heart_rate != 0")

				df_minus_timestamp
    } // end - getUCIImputedDataframe()  function

    def encodeLabel=udf((activity_id: Double) => {
                            activity_id match {
                                case 1 => 0.0 
                                case 2 => 1.0
                                case 3 => 2.0
                                case 4 => 3.0
                                case 5 => 4.0
                                case 6 => 5.0
                                case 7 => 6.0
                                case 9 => 7.0
                                case 10 => 8.0
                                case 11 => 9.0
                                case 12 => 10.0
                                case 13 => 11.0
                                case 16 => 12.0
                                case 17 => 13.0
                                case 18 => 14.0
                                case 19 => 15.0
                                case 20 => 16.0
                                case 24 => 17.0
                              //case 0 => 18.0
                                case _ => 18.0
                       }}) // end - udf - encodeLabel function
    
    def convert2Vector =udf((heart_rate: Double,
                      IMU_hand_temperature: Double,  // 2
                       IMU_hand_accl_16g_1: BigDecimal
                      ) => {
                                                        Vectors.dense(heart_rate,             // 1
                                                                      IMU_hand_temperature,   // 2
                                                                       IMU_hand_accl_16g_1
                                                 )})  // end - udf - convert2Vector function
       
                                                                      
    def getUCIPreparedDataframe(df_imputed : org.apache.spark.sql.DataFrame ) = {

		      val df_prepared = df_imputed.withColumn(
										                              "features",
										                               convert2Vector(
												                                          df_imputed("heart_rate"),
												                                          df_imputed("IMU_hand_temperature"), // 2   
                                                                  //df_imputed("IMU_hand_temperature"), // 3
                                                                  //df_imputed("IMU_hand_temperature"), // 4
                                                                  //df_imputed("IMU_hand_temperature"), // 5
                                                                  //df_imputed("IMU_hand_temperature"), // 6
                                                                  //df_imputed("IMU_hand_temperature"), // 7
                                                                  //df_imputed("IMU_hand_temperature"), // 8
                                                                  //df_imputed("IMU_hand_temperature"), // 9
                                                                  //df_imputed("IMU_hand_temperature"), // 10
                                                                  //df_imputed("IMU_hand_temperature"), // 11
                                                                  //df_imputed("IMU_hand_temperature"), // 12
                                                                  //df_imputed("IMU_hand_temperature"), // 13
                                                                  //df_imputed("IMU_hand_temperature"), // 14
                                                                  //df_imputed("IMU_hand_temperature"), // 15
                                                                  //df_imputed("IMU_hand_temperature"), // 16
                                                                  //df_imputed("IMU_hand_temperature"), // 17
                                                                  //df_imputed("IMU_hand_temperature"), // 18
                                                                  //df_imputed("IMU_hand_temperature"), // 19
                                                                  //df_imputed("IMU_hand_temperature"), // 20
                                                                  //df_imputed("IMU_hand_temperature"), // 21
                                                                  df_imputed("IMU_hand_accl_16g_1").toDouble
                                                                   )
										                                               )
                                      .withColumn("label", encodeLabel(df_imputed("activity_id")))
										                  .select("features", "label")

					 df_prepared

	  } // end - getUCIPreparedDataframe function
   
} // end - class - UCIDataFrameGenerator
