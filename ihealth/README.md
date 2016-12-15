# Spark_HealthCare_Analytics

For Local Run

1.sbt assembly

2.spark-submit --class "com.objectfrontier.health.analytics.main.HistoricalHealthAnalyticsApp" --master local[8] target/scala-2.11/HistoricalHealthAnalyticsApp.jar
