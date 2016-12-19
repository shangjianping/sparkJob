package com.nuctech.test

import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.{Milliseconds, _}


object InspectionImage {
  def main(args: Array[String]) {
    val batchInterval = Milliseconds(10000)
    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("InpectionImage")
    val ssc = new StreamingContext(sparkConf, batchInterval)


    val trainingData = ssc.textFileStream("/user/spark/").map(LabeledPoint.parse).cache()
    val testData = ssc.textFileStream("/user/spark/").map(LabeledPoint.parse)
    val numFeatures = 2
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.zeros(numFeatures))
    model.trainOn(trainingData)
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
