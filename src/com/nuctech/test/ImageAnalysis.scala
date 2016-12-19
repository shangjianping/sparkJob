package com.nuctech.test

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}

import scala.util.Random

object ImageAnalysis {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName(args(0))




    val sc = new SparkContext(sparkConf)
    val file = sc.textFile("file://"+args(1))
    val date = Random.nextInt()
    println("aa"+date)
    file.saveAsTextFile(args(2)+"/tmp/"+date)





    //val data = sc.textFile("file:///usr/hdp/2.2.4.2-2/spark/data/mllib/ridge-data/lpsa.data")
    val data = sc.textFile("file://"+args(3))
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()

    // Building the model
    val numIterations = 100
    val model = LinearRegressionWithSGD.train(parsedData, numIterations)

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
    println("training Mean Squared Error = " + MSE)
    sc.stop()
  }
}
