package net.am.kaggle.titanic

import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql._


object Explore {

  lazy val spark = SparkSession.builder().appName("titanicExplore").master("local").getOrCreate()

  def main(args: Array[String]) = {

    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.OFF)

//    chiTest
//    corr
    inspectTitanc
  }

  def inspectTitanc = {

    val df = spark.read.option("inferSchema", true).option("header", true)
      .csv("/home/amathew/kaggle/titanic/titanicProject/data/test.csv")

    import net.am.spark.SparkExt._
    import spark.implicits._

    df.showNumericData
    df.showNullCounts

//    df.toVectorRdd.createD
//    df.showCorrMatrices()
  }

  def corr = {

    import spark.implicits._

    val data = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )

    val df = data.map(Tuple1.apply).toDF("features")

    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
    println(s"Pearson correlation matrix:\n $coeff1")

    val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
    println(s"Spearman correlation matrix:\n $coeff2")
    //
    //    val seriesX: RDD[Double] = sc.parallelize(Array(1, 2, 3, 3, 5))
    //
    //
    //    val data2: RDD[mllibVector] = sc.parallelize(
    //      Seq(
    //        mllibVectors.dense(1.0, 10.0, 100.0),
    //        mllibVectors.dense(2.0, 20.0, 200.0),
    //        mllibVectors.dense(5.0, 33.0, 366.0))
    //    )
    //
    //
    //    val correlMatrix: Matrix = Statistics.corr(data2, "pearson")
    //    println(correlMatrix.toString)

  }

  private def chiTest = {

    val spark = SparkSession.builder().appName("titanicExplore").master("local").getOrCreate()

    val sc = spark.sparkContext

    import org.apache.spark.ml.linalg.{Vector, Vectors}
    import org.apache.spark.ml.stat.ChiSquareTest
    import spark.implicits._

    val data = Seq(
      (0.0, Vectors.dense(0.5, 10.0)),
      (0.0, Vectors.dense(1.5, 20.0)),
      (1.0, Vectors.dense(1.5, 30.0)),
      (0.0, Vectors.dense(3.5, 30.0)),
      (0.0, Vectors.dense(3.5, 40.0)),
      (1.0, Vectors.dense(3.5, 40.0))
    )

    val df = data.toDF("label", "features")
    val chi = ChiSquareTest.test(df, "features", "label").head
    println(s"pValues = ${chi.getAs[Vector](0)}")
    println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
    println(s"statistics ${chi.getAs[Vector](2)}")
  }

}