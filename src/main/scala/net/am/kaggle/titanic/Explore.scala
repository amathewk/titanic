package net.am.kaggle.titanic

import org.apache.commons.math3.stat.descriptive.MultivariateSummaryStatistics
import org.apache.spark.sql._
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.linalg.{Vectors => MllibVectors}
import org.apache.spark.mllib.linalg.{Vector => MllibVector}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
//import org.apache.spark.mllib.linalg.{Vector => mllibVector, Vectors => mllibVectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.functions._


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
      .csv("/home/amathew/kaggle/titanic/project/data/test.csv")

    inspectNumericData(df)
    inspectNullCounts(df)
  }

  def inspectNullCounts(df: DataFrame) = {
//    df.columns.foldLeft(df){(df1, field) => df1.withColumn(s"${field}Null")}
    val totalCount = df.count
    println(s"total count = ${totalCount}")
    df.columns.foreach { field =>
      val nullCount = df.where(col(field).isNull).count
      if (nullCount > 0) println(s"$field null count = $nullCount, percent = ${nullCount.toDouble/totalCount}")
    }
  }

//  def nullCountCol(col: Column) = {
//
//  }

  def inspectNumericData(df: DataFrame) = {
    val (fields, stats) = colStats(df)

    println(s"count = ${stats.count}")

    fields.zipWithIndex.foreach { case (field, index) =>
      println(field.name)
      println(s"non zeroes = ${stats.numNonzeros(index)}")
      println(s"min = ${stats.min(index)}")
      println(s"mean = ${stats.mean(index)}")
      println(s"max = ${stats.max(index)}")
      println(s"max = ${stats.normL1(index)}")
      println(s"max = ${stats.normL2(index)}")
      println(s"max = ${stats.variance(index)}")
      println("")
    }

  }

  def colStats(df: DataFrame): (Seq[StructField], MultivariateStatisticalSummary) = {
    val (fields, rdd) = toVectorRdd(df)
    (fields, Statistics.colStats(rdd))
  }

  def toVectorRdd(df: DataFrame): (Seq[StructField], RDD[MllibVector]) = {
    val fields = numericFields(df.schema)
    val cols = fields.map(_.name).map(col(_))

    (fields, df.select(cols:_*).rdd.map(toVector))
//
//    val rdd = df.select(cols:_*).select(array(df.columns.map(col(_)): _*)).rdd.map {
//      case Row(xs: Seq[Double @unchecked]) => Vector(xs:_*)
//    }

//    Encoders.product[Vector]
//    df.map(toVector, Encoders.product[Vectors])
  }

  def toVector(row: Row) = {
    val numericFields = row.schema.filter(_.dataType.isInstanceOf[NumericType])

    val doubleVals = numericFields.map{ field =>
      field.dataType match {
        case ByteType => row.getAs[Byte](field.name).toDouble
        case ShortType => row.getAs[Short](field.name).toDouble
        case IntegerType => row.getAs[Int](field.name).toDouble
        case LongType => row.getAs[Long](field.name).toDouble
        case FloatType => row.getAs[Float](field.name).toDouble
        case DoubleType => row.getAs[Double](field.name)
        case _ => throw new RuntimeException
      }
    }

    MllibVectors.dense(doubleVals.toArray)
  }

  def numericFields(schema: StructType) = schema.filter(_.dataType.isInstanceOf[NumericType])

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

    import spark.implicits._

    import org.apache.spark.ml.linalg.{Vector, Vectors}
    import org.apache.spark.ml.stat.ChiSquareTest

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