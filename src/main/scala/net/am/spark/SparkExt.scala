package net.am.spark

import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.mllib.linalg.{Vector => MllibVector, Vectors => MllibVectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

object SparkExt {

  implicit class DataFrameExt(df: DataFrame) {
    def showNullCounts = {
      //    df.columns.foldLeft(df){(df1, field) => df1.withColumn(s"${field}Null")}
      val totalCount = df.count
      println(s"total count = ${totalCount}")
      df.columns.foreach { field =>
        val nullCount = df.where(col(field).isNull).count
        if (nullCount > 0) println(s"$field null count = $nullCount, percent = ${nullCount.toDouble/totalCount}")
      }
    }

    def showNumericData = {
      val (fields, stats) = colStats

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

    def showCorrMatrices(featureCol: String) = {

      val Row(coeff1: Matrix) = Correlation.corr(df, featureCol).head
      println(s"Pearson correlation matrix:\n $coeff1")

      val Row(coeff2: Matrix) = Correlation.corr(df, featureCol, "spearman").head
      println(s"Spearman correlation matrix:\n $coeff2")

    }

    def colStats: (Seq[StructField], MultivariateStatisticalSummary) = {
      val (fields, rdd) = toVectorRdd
      (fields, Statistics.colStats(rdd))
    }

    def toVectorRdd: (Seq[StructField], RDD[MllibVector]) = {
      val fields = df.schema.numericFields
      val cols = fields.map(_.name).map(col(_))

      (fields, df.select(cols:_*).rdd.map(_.toVector))
      //
      //    val rdd = df.select(cols:_*).select(array(df.columns.map(col(_)): _*)).rdd.map {
      //      case Row(xs: Seq[Double @unchecked]) => Vector(xs:_*)
      //    }

      //    Encoders.product[Vector]
      //    df.map(toVector, Encoders.product[Vectors])
    }

  }

  implicit class RowExt(row: Row) {

    /**
      * @return vector of doubles. Row must have all field types that can be converted to double.
      */
    def toVector = {
      val doubleVals = row.schema.numericFields.map{ field =>
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
  }

  implicit class StructTypeExt(schema: StructType) {
    def numericFields = schema.filter(_.dataType.isInstanceOf[NumericType])
  }

//  implicit val byteDoubleLike: DoubleLikeCol[ByteType.type] = new DoubleLikeCol[ByteType.type ] {
//    override def getValue(a: ByteType.type, row: Row, field: StructField): Double = row.getAs[Byte](field.name).toDouble
//  }
//
//  implicit val shortDoubleLike: DoubleLikeCol[ShortType] = new DoubleLikeCol[ShortType] {
//    override def getValue(a: ShortType, row: Row, field: StructField): Double = row.getAs[Short](field.name).toDouble
//  }
//
//  implicit val integerDoubleLike: DoubleLikeCol[IntegerType] = new DoubleLikeCol[IntegerType] {
//    override def getValue(a: IntegerType, row: Row, field: StructField): Double = row.getAs[Int](field.name).toDouble
//  }
//
//  implicit val longDoubleLike: DoubleLikeCol[LongType] = new DoubleLikeCol[LongType] {
//    override def getValue(a: LongType, row: Row, field: StructField): Double = row.getAs[Long](field.name).toDouble
//  }
//
//  implicit val floatDoubleLike: DoubleLikeCol[FloatType] = new DoubleLikeCol[FloatType] {
//    override def getValue(a: FloatType, row: Row, field: StructField): Double = row.getAs[Float](field.name).toDouble
//  }
//
//  implicit val doubleDoubleLike: DoubleLikeCol[Double] = new DoubleLikeCol[Double] {
//    override def getValue(a: Double, row: Row, field: StructField): Double = row.getAs[Double](field.name)
//  }
//
//  def getDoubleVal[A: DoubleLikeCol](a: A, row: Row, field: StructField) = implicitly[DoubleLikeCol[A]].getValue(a, row, field)


}

//trait DoubleLikeCol[A] {
//  def getValue(a: A, row: Row, field: StructField): Double
//}


