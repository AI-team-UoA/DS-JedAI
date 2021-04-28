package utils


import model.{Entity, IM, MBR}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import utils.Constants.ThetaOption
import utils.Constants.ThetaOption.ThetaOption

import scala.collection.mutable
import scala.reflect.ClassTag



object Utils extends Serializable {

	implicit def singleSTR[A](implicit c: ClassTag[String]): Encoder[String] = Encoders.STRING
	implicit def singleInt[A](implicit c: ClassTag[Int]): Encoder[Int] = Encoders.scalaInt
	implicit def tuple[String, Int](implicit s: Encoder[String], t: Encoder[Int]): Encoder[(String,Int)] = Encoders.tuple[String,Int](s, t)


	def getBordersOfMBR(mbrs: Seq[MBR], theta: (Double, Double)): Seq[MBR] ={
		val adjustedMBRs = mbrs.map(_.adjust(theta))

		// get overall borders
		val globalMinX: Double = adjustedMBRs.map(p => p.minX).min
		val globalMaxX: Double = adjustedMBRs.map(p => p.maxX).max
		val globalMinY: Double = adjustedMBRs.map(p => p.minY).min
		val globalMaxY: Double = adjustedMBRs.map(p => p.maxY).max

		// make them integers - filtering is discrete
		val spaceMinX = math.floor(globalMinX).toInt - 1
		val spaceMaxX = math.ceil(globalMaxX).toInt + 1
		val spaceMinY = math.floor(globalMinY).toInt - 1
		val spaceMaxY = math.ceil(globalMaxY).toInt + 1

		adjustedMBRs.map { mbr =>
			val minX = if (mbr.minX == globalMinX) spaceMinX else mbr.minX
			val maxX = if (mbr.maxX == globalMaxX) spaceMaxX else mbr.maxX
			val minY = if (mbr.minY == globalMinY) spaceMinY else mbr.minY
			val maxY = if (mbr.maxY == globalMaxY) spaceMaxY else mbr.maxY
			MBR(maxX, minX, maxY, minY)
		}
	}


	def getTheta(source: RDD[MBR], count: Option[Long] = None, thetaOption: ThetaOption = ThetaOption.AVG): (Double, Double) = {

		val sourceCount = count.getOrElse(source.count()).toDouble

		val (tx, ty) = thetaOption match {
			case ThetaOption.MIN =>
				// need filtering because there are cases where the geometries are perpendicular to the axes
				// hence its width or height is equal to 0.0
				val thetaX = source.map(mbb => mbb.maxX - mbb.minX).filter(_ != 0.0d).min
				val thetaY = source.map(mbb => mbb.maxY - mbb.minY).filter(_ != 0.0d).min
				(thetaX, thetaY)
			case ThetaOption.MAX =>
				val thetaX = source.map(mbb => mbb.maxX - mbb.minX).max
				val thetaY = source.map(mbr => mbr.maxY - mbr.minY).max
				(thetaX, thetaY)
			case ThetaOption.AVG =>
				val thetaX = source.map(mbr => mbr.maxX - mbr.minX).sum() / sourceCount
				val thetaY = source.map(mbr => mbr.maxY - mbr.minY).sum() / sourceCount
				(thetaX, thetaY)
			case ThetaOption.AVG_x2 =>
				val thetaXs = source.map(mbr => mbr.maxX - mbr.minX).sum() / sourceCount
				val thetaYs = source.map(mbr => mbr.maxY - mbr.minY).sum() / sourceCount
				val thetaX = 0.5 * thetaXs
				val thetaY = 0.5 * thetaYs
				(thetaX, thetaY)
			case _ =>
				(1d, 1d)
		}
		(tx, ty)
	}


	implicit class TupleAdd(t: (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)) {
		def +(p: (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)): (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) =
			(p._1 + t._1, p._2 + t._2, p._3 +t._3, p._4+t._4, p._5+t._5, p._6+t._6, p._7+t._7, p._8+t._8, p._9+t._9, p._10+t._10, p._11+t._11)
	}

	val accumulate: Iterator[IM] => (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) = imIterator =>{
		var totalContains: Int = 0
		var totalCoveredBy: Int = 0
		var totalCovers: Int = 0
		var totalCrosses: Int = 0
		var totalEquals: Int = 0
		var totalIntersects: Int = 0
		var totalOverlaps: Int = 0
		var totalTouches: Int = 0
		var totalWithin: Int = 0
		var verifications: Int = 0
		var qualifiedPairs: Int = 0
		imIterator.foreach { im =>
			verifications += 1
			if (im.relate) {
				qualifiedPairs += 1
				if (im.isContains) totalContains += 1
				if (im.isCoveredBy) totalCoveredBy += 1
				if (im.isCovers) totalCovers += 1
				if (im.isCrosses) totalCrosses += 1
				if (im.isEquals) totalEquals += 1
				if (im.isIntersects) totalIntersects += 1
				if (im.isOverlaps) totalOverlaps += 1
				if (im.isTouches) totalTouches += 1
				if (im.isWithin) totalWithin += 1
			}
		}
		(totalContains, totalCoveredBy, totalCovers, totalCrosses, totalEquals, totalIntersects,
			totalOverlaps, totalTouches, totalWithin, verifications, qualifiedPairs)
	}

	def countAllRelations(imRDD: RDD[IM]): (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) =
		imRDD
			.mapPartitions { imIterator => Iterator(accumulate(imIterator)) }
			.treeReduce({ case (im1, im2) => im1 + im2}, 4)




	def printPartition(joinedRDD: RDD[(Int, (Iterable[Entity],  Iterable[Entity]))], bordersMBR: Seq[MBR], thetaXY: (Double, Double)): Unit ={
		val partitionsBorders = getBordersOfMBR(bordersMBR, thetaXY).toArray
		val c = joinedRDD.map(p => (p._1, (p._2._1.size, p._2._2.size))).sortByKey().collect()
		val log: Logger = LogManager.getRootLogger
		log.info("Printing Partitions")
		log.info("----------------------------------------------------------------------------")
		var pSet = mutable.HashSet[String]()
		c.foreach(p => {
			val zoneStr = partitionsBorders(p._1).getGeometry.toText
			pSet += zoneStr
			log.info(p._1 + " ->  (" + p._2._1 + ", " + p._2._2 +  ") - " + zoneStr)
		})
		log.info("----------------------------------------------------------------------------")
		log.info("Unique blocks: " + pSet.size)
	}


	def exportCSV(rdd: RDD[(String, String)], path:String): Unit ={
		val spark: SparkSession = SparkSession.builder().getOrCreate()

		val schema = StructType(
			StructField("id1", StringType, nullable = true) ::
				StructField("id2", StringType, nullable = true) :: Nil
		)
		val rowRDD: RDD[Row] = rdd.map(s => new GenericRowWithSchema(Array(s._1, s._2), schema))
		val df = spark.createDataFrame(rowRDD, schema)
		df.write.option("header", "true").csv(path)
	}


	def exportRDF(rdd: RDD[IM], path:String): Unit ={
		val contains = "<http://www.opengis.net/ont/geosparql#sfContains>"
		val coveredBy = "<http://www.opengis.net/ont/geosparql#sfCoverdBy>"
		val covers = "<http://www.opengis.net/ont/geosparql#sfCovers>"
		val crosses = "<http://www.opengis.net/ont/geosparql#sfCrosses>"
		val equals = "<http://www.opengis.net/ont/geosparql#sfEquals>"
		val intersects = "<http://www.opengis.net/ont/geosparql#sfIntersects>"
		val overlaps = "<http://www.opengis.net/ont/geosparql#sfOverlaps>"
		val touches = "<http://www.opengis.net/ont/geosparql#sfTouches>"
		val within = "<http://www.opengis.net/ont/geosparql#sfWithin>"
		rdd.mapPartitions { imIterator =>
			val sb = new StringBuilder()
			imIterator.foreach { im =>
				if (im.isContains)
					sb.append(im.idPair._1 + " " + contains + " " + im.idPair._2 + " .\n")
				if (im.isCoveredBy)
					sb.append(im.idPair._1 + " " + coveredBy + " " + im.idPair._2 + " .\n")
				if (im.isCovers)
					sb.append(im.idPair._1 + " " + covers + " " + im.idPair._2 + " .\n")
				if (im.isCrosses)
					sb.append(im.idPair._1 + " " + crosses + " " + im.idPair._2 + " .\n")
				if (im.isEquals)
					sb.append(im.idPair._1 + " " + equals + " " + im.idPair._2 + " .\n")
				if (im.isIntersects)
					sb.append(im.idPair._1 + " " + intersects + " " + im.idPair._2 + " .\n")
				if (im.isOverlaps)
					sb.append(im.idPair._1 + " " + overlaps + " " + im.idPair._2 + " .\n")
				if (im.isTouches)
					sb.append(im.idPair._1 + " " + touches + " " + im.idPair._2 + " .\n")
				if (im.isWithin)
					sb.append(im.idPair._1 + " " + within + " " + im.idPair._2 + " .\n")
			}
			Iterator(sb.toString())
		}.saveAsTextFile(path)
	}
}