package utils


import model.entities.Entity
import model.{IM, TileGranularities}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.locationtech.jts.geom.Envelope

import scala.collection.mutable
import scala.reflect.ClassTag
import cats.implicits._

object Utils extends Serializable {

	implicit def singleSTR[A](implicit c: ClassTag[String]): Encoder[String] = Encoders.STRING
	implicit def singleInt[A](implicit c: ClassTag[Int]): Encoder[Int] = Encoders.scalaInt
	implicit def tuple[String, Int](implicit s: Encoder[String], t: Encoder[Int]): Encoder[(String,Int)] = Encoders.tuple[String,Int](s, t)

	val accumulate: Iterator[IM] => (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) = imIterator => {
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
			.treeReduce({ case (im1, im2) => im1 |+| im2}, 4)




	def printPartition(joinedRDD: RDD[(Int, (Iterable[Entity],  Iterable[Entity]))], bordersEnvelope: Array[Envelope], tilesGranularities: TileGranularities): Unit ={
		val c = joinedRDD.map(p => (p._1, (p._2._1.size, p._2._2.size))).sortByKey().collect()
		val log: Logger = LogManager.getRootLogger
		log.info("Printing Partitions")
		log.info("----------------------------------------------------------------------------")
		var pSet = mutable.HashSet[String]()
		c.foreach(p => {
			val zoneStr = bordersEnvelope(p._1).toString
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
		rdd.map { im =>
			val sb = new StringBuilder()
			if (im.isContains)
				sb.append("<" + im.getId1 +">" + " " + contains + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isCoveredBy)
				sb.append("<" + im.getId1 +">" + " " + coveredBy + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isCovers)
				sb.append("<" + im.getId1 +">" + " " + covers + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isCrosses)
				sb.append("<" + im.getId1 +">" + " " + crosses + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isEquals)
				sb.append("<" + im.getId1 +">" + " " + equals + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isIntersects)
				sb.append("<" + im.getId1 +">" + " " + intersects + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isOverlaps)
				sb.append("<" + im.getId1 +">" + " " + overlaps + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isTouches)
				sb.append("<" + im.getId1 +">" + " " + touches + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isWithin)
				sb.append("<" + im.getId1 +">" + " " + within + " " + "<" + im.getId2 + ">" + " .\n")
			sb.toString()
		}.saveAsTextFile(path)
	}
}