package utils


import cats.implicits._
import model.IM
import model.entities.EntityT
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.locationtech.jts.geom.Envelope

import scala.collection.mutable

object Utils extends Serializable {

	def printPartition(joinedRDD: RDD[(Int, (Iterable[EntityT],  Iterable[EntityT]))], bordersEnvelope: Array[Envelope]): Unit ={
		val c = joinedRDD.map(p => (p._1, (p._2._1.size, p._2._2.size))).sortByKey().collect()
		val log: Logger = LogManager.getRootLogger
		log.info("Printing Partitions")
		log.info("----------------------------------------------------------------------------")
		val pSet = mutable.HashSet[String]()
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
				sb.append( im.getId1 + " " + contains + " " + im.getId2 + " .\n")
			if (im.isCoveredBy)
				sb.append(im.getId1 + " " + coveredBy + " " + im.getId2 + " .\n")
			if (im.isCovers)
				sb.append(im.getId1 + " " + covers + " " + im.getId2 + " .\n")
			if (im.isCrosses)
				sb.append(im.getId1 + " " + crosses + " " + im.getId2 + " .\n")
			if (im.isEquals)
				sb.append(im.getId1 + " " + equals + " " + im.getId2 + " .\n")
			if (im.isIntersects)
				sb.append(im.getId1 + " " + intersects + " " + im.getId2 + " .\n")
			if (im.isOverlaps)
				sb.append(im.getId1 + " " + overlaps + " " + im.getId2 + " .\n")
			if (im.isTouches)
				sb.append(im.getId1 + " " + touches + " " + im.getId2 + " .\n")
			if (im.isWithin)
				sb.append(im.getId1 + " " + within + " " + im.getId2 + " .\n")
			sb.toString()
		}.saveAsTextFile(path)
	}


	def exportMatchingPairs(matchingPairsRDD: RDD[(String, String)], path:String): Unit ={
		matchingPairsRDD.mapPartitions { pairIterator =>
			val sb = new StringBuilder()
			pairIterator.foreach{case (s, t) => sb.append(s"$s\t$t\t1.0\n")}
			Iterator(sb.toString())
		}.coalesce(numPartitions=1).saveAsTextFile(path)
	}


	def exportNTRIPLES(rdd: RDD[(String, String)], path:String): Unit ={
		val predicate = "<http://earthanalytics.eu/fs/ontology/belongsToAdministrativeUnit>"
		rdd.mapPartitions { pairIterator =>
			val sb = new StringBuilder()
			pairIterator.foreach { case (sbj, obj) => sb.append(sbj + " " + predicate + " " + obj + " .\n")}
			Iterator(sb.toString())
		}.coalesce(numPartitions=1).saveAsTextFile(path)
	}
}