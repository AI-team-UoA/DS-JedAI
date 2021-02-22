package utils


import dataModel.{Entity, MBR}
import com.vividsolutions.jts.geom.Geometry
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import utils.Constants.ThetaOption
import utils.Constants.ThetaOption.ThetaOption

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
object Utils extends Serializable {

	val spark: SparkSession = SparkSession.builder().getOrCreate()

	var thetaOption: ThetaOption = _
	var source: RDD[MBR] = spark.sparkContext.emptyRDD
	var partitionsZones: Array[MBR] = Array()
	lazy val sourceCount: Long = source.count()
	lazy val thetaXY: (Double, Double) = initTheta()

	def apply(sourceRDD: RDD[MBR], thetaOpt: ThetaOption = Constants.ThetaOption.AVG, pz: Array[MBR]=Array()): Unit ={
		source = sourceRDD
		source.cache()
		thetaOption = thetaOpt
		partitionsZones = pz
	}

	def getTheta: (Double, Double) = thetaXY
	def getSourceCount: Long = sourceCount


	implicit def singleSTR[A](implicit c: ClassTag[String]): Encoder[String] = Encoders.STRING
	implicit def singleInt[A](implicit c: ClassTag[Int]): Encoder[Int] = Encoders.scalaInt
	implicit def tuple[String, Int](implicit e1: Encoder[String], e2: Encoder[Int]): Encoder[(String,Int)] = Encoders.tuple[String,Int](e1, e2)

	lazy val globalMinX: Double = partitionsZones.map(p => p.minX / thetaXY._1).min
	lazy val globalMaxX: Double = partitionsZones.map(p => p.maxX / thetaXY._1).max
	lazy val globalMinY: Double = partitionsZones.map(p => p.minY / thetaXY._2).min
	lazy val globalMaxY: Double = partitionsZones.map(p => p.maxY / thetaXY._2).max

	lazy val totalBlocks: Double = (globalMaxX - globalMinX + 1) * (globalMaxY - globalMinY + 1)


	/**
	 * initialize theta based on theta granularity
	 */
	private def initTheta(): (Double, Double) = {

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
		source.unpersist()
		(tx, ty)
	}


	def getZones: Array[MBR] ={
		val (thetaX, thetaY) = thetaXY

		val spaceMinX = math.floor(partitionsZones.map(p => p.minX / thetaX).min).toInt - 1
		val spaceMaxX = math.ceil(partitionsZones.map(p => p.maxX / thetaX).max).toInt + 1
		val spaceMinY = math.floor(partitionsZones.map(p => p.minY / thetaY).min).toInt - 1
		val spaceMaxY = math.ceil(partitionsZones.map(p => p.maxY / thetaY).max).toInt + 1

		partitionsZones.map(mbr => {
			val minX = if (mbr.minX / thetaX == globalMinX) spaceMinX else mbr.minX / thetaX
			val maxX = if (mbr.maxX / thetaX == globalMaxX) spaceMaxX else mbr.maxX / thetaX
			val minY = if (mbr.minY / thetaY == globalMinY) spaceMinY else mbr.minY / thetaY
			val maxY = if (mbr.maxY / thetaY == globalMaxY) spaceMaxY else 	mbr.maxY / thetaY

			MBR(maxX, minX, maxY, minY)
		})
	}


	def normalizeWeight(weight: Double, geom1: Geometry, geom2: Geometry): Double ={
		val area1 = geom1.getArea
		val area2 = geom2.getArea
		if (area1 == 0 || area2 == 0 ) weight
		else weight/(geom1.getArea * geom2.getArea)
	}


	def printPartition(joinedRDD: RDD[(Int, (Iterable[Entity],  Iterable[Entity]))]): Unit ={
		val c = joinedRDD.map(p => (p._1, (p._2._1.size, p._2._2.size))).sortByKey().collect()
		val log: Logger = LogManager.getRootLogger
		log.info("Printing Partitions")
		log.info("----------------------------------------------------------------------------")
		var pSet = mutable.HashSet[String]()
		c.foreach(p => {
			val zoneStr = getZones(p._1).getGeometry.toText
			pSet += zoneStr
			log.info(p._1 + " ->  (" + p._2._1 + ", " + p._2._2 +  ") - " + zoneStr)
		})
		log.info("----------------------------------------------------------------------------")
		log.info("Unique blocks: " + pSet.size)
	}

	def export(rdd: RDD[Entity], path:String): Unit ={
		val schema = StructType(
			StructField("id", IntegerType, nullable = true) ::
				StructField("wkt", StringType, nullable = true) :: Nil
		)
		val rowRDD: RDD[Row] = rdd.map(s => new GenericRowWithSchema(Array(TaskContext.getPartitionId(), s.geometry.toText), schema))
		val df = spark.createDataFrame(rowRDD, schema)
		df.write.option("header", "true").csv(path)
	}

}