package utils


import DataStructures.{MBR, Entity}
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
object Utils {

	val spark: SparkSession = SparkSession.builder().getOrCreate()
	val log: Logger = LogManager.getRootLogger
	var thetaOption: ThetaOption = _
	var source: RDD[MBR] = _
	var partitionsZones: Array[MBR] = _
	lazy val sourceCount: Long = source.count()
	lazy val thetaXY: (Double, Double) = initTheta()

	def apply(sourceRDD: RDD[MBR], thetaOpt: ThetaOption = Constants.ThetaOption.AVG, pz: Array[MBR]=Array()): Unit ={
		source = sourceRDD
		thetaOption = thetaOpt
		partitionsZones = pz
	}

	def getTheta: (Double, Double)= thetaXY
	def getSourceCount: Long = sourceCount

	/**
	 * Cantor Pairing function. Map two positive integers to a unique integer number.
	 *
	 * @param a Long
	 * @param b Long
	 * @return the unique mapping of the integers
	 */
	def cantorPairing(a: Long, b: Long): Long =  (((a + b) * (a + b + 1))/2) + b

	/**
	 * Apply cantor pairing for negative integers
	 *
	 * @param x integer
	 * @param y integer
	 * @return the unique mapping of the integers
	 */
	def signedPairing(x: Long, y: Long): Long ={
		val a = if (x < 0) (-2)*x - 1 else 2*x
		val b = if (y < 0) (-2)*y - 1 else 2*y

		cantorPairing(a, b)
	}


	/**
	 * Compute the Estimation of the Total Hyper-volume
	 *
	 * @param seRDD Spatial Entities
	 * @param count number of the entities
	 * @return Estimation of the Total Hyper-volume
	 */
	def getETH(seRDD: RDD[MBR], count: Double): Double ={
		val denom = 1/count
		val coords_sum = seRDD
			.map(mbb => (mbb.maxX - mbb.minX, mbb.maxY - mbb.minY))
			.fold((0, 0)) { case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2) }

		val eth = count * ( (denom * coords_sum._1) * (denom * coords_sum._2) )
		eth
	}

	implicit def singleSTR[A](implicit c: ClassTag[String]): Encoder[String] = Encoders.STRING
	implicit def singleInt[A](implicit c: ClassTag[Int]): Encoder[Int] = Encoders.scalaInt
	implicit def tuple[String, Int](implicit e1: Encoder[String], e2: Encoder[Int]): Encoder[(String,Int)] = Encoders.tuple[String,Int](e1, e2)

	def export(rdd: RDD[Entity], path:String): Unit ={
		val schema = StructType(
			StructField("id", IntegerType, nullable = true) ::
			StructField("wkt", StringType, nullable = true) :: Nil
		)
		val rowRDD: RDD[Row] = rdd.map(s => new GenericRowWithSchema(Array(TaskContext.getPartitionId(), s.geometry.toText), schema))
		val df = spark.createDataFrame(rowRDD, schema)
		df.write.option("header", "true").csv(path)
	}


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
				val thetaY = source.map(mbb => mbb.maxY - mbb.minY).max
				(thetaX, thetaY)
			case ThetaOption.AVG =>
				val thetaX = source.map(mbb => mbb.maxX - mbb.minX).sum() / sourceCount
				val thetaY = source.map(mbb => mbb.maxY - mbb.minY).sum() / sourceCount
				(thetaX, thetaY)
			case ThetaOption.AVG_x2 =>
				val thetaXs = source.map(mbb => mbb.maxX - mbb.minX).sum() / sourceCount
				val thetaYs = source.map(mbb => mbb.maxY - mbb.minY).sum() / sourceCount
				val thetaX = 0.5 * thetaXs
				val thetaY = 0.5 * thetaYs
				(thetaX, thetaY)
			case _ =>
				(1d, 1d)
		}
		source.unpersist()
		(tx, ty)
	}

	// todo fix spaghetti code
	def getZones: Array[MBR] ={
		val (thetaX, thetaY) = thetaXY

		val globalMinX = partitionsZones.map(p => p.minX / thetaX).min
		val globalMaxX = partitionsZones.map(p => p.maxX / thetaX).max
		val globalMinY = partitionsZones.map(p => p.minY / thetaY).min
		val globalMaxY = partitionsZones.map(p => p.maxY / thetaY).max

		val spaceMinX = math.floor(partitionsZones.map(p => p.minX / thetaX).min).toInt - 1
		val spaceMaxX = math.ceil(partitionsZones.map(p => p.maxX / thetaX).max).toInt + 1
		val spaceMinY = math.floor(partitionsZones.map(p => p.minY / thetaY).min).toInt - 1
		val spaceMaxY = math.ceil(partitionsZones.map(p => p.maxY / thetaY).max).toInt + 1

		partitionsZones.map(mbb => {
			val minX = if (mbb.minX / thetaX == globalMinX) spaceMinX else mbb.minX / thetaX
			val maxX = if (mbb.maxX / thetaX == globalMaxX) spaceMaxX else mbb.maxX / thetaX
			val minY = if (mbb.minY / thetaY == globalMinY) spaceMinY else mbb.minY / thetaY
			val maxY = if (mbb.maxY / thetaY == globalMaxY) spaceMaxY else 	mbb.maxY / thetaY

			MBR(maxX, minX, maxY, minY)
		})
	}

	def getSpaceEdges: MBR ={
		val (thetaX, thetaY) = thetaXY
		val minX = math.floor(partitionsZones.map(p => p.minX / thetaX).min).toInt - 1
		val maxX = math.ceil(partitionsZones.map(p => p.maxX / thetaX).max).toInt + 1
		val minY = math.floor(partitionsZones.map(p => p.minY / thetaY).min).toInt - 1
		val maxY = math.ceil(partitionsZones.map(p => p.maxY / thetaY).max).toInt + 1
		MBR(maxX, minX, maxY, minY)
	}

	def normalizeWeight(weight: Double, geom1: Geometry, geom2: Geometry): Double ={
		val area1 = geom1.getArea
		val area2 = geom2.getArea
		if (area1 == 0 || area2 == 0 ) weight
		else weight/(geom1.getArea * geom2.getArea)
	}


	def printPartition(joinedRDD: RDD[(Int, (Iterable[Entity],  Iterable[Entity]))]): Unit ={
		val c = joinedRDD.map(p => (p._1, (p._2._1.size, p._2._2.size))).sortByKey().collect()
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

}