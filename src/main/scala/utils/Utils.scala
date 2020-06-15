package utils


import DataStructures.{MBB, SpatialEntity}
import com.vividsolutions.jts.geom.Geometry
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import utils.Readers.SpatialReader

import scala.reflect.ClassTag

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
object Utils {

	val spark: SparkSession = SparkSession.builder().getOrCreate()
	var swapped = false
	var thetaXY: (Double, Double) = _
	var sourceCount: Long = _
	var targetCount: Long = _
	val log: Logger = LogManager.getRootLogger
	/**
	 * Cantor Pairing function. Map two positive integers to a unique integer number.
	 *
	 * @param a Long
	 * @param b Long
	 * @return the unique mapping of the integers
	 */
	def cantorPairing(a: Long, b: Long): Long =  (((a + b) * (a + b + 1))/2) + b

	/**
	 * Bijective cantor pairing. CantorPairing(x, y) == CantorPairing(y, x)
	 *
	 * @param x integer
	 * @param y integer
	 * @return the unique mapping of the integers
	 */
	def bijectivePairing(x: Long, y: Long): Long ={
		if (x < y)
			cantorPairing(y, x)
		else
			cantorPairing(y, x)
	}

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

	def inversePairing(z: Long): (Double, Double) ={
		val x = (-1 + math.sqrt(1 + 8 * z))/2
		val floorX = math.floor(x)
		val a = z - (floorX*(1+floorX))/2
		val b = (floorX*(3+floorX)/2) - z
		(a,b)
	}

	/**
	 * Compute the Estimation of the Total Hyper-volume
	 *
	 * @param seRDD Spatial Entities
	 * @return Estimation of the Total Hyper-volume
	 */
	def getETH(seRDD: RDD[SpatialEntity]): Double ={
		getETH(seRDD, seRDD.count())
	}


	/**
	 * Compute the Estimation of the Total Hyper-volume
	 *
	 * @param seRDD Spatial Entities
	 * @param count number of the entities
	 * @return Estimation of the Total Hyper-volume
	 */
	def getETH(seRDD: RDD[SpatialEntity], count: Double): Double ={
		val denom = 1/count
		val coords_sum = seRDD
			.map(se => (se.mbb.maxX - se.mbb.minX, se.mbb.maxY - se.mbb.minY))
			.fold((0, 0)) { case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2) }

		val eth = count * ( (denom * coords_sum._1) * (denom * coords_sum._2) )
		eth
	}

	/**
	 * Swaps source to the set with the smallest ETH, and change the relation respectively.
	 *
	 * @param sourceRDD source
	 * @param targetRDD target
	 * @param relation relation
	 * @return the swapped values
	 */
	def swappingStrategy(sourceRDD: RDD[SpatialEntity], targetRDD: RDD[SpatialEntity], relation: String,
						 scount: Long = -1, tcount: Long = -1):	(RDD[SpatialEntity], RDD[SpatialEntity], String)= {

		sourceCount = if (scount > 0) scount else sourceRDD.count()
		targetCount = if (tcount > 0) tcount else targetRDD.count()
		val sourceETH = getETH(sourceRDD, sourceCount)
		val targetETH = getETH(targetRDD, targetCount)

		if (targetETH < sourceETH){
			swapped = true
			val temp = sourceCount
			sourceCount = targetCount
			targetCount = temp

			val newRelation =
				relation match {
					case Constants.WITHIN => Constants.CONTAINS
					case Constants.CONTAINS => Constants.WITHIN
					case Constants.COVERS => Constants.COVEREDBY
					case Constants.COVEREDBY => Constants.COVERS;
					case _ => relation
				}
			(targetRDD, sourceRDD, newRelation)
		}
		else
			(sourceRDD, targetRDD, relation)
	}

	implicit def singleSTR[A](implicit c: ClassTag[String]): Encoder[String] = Encoders.STRING
	implicit def singleInt[A](implicit c: ClassTag[Int]): Encoder[Int] = Encoders.scalaInt
	implicit def tuple[String, Int](implicit e1: Encoder[String], e2: Encoder[Int]): Encoder[(String,Int)] = Encoders.tuple[String,Int](e1, e2)
	def printPartitions(rdd: RDD[Any]): Unit ={
		spark.createDataset(rdd.mapPartitionsWithIndex{ case (i,rows) => Iterator((i,rows.size))}).show(100)
	}

	def export(rdd: RDD[SpatialEntity], path:String): Unit ={
		val schema = StructType(
			StructField("id", IntegerType, nullable = true) ::
			StructField("wkt", StringType, nullable = true)  :: Nil
		)
		val rowRDD: RDD[Row] = rdd.map(s => new GenericRowWithSchema(Array(TaskContext.getPartitionId(), s.geometry.toText), schema))
		val df = spark.createDataFrame(rowRDD, schema)
		df.write.option("header", "true").csv(path)
	}


	/**
	 * initialize theta based on theta measure
	 */
	def initTheta(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaMsrSTR: String): (Double, Double) ={
		thetaXY =
			thetaMsrSTR match {
				case Constants.MIN =>
					// need filtering because there are cases where the geometries are perpendicular to the axes
					// hence its width or height is equal to 0.0
					val union = source.union(target)
					val thetaX = union.map(se => se.mbb.maxX - se.mbb.minX).filter(_ != 0.0d).min
					val thetaY = union.map(se => se.mbb.maxY - se.mbb.minY).filter(_ != 0.0d).min
					(thetaX, thetaY)
				case Constants.MAX =>
					val union = source.union(target)
					val thetaX = union.map(se => se.mbb.maxX - se.mbb.minX).max
					val thetaY = union.map(se => se.mbb.maxY - se.mbb.minY).max
					(thetaX, thetaY)
				case Constants.AVG =>
					val union = source.union(target)
					val total = sourceCount + targetCount
					val thetaX = union.map(se => se.mbb.maxX - se.mbb.minX).sum() / total
					val thetaY = union.map(se => se.mbb.maxY - se.mbb.minY).sum() / total
					(thetaX, thetaY)
				case Constants.AVG_x2 =>
					val sourceX = source.map(se => se.mbb.maxX - se.mbb.minX).sum()
					val sourceY = source.map(se => se.mbb.maxY - se.mbb.minY).sum()
					val thetaXs = sourceX / sourceCount
					val thetaYs = sourceY / sourceCount

					val targetX = target.map(se => se.mbb.maxX - se.mbb.minX).sum()
					val targetY = target.map(se => se.mbb.maxY - se.mbb.minY).sum()
					val thetaXt = targetX / targetCount
					val thetaYt = targetY / targetCount

					val thetaX = 0.5 * (thetaXs + thetaXt)
					val thetaY = 0.5 * (thetaYs + thetaYt)
					(thetaX, thetaY)
				case _ =>
					(1d, 1d)
			}
		thetaXY
	}


	def getZones: Array[MBB] ={
		val partitionsZones = SpatialReader.partitionsZones
		val (thetaX, thetaY) = thetaXY

		partitionsZones.map(mbb => {
			val maxX = math.ceil(mbb.maxX / thetaX).toInt
			val minX = math.floor(mbb.minX / thetaX).toInt
			val maxY = math.ceil(mbb.maxY / thetaY).toInt
			val minY = math.floor(mbb.minY / thetaY).toInt

			MBB(maxX, minX, maxY, minY)
		})
	}

	def getSpaceEdges: MBB ={
		val (thetaX, thetaY) = thetaXY
		val minX = SpatialReader.partitionsZones.map(p => p.minX / thetaX).min
		val maxX = SpatialReader.partitionsZones.map(p => p.maxX / thetaX).max
		val minY = SpatialReader.partitionsZones.map(p => p.minY / thetaY).min
		val maxY = SpatialReader.partitionsZones.map(p => p.maxY / thetaY).max
		MBB(maxX, minX, maxY, minY)
	}

	def normalizeWeight(weight: Double, geom1: Geometry, geom2: Geometry): Double ={
		val area1 = geom1.getArea
		val area2 = geom2.getArea
		if (area1 == 0 || area2 == 0 ) weight
		else weight/(geom1.getArea * geom2.getArea)
	}


	def printPartition(joinedRDD: RDD[(Int, (Iterable[SpatialEntity],  Iterable[SpatialEntity]))]): Unit ={
		val c = joinedRDD.map(p => (p._1, (p._2._1.size, p._2._2.size))).sortByKey().collect()
		log.info("Printing Partitions")
		log.info("----------------------------------------------------------------------------")
		c.foreach(p => log.info(p._1 + " ->  (" + p._2._1 + ", " + p._2._2 +  ")" ))
		log.info("----------------------------------------------------------------------------")

	}

}