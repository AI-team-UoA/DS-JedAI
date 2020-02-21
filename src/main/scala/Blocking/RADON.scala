package Blocking

import DataStructures.{Block, SpatialEntity}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.Constants

import scala.collection.mutable.ArrayBuffer

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

/**
 * RADON blocking algorithm
 * @param source source set as RDD
 * @param target target set as RDD
 * @param theta_msr theta measure
 */
case class RADON(var source: RDD[SpatialEntity], var target: RDD[SpatialEntity], theta_msr: String) extends  Blocking with Serializable
{

	/**
	 * initialize theta based on theta measure
	 */
	def initTheta(): Unit ={
		val thetaMsr: RDD[(Double, Double)] = source
			.union(target)
			.map {
				sp =>
					val env = sp.geometry.getEnvelopeInternal
					(env.getHeight, env.getHeight)
			}
    		.setName("thetaMsr")
    		.cache()

		var thetaX = 1d
		var thetaY = 1d
		theta_msr match {
			// WARNING: small or big values of theta may affect negatively the indexing procedure
			case Constants.MIN =>
				// filtering because there are cases that the geometries are perpendicular to the axes
				// and have width or height equals to 0.0
				thetaX = thetaMsr.map(_._1).filter(_ != 0.0d).min
				thetaY = thetaMsr.map(_._2).filter(_ != 0.0d).min
			case Constants.MAX =>
				thetaX = thetaMsr.map(_._1).max
				thetaY = thetaMsr.map(_._2).max
			case Constants.AVG =>
				val length = thetaMsr.count
				thetaX = thetaMsr.map(_._1).sum() / length
				thetaY = thetaMsr.map(_._2).sum() / length
			case _ =>
		}
		val broadcastedTheta = SparkContext.getOrCreate().broadcast((thetaX, thetaY))
		broadcastMap += ("theta" -> broadcastedTheta.asInstanceOf[Broadcast[Any]])
		thetaMsr.unpersist()
	}


	/**
	 * index a spatial entities set. If acceptedBlocks is provided then the entities will be assigned
	 * to blocks that exist in the accepted blocks
	 *
	 * @param spatialEntitiesRDD the set to index
	 * @param acceptedBlocks the accepted blocks that the set can be indexed to
	 * @return an Array of block ids for each spatial entity
	 */
	def index(spatialEntitiesRDD: RDD[SpatialEntity], acceptedBlocks: Set[(Int, Int)] = Set()): RDD[((Int, Int), ArrayBuffer[SpatialEntity])] ={
		val acceptedBlocksBD = SparkContext.getOrCreate().broadcast(acceptedBlocks)
		broadcastMap += ("acceptedBlocks" -> acceptedBlocksBD.asInstanceOf[Broadcast[Any]])
		spatialEntitiesRDD.map { se =>
			val (thetaX, thetaY) = broadcastMap("theta").value.asInstanceOf[(Double, Double)]

			val blockIDs = {
				// Split on Meridian and index on eastern and western mbb
				if (se.crossesMeridian) {
					val (westernMBB, easternMBB) = se.mbb.splitOnMeridian

					val wmbb_maxX = math.ceil(westernMBB.maxX / thetaX).toInt
					val wmbb_minX = math.floor(westernMBB.minX / thetaX).toInt
					val wmbb_maxY = math.ceil(westernMBB.maxY / thetaY).toInt
					val wmbb_minY = math.floor(westernMBB.minY / thetaY).toInt

					val embb_maxX = math.ceil(easternMBB.maxX / thetaX).toInt
					val embb_minX = math.floor(easternMBB.minX / thetaX).toInt
					val embb_maxY = math.ceil(easternMBB.maxY / thetaY).toInt
					val embb_minY = math.floor(easternMBB.minY / thetaY).toInt

					if (acceptedBlocksBD.value.nonEmpty) {
						(for (x <- wmbb_minX to wmbb_maxX; y <- wmbb_minY to wmbb_maxY; if acceptedBlocksBD.value.contains((x, y))) yield (x, y)) ++
						(for (x <- embb_minX to embb_maxX; y <- embb_minY to embb_maxY; if acceptedBlocksBD.value.contains((x, y))) yield (x, y))
					}
					else
						(for (x <- wmbb_minX to wmbb_maxX; y <- wmbb_minY to wmbb_maxY) yield (x, y)) ++ (for (x <- embb_minX to embb_maxX; y <- embb_minY to embb_maxY) yield (x, y))
				}
				else {
					val maxX = math.ceil(se.mbb.maxX / thetaX).toInt
					val minX = math.floor(se.mbb.minX / thetaX).toInt
					val maxY = math.ceil(se.mbb.maxY / thetaY).toInt
					val minY = math.floor(se.mbb.minY / thetaY).toInt

					for (x <- minX to maxX; y <- minY to maxY) yield (x, y)
				}
			}
			(blockIDs, se)
		}
		.flatMap(b => b._1.map(id => (id, ArrayBuffer[SpatialEntity](b._2) ))).reduceByKey(_ ++ _ )
	}

	/**
	 * Apply indexing
	 * @return RDD of blocks
	 */
	override def apply(): RDD[Block] = {
		initTheta()
		super.apply()
	}

}
