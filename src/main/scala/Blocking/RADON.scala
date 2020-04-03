package Blocking

import DataStructures.{Block, SpatialEntity, TBlock}
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
					(env.getHeight, env.getWidth)
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
	 * only to blocks that exist in the accepted blocks set
	 *
	 * @param spatialEntitiesRDD the set to index
	 * @param acceptedBlocks the accepted blocks that the set can be indexed to
	 * @return an Array of block ids for each spatial entity
	 */
	def index(spatialEntitiesRDD: RDD[SpatialEntity], acceptedBlocks: Set[(Int, Int)] = Set()): RDD[((Int, Int), ArrayBuffer[SpatialEntity])] ={
		val acceptedBlocksBD = SparkContext.getOrCreate().broadcast(acceptedBlocks)
		broadcastMap += ("acceptedBlocks" -> acceptedBlocksBD.asInstanceOf[Broadcast[Any]])
		spatialEntitiesRDD.mapPartitions { seIter =>
			val thetaMsr = broadcastMap("theta").value.asInstanceOf[(Double, Double)]
			val acceptedBlocks = acceptedBlocksBD.value
			seIter.map(se => (indexSpatialEntity(se, acceptedBlocks, thetaMsr), se))
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
