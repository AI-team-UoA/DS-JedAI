package Blocking

import DataStructures.{Block, SpatialEntity}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.Constants

/**
 * @author George MAndilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
class RADON	(var source: RDD[SpatialEntity], var target: RDD[SpatialEntity], theta_msr: String) extends  Blocking with Serializable
{
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



	def index(spatialEntitiesRDD: RDD[SpatialEntity], acceptedBlocks: Set[(Int, Int)] = Set()): RDD[((Int, Int), Array[Int])] ={
		val blocks = spatialEntitiesRDD
			.map {
			se =>
				val (thetaX, thetaY) = broadcastMap("theta").value.asInstanceOf[(Double, Double)]
				val seID = se.id
				var blockIDs: Array[(Int, Int)] = Array()

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

					(wmbb_minX to wmbb_maxX).map(x => (wmbb_minY to wmbb_maxY).map(y => blockIDs :+= (x, y)))

					(embb_minX to embb_maxX).map(x => (embb_minY to embb_maxY).map(y => blockIDs :+= (x, y)))
				}
				else {
					val maxX = math.ceil(se.mbb.maxX / thetaX).toInt
					val minX = math.floor(se.mbb.minX / thetaX).toInt
					val maxY = math.ceil(se.mbb.maxY / thetaY).toInt
					val minY = math.floor(se.mbb.minY / thetaY).toInt

					(minX to maxX).map(x => (minY to maxY).map(y => blockIDs :+= (x, y)))
				}
				(blockIDs, seID)
			}

			if (acceptedBlocks.nonEmpty) {
				// filters out blocks that don't contain entities of the source
				val acceptedBlocksBD = SparkContext.getOrCreate().broadcast(acceptedBlocks)
				broadcastMap += ("acceptedBlocks" -> acceptedBlocksBD.asInstanceOf[Broadcast[Any]])
				blocks.flatMap(p => p._1.filter(acceptedBlocksBD.value.contains).map(blockID => (blockID, Array(p._2)))).reduceByKey(_ ++ _)
			}
			else blocks.flatMap(p => p._1.map(blockID => (blockID, Array(p._2)))).reduceByKey(_++_)
	}


	override def apply(): RDD[Block] = {
		initTheta()
		super.apply()
	}

}
