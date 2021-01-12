package Blocking

import DataStructures.{Block, Entity}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.Constants.ThetaOption.ThetaOption
import utils.Utils

import scala.collection.mutable.ListBuffer

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

/**
 * RADON blocking algorithm
 * @param source source set as RDD
 * @param target target set as RDD
 * @param thetaXY theta granularity
 */
case class RADON(source: RDD[Entity], target: RDD[Entity], thetaXY: (Double, Double)) extends  Blocking with Serializable
{

	/**
	 * index a spatial entities set. If acceptedBlocks is provided then the entities will be assigned
	 * only to blocks that exist in the accepted blocks set
	 *
	 * @param spatialEntitiesRDD the set to index
	 * @param acceptedBlocks the accepted blocks that the set can be indexed to
	 * @return an Array of block ids for each spatial entity
	 */
	def index(spatialEntitiesRDD: RDD[Entity], acceptedBlocks: Set[(Int, Int)] = Set()): RDD[((Int, Int), Array[Entity])] ={
		val acceptedBlocksBD = SparkContext.getOrCreate().broadcast(acceptedBlocks)
		broadcastMap += ("acceptedBlocks" -> acceptedBlocksBD.asInstanceOf[Broadcast[Any]])
		spatialEntitiesRDD
			.mapPartitions { seIter =>
				val acceptedBlocks = acceptedBlocksBD.value
				if (acceptedBlocks.nonEmpty)
					seIter.map(se => (se.index(thetaXY, acceptedBlocks.contains), se))
				else
					seIter.map(se => (se.index(thetaXY), se))
			}
			.flatMap(b => b._1.map(id => (id, ListBuffer(b._2))))
			.reduceByKey(_ ++ _ )
    		.map(p => (p._1, p._2.to[Array]))
	}

	/**
	 * Apply indexing
	 * @return RDD of blocks
	 */
	override def apply(): RDD[Block] = {
		super.apply()
	}
}

object RADON{
	def apply(source: RDD[Entity], target: RDD[Entity], thetaOption: ThetaOption): RADON={
		val thetaXY = Utils.getTheta
		RADON(source, target, thetaXY)
	}
}