package Blocking

import DataStructures.{Block, SpatialEntity}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.Constants

/**
 * @author George MAndilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
case class StaticBlocking (var source: RDD[SpatialEntity], var target: RDD[SpatialEntity], distance: Double, blockingParameter: Double) extends  Blocking with Serializable {

	def index(spatialEntitiesRDD: RDD[SpatialEntity], acceptedBlocks: Set[(Int, Int)] = Set()): RDD[((Int, Int), Array[Int])] = {

		val blocks = spatialEntitiesRDD.map {
			se =>
				val envelope = se.geometry.getEnvelopeInternal
				if (distance != 0.0)
					envelope.expandBy((distance / Constants.EARTH_CIRCUMFERENCE_EQUATORIAL) * Constants.LONG_RANGE, (distance / Constants.EARTH_CIRCUMFERENCE_MERIDIONAL) * Constants.LAT_RANGE)

				val minLatBlock = (envelope.getMinY*blockingParameter).toInt
				val maxLatBlock = (envelope.getMaxY*blockingParameter).toInt
				val minLongBlock = (envelope.getMinX*blockingParameter).toInt
				val maxLongBlock = (envelope.getMaxX*blockingParameter).toInt

				//TODO: crosses meridian case
				val blockIDs = for(x <- minLongBlock to maxLongBlock; y <- minLatBlock to maxLatBlock) yield (x, y)
				(blockIDs, se.id)
		}
		if (acceptedBlocks.nonEmpty) {
			// filters out blocks that don't contain entities of the source
			val acceptedBlocksBD = SparkContext.getOrCreate().broadcast(acceptedBlocks)
			broadcastMap += ("acceptedBlocks" -> acceptedBlocksBD.asInstanceOf[Broadcast[Any]])
			blocks.flatMap(p => p._1.filter(acceptedBlocksBD.value.contains).map(blockID => (blockID, Array(p._2)))).reduceByKey(_ ++ _)
		}
		else blocks.flatMap(p => p._1.map(blockID => (blockID, Array(p._2)))).reduceByKey(_++_)
	}


}
