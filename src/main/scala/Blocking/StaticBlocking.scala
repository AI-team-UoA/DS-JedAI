package Blocking

import DataStructures.SpatialEntity
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.{Constants, Utils}

import scala.collection.mutable.ArrayBuffer

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
case class StaticBlocking (source: RDD[SpatialEntity], target: RDD[SpatialEntity], thetaXY: (Double, Double),
						   blockingParameter: Double, distance: Double) extends  Blocking with Serializable {


	def index(spatialEntitiesRDD: RDD[SpatialEntity], acceptedBlocks: Set[(Int, Int)] = Set()): RDD[((Int, Int), ArrayBuffer[SpatialEntity])] = {

		val acceptedBlocksBD = SparkContext.getOrCreate().broadcast(acceptedBlocks)
		broadcastMap += ("acceptedBlocks" -> acceptedBlocksBD.asInstanceOf[Broadcast[Any]])

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
				val blockIDs =
					if (acceptedBlocksBD.value.nonEmpty)
						for(x <- minLongBlock to maxLongBlock; y <- minLatBlock to maxLatBlock;  if acceptedBlocksBD.value.contains((x, y))) yield (x, y)
					else
						for(x <- minLongBlock to maxLongBlock; y <- minLatBlock to maxLatBlock) yield (x, y)

				(blockIDs, se)
		}
		blocks.flatMap(p => p._1.map(blockID => (blockID, ArrayBuffer(p._2)))).reduceByKey(_++_)
	}
}

object StaticBlocking{
	def apply(source: RDD[SpatialEntity], target: RDD[SpatialEntity], thetaMsrSTR: String, blockingParameter: Double, distance: Double): StaticBlocking={
		val thetaXY = Utils.initTheta(source, target, thetaMsrSTR)
		StaticBlocking(source, target, thetaXY, blockingParameter, distance)
	}
}
