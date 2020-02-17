package Blocking

import DataStructures.{Block, SpatialEntity}
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

				val blockCountLat = (Constants.LAT_RANGE*blockingParameter).toInt
				val blockCountLong = (Constants.LONG_RANGE*blockingParameter).toInt

				val minLatBlock = (envelope.getMinY*blockingParameter).toInt
				val maxLatBlock = (envelope.getMaxY*blockingParameter).toInt
				val minLongBlock = (envelope.getMinX*blockingParameter).toInt
				val maxLongBlock = (envelope.getMaxX*blockingParameter).toInt

				val latBlocks = (for (i <- minLatBlock to maxLatBlock) yield i).toSet
				val longBlocks = (for(i <- minLongBlock to maxLongBlock) yield i).toSet
		}
    		.collect()
		null

		//Index.oneDim(latBlocks, blockCountLat) conjunction Index.oneDim(longBlocks, blockCountLong)


	}


}
