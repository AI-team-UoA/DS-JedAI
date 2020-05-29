package Blocking

import DataStructures.SpatialEntity
import org.apache.spark.rdd.RDD
import utils.Readers.SpatialReader
import utils.{Configuration, Constants}

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
object BlockingFactory {

	def getBlocking(conf: Configuration, source: RDD[SpatialEntity], target: RDD[SpatialEntity], spatialPartitioned: Boolean = false): Blocking = {
		val theta_msr = conf.getThetaMSR

		if (spatialPartitioned) {
			val blocking =  PartitionBlocking(source, target, theta_msr)
			blocking.setPartitionsZones(SpatialReader.partitionsZones)
			return blocking
		}
		val algorithm = conf.getBlockingAlgorithm
		algorithm match {
			case Constants.STATIC_BLOCKING =>
				val blockingFactor: Int = conf.getBlockingFactor
				val distance: Double = conf.getBlockingDistance
				StaticBlocking(source, target, blockingFactor, distance)
			case Constants.RADON| _ =>
				RADON(source, target, theta_msr)
		}
	}
}
