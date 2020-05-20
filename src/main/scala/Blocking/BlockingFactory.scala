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
		val theta_msr = conf.configurations.getOrElse(Constants.CONF_THETA_MEASURE, Constants.NO_USE)

		if (spatialPartitioned) {
			val blocking =  PartitionBlocking(source, target, theta_msr)
			blocking.setPartitionsZones(SpatialReader.partitionsZones)
			return blocking
		}
		val algorithm = conf.configurations.getOrElse(Constants.CONF_BLOCK_ALG, Constants.RADON)
		algorithm match {
			case Constants.STATIC_BLOCKING =>
				val blockingFactor: Int = conf.configurations.getOrElse(Constants.CONF_SPATIAL_BLOCKING_FACTOR, "10").toInt
				val distance: Double = conf.configurations.getOrElse(Constants.CONF_STATIC_BLOCKING_DISTANCE, "0.0").toDouble
				StaticBlocking(source, target, blockingFactor, distance)
			// case Constants.LIGHT_RADON =>
			//	val theta_msr = conf.configurations.getOrElse(Constants.CONF_THETA_MEASURE, Constants.NO_USE)
			//	LightRADON(source, target, theta_msr)
			case Constants.RADON| _ =>
				RADON(source, target, theta_msr)
		}
	}
}
