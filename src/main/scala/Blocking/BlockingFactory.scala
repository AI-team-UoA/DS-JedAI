package Blocking

import DataStructures.Entity
import org.apache.spark.rdd.RDD
import utils.Constants.BlockingAlgorithm
import utils.Configuration

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
object BlockingFactory {

	def getBlocking(conf: Configuration, source: RDD[Entity], target: RDD[Entity], spatialPartitioned: Boolean = false): Blocking = {
		val theta_msr = conf.getTheta

		if (spatialPartitioned) {
			return PartitionBlocking(source, target, theta_msr)
		}
		val algorithm = conf.getBlockingAlgorithm
		algorithm match {
			case BlockingAlgorithm.STATIC_BLOCKING =>
				val blockingFactor: Int = conf.getBlockingFactor
				val distance: Double = conf.getBlockingDistance
				StaticBlocking(source, target, theta_msr, blockingFactor, distance)
			case BlockingAlgorithm.RADON| _ =>
				RADON(source, target, theta_msr)
		}
	}
}
