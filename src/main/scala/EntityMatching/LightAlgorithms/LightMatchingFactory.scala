package EntityMatching.LightAlgorithms

import DataStructures.SpatialEntity
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import utils.{Configuration, Constants}

object LightMatchingFactory {

    val log: Logger = LogManager.getRootLogger

    def getMatchingAlgorithm(conf: Configuration, source: RDD[SpatialEntity], target: RDD[SpatialEntity]): LightMatchingTrait = {
        val algorithm = conf.getMatchingAlgorithm
        val weightingStrategy = conf.getWeightingScheme
        val theta_msr = conf.getThetaMSR
        algorithm match {
            case Constants.COMPARISON_CENTRIC =>
                log.info("Matching Algorithm: " + Constants.COMPARISON_CENTRIC)
                ComparisonCentricPrioritization(source, target, theta_msr, weightingStrategy)
            case Constants.ΕΝΤΙΤΥ_CENTRIC =>
                log.info("Matching Algorithm: " + Constants.ΕΝΤΙΤΥ_CENTRIC)
                EntityCentricPrioritization(source, target, theta_msr, weightingStrategy)
            case _=>
                log.info("Matching Algorithm: " + Constants.LIGHT_RADON)
                LightRADON(source, target, theta_msr)
        }
    }
}
