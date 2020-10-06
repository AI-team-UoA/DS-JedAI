package EntityMatching.SemiDistributedMatching

import DataStructures.SpatialEntity
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import utils.Constants.{BlockingAlgorithm, MatchingAlgorithm}
import utils.Configuration

object SDMFactory {

    val log: Logger = LogManager.getRootLogger

    def getMatchingAlgorithm(conf: Configuration, source: RDD[SpatialEntity], target: RDD[SpatialEntity]): SDMTrait = {
        val algorithm = conf.getMatchingAlgorithm
        val ws = conf.getWeightingScheme
        val theta_msr = conf.getTheta
        algorithm match {
            case MatchingAlgorithm.PROGRESSIVE_GIANT =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.PROGRESSIVE_GIANT)
                ComparisonCentricPrioritization(source, target, theta_msr, ws)
            case MatchingAlgorithm.ΕΝΤΙΤΥ_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.ΕΝΤΙΤΥ_CENTRIC)
                EntityCentricPrioritization(source, target, theta_msr, ws)
            case _=>
                log.info("Matching Algorithm: " + BlockingAlgorithm.LIGHT_RADON)
                LightRADON(source, target, theta_msr)
        }
    }
}
