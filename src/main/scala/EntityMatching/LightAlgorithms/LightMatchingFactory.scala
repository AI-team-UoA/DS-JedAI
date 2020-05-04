package EntityMatching.LightAlgorithms

import DataStructures.SpatialEntity
import EntityMatching.LightAlgorithms.prioritization.{ComparisonCentricPrioritization, EntityCentricPrioritization}
import org.apache.spark.rdd.RDD
import utils.{Configuration, Constants}

object LightMatchingFactory {

    def getMatchingAlgorithm(conf: Configuration, source: RDD[SpatialEntity], target: RDD[SpatialEntity]): LightMatchingTrait = {
        val algorithm = conf.configurations.getOrElse(Constants.CONF_MATCHING_ALG, Constants.BLOCK_CENTRIC)
        val weightingStrategy = conf.configurations.getOrElse(Constants.CONF_WEIGHTING_STRG, Constants.CBS)
        val theta_msr = conf.configurations.getOrElse(Constants.CONF_THETA_MEASURE, Constants.NO_USE)
        algorithm match {
            case Constants.COMPARISON_CENTRIC =>
                ComparisonCentricPrioritization(source, target, theta_msr, weightingStrategy)
            case Constants.ΕΝΤΙΤΥ_CENTRIC =>
                EntityCentricPrioritization(source, target, theta_msr, weightingStrategy)
            case _=>
                LightRADON(source, target, theta_msr)
        }
    }
}
