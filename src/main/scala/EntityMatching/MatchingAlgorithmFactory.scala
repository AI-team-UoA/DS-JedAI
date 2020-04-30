package EntityMatching

import EntityMatching.prioritization.{BlockCentricPrioritization, ComparisonCentricPrioritization, EntityCentricPrioritization}
import utils.{Configuration, Constants}

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

object MatchingAlgorithmFactory {

    def getMatchingAlgorithm(conf: Configuration, totalBlocks: Long): MatchingTrait = {
        val algorithm = conf.configurations.getOrElse(Constants.CONF_PRIORITIZATION_ALG, Constants.BLOCK_CENTRIC)
        val weightingStrategy = conf.configurations.getOrElse(Constants.CONF_WEIGHTING_STRG, Constants.CBS)
        algorithm match {
            case Constants.COMPARISON_CENTRIC =>
                ComparisonCentricPrioritization(totalBlocks, weightingStrategy)
            case Constants.ΕΝΤΙΤΥ_CENTRIC =>
                EntityCentricPrioritization(totalBlocks, weightingStrategy)
            case Constants.BLOCK_CENTRIC =>
                BlockCentricPrioritization(totalBlocks, weightingStrategy)
            case _=>
                SpatialMatching(totalBlocks)
        }
    }

}
