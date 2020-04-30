package EntityMatching.prioritization

import utils.{Configuration, Constants}


/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

object PrioritizationAlgorithmFactory {

    def getPrioritizationAlgorithm(conf: Configuration, totalBlocks: Long): PrioritizationTrait = {
        val algorithm = conf.configurations.getOrElse(Constants.CONF_PRIORITIZATION_ALG, Constants.BLOCK_CENTRIC)
        val weightingStrategy = conf.configurations.getOrElse(Constants.CONF_WEIGHTING_STRG, Constants.CBS)
        algorithm match {
            case Constants.COMPARISON_CENTRIC =>
                ComparisonCentricPrioritization(totalBlocks, weightingStrategy)
            case Constants.ΕΝΤΙΤΥ_CENTRIC =>
                EntityCentricPrioritization(totalBlocks, weightingStrategy)
            case Constants.BLOCK_CENTRIC|_ =>
                BlockCentricPrioritization(totalBlocks, weightingStrategy)
        }
    }

}
