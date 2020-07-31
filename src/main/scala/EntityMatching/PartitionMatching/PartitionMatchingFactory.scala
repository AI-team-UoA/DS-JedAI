package EntityMatching.PartitionMatching

import DataStructures.SpatialEntity
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import utils.Constants.MatchingAlgorithm
import utils.{Configuration, Constants, Utils}

object PartitionMatchingFactory {


    val log: Logger = LogManager.getRootLogger

    def getMatchingAlgorithm(conf: Configuration, source: RDD[SpatialEntity], target: RDD[SpatialEntity]): PartitionMatchingTrait ={

        val algorithm = conf.getMatchingAlgorithm
        val ws = conf.getWeightingScheme
        val theta_msr = conf.getTheta
        val budget = conf.getBudget
        algorithm match {
            case MatchingAlgorithm.COMPARISON_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.COMPARISON_CENTRIC)
                ComparisonCentricPrioritization(source, target, theta_msr, ws, budget)
            case MatchingAlgorithm.ΕΝΤΙΤΥ_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.ΕΝΤΙΤΥ_CENTRIC)
                EntityCentricPrioritization(source, target, theta_msr, ws, budget)
            case MatchingAlgorithm.ITERATIVE_ΕΝΤΙΤΥ_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.ITERATIVE_ΕΝΤΙΤΥ_CENTRIC)
                IterativeEntityCentricPrioritization(source, target, theta_msr, ws)
            case _ =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.SPATIAL)
                PartitionMatching(source, target, theta_msr)
        }
    }
}
