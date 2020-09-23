package EntityMatching.PartitionMatching

import DataStructures.SpatialEntity
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import utils.Constants.{MatchingAlgorithm, WeightStrategy}
import utils.Configuration
import utils.Constants.WeightStrategy.WeightStrategy

object PartitionMatchingFactory {


    val log: Logger = LogManager.getRootLogger

    def getMatchingAlgorithm(conf: Configuration, source: RDD[SpatialEntity], target: RDD[SpatialEntity], budgetArg: Int = -1, wsArg: String = ""): PartitionMatchingTrait ={

        val algorithm = conf.getMatchingAlgorithm
        val theta_msr = conf.getTheta
        val budget = if(budgetArg > 0) budgetArg else conf.getBudget
        val ws: WeightStrategy = if(WeightStrategy.exists(wsArg.toString)) WeightStrategy.withName(wsArg) else conf.getWeightingScheme

        algorithm match {
            case MatchingAlgorithm.COMPARISON_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.COMPARISON_CENTRIC)
                ComparisonCentricPrioritization(source, target, theta_msr, ws, budget)
            case MatchingAlgorithm.ΕΝΤΙΤΥ_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.ΕΝΤΙΤΥ_CENTRIC)
                EntityCentricPrioritization(source, target, theta_msr, ws, budget)
            case MatchingAlgorithm.ITERATIVE_ΕΝΤΙΤΥ_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.ITERATIVE_ΕΝΤΙΤΥ_CENTRIC)
                IterativeEntityCentricPrioritization(source, target, theta_msr, ws, budget)
            case _ =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.SPATIAL)
                PartitionMatching(source, target, theta_msr)
        }
    }


    def getProgressiveAlgorithm(conf: Configuration, source: RDD[SpatialEntity], target: RDD[SpatialEntity], budgetArg: Int = -1,  wsArg: String = ""): ProgressiveTrait ={

        val algorithm = conf.getMatchingAlgorithm
        val theta_msr = conf.getTheta
        val budget = if(budgetArg > 0) budgetArg else conf.getBudget
        val ws:WeightStrategy = if(WeightStrategy.exists(wsArg.toString)) WeightStrategy.withName(wsArg) else conf.getWeightingScheme

        algorithm match {
            case MatchingAlgorithm.COMPARISON_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.COMPARISON_CENTRIC)
                ComparisonCentricPrioritization(source, target, theta_msr, ws, budget)
            case MatchingAlgorithm.ΕΝΤΙΤΥ_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.ΕΝΤΙΤΥ_CENTRIC)
                EntityCentricPrioritization(source, target, theta_msr, ws, budget)
            case MatchingAlgorithm.ITERATIVE_ΕΝΤΙΤΥ_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.ITERATIVE_ΕΝΤΙΤΥ_CENTRIC)
                IterativeEntityCentricPrioritization(source, target, theta_msr, ws, budget)
        }
    }
}
