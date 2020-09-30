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
            case MatchingAlgorithm.PROGRESSIVE_GIANT =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.PROGRESSIVE_GIANT)
                ProgressiveGIAnt(source, target, theta_msr, ws, budget)
            case MatchingAlgorithm.ΕΝΤΙΤΥ_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.ΕΝΤΙΤΥ_CENTRIC)
                EntityCentricPrioritization(source, target, theta_msr, ws, budget)
            case MatchingAlgorithm.TOPK =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.TOPK)
                TopKPairs(source, target, theta_msr, ws, budget)
            case MatchingAlgorithm.RECIPROCAL_TOPK =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.RECIPROCAL_TOPK)
                ReciprocalTopK(source, target, theta_msr, ws, budget)
            case _ =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.GIANT)
                GIAnt(source, target, theta_msr)
        }
    }


    def getProgressiveAlgorithm(conf: Configuration, source: RDD[SpatialEntity], target: RDD[SpatialEntity], budgetArg: Int = -1,  wsArg: String = ""): ProgressiveTrait ={

        val algorithm = conf.getMatchingAlgorithm
        val theta_msr = conf.getTheta
        val budget = if(budgetArg > 0) budgetArg else conf.getBudget
        val ws:WeightStrategy = if(WeightStrategy.exists(wsArg.toString)) WeightStrategy.withName(wsArg) else conf.getWeightingScheme

        algorithm match {
            case MatchingAlgorithm.PROGRESSIVE_GIANT =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.PROGRESSIVE_GIANT)
                ProgressiveGIAnt(source, target, theta_msr, ws, budget)
            case MatchingAlgorithm.ΕΝΤΙΤΥ_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.ΕΝΤΙΤΥ_CENTRIC)
                EntityCentricPrioritization(source, target, theta_msr, ws, budget)
            case MatchingAlgorithm.RECIPROCAL_TOPK =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.RECIPROCAL_TOPK)
                ReciprocalTopK(source, target, theta_msr, ws, budget)
            case MatchingAlgorithm.TOPK =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.TOPK)
                TopKPairs(source, target, theta_msr, ws, budget)
        }
    }
}
