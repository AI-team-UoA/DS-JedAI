package EntityMatching.DistributedMatching

import DataStructures.SpatialEntity
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner
import utils.Constants.{MatchingAlgorithm, WeightStrategy}
import utils.Configuration
import utils.Constants.WeightStrategy.WeightStrategy

object DMFactory {

    val log: Logger = LogManager.getRootLogger

    def getMatchingAlgorithm(conf: Configuration, source: RDD[SpatialEntity], target: RDD[SpatialEntity],
                             partitioner: Option[SpatialPartitioner]=None,
                             budgetArg: Int = -1, wsArg: String = "", ma: String = ""): DMTrait ={

        val algorithm = if(MatchingAlgorithm.exists(ma)) MatchingAlgorithm.withName(ma) else conf.getMatchingAlgorithm
        val budget = if(budgetArg > 0) budgetArg else conf.getBudget
        val ws: WeightStrategy = if(WeightStrategy.exists(wsArg.toString)) WeightStrategy.withName(wsArg) else conf.getWeightingScheme

        algorithm match {
            case MatchingAlgorithm.PROGRESSIVE_GIANT =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.PROGRESSIVE_GIANT)
                ProgressiveGIAnt(source, target, ws, budget, partitioner.get)
            case MatchingAlgorithm.GEOMETRY_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.GEOMETRY_CENTRIC)
                EntityCentricPrioritization(source, target, ws, budget, partitioner.get)
            case MatchingAlgorithm.TOPK =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.TOPK)
                TopKPairs(source, target, ws, budget, partitioner.get)
            case MatchingAlgorithm.RECIPROCAL_TOPK =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.RECIPROCAL_TOPK)
                ReciprocalTopK(source, target, ws, budget, partitioner.get)
            case _ =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.GIANT)
                GIAnt(source, target)
        }
    }


    def getProgressiveAlgorithm(conf: Configuration, source: RDD[SpatialEntity], target: RDD[SpatialEntity],
                                partitioner: SpatialPartitioner, budgetArg: Int = -1, wsArg: String = "",
                                ma: String = ""): DMProgressiveTrait ={

        val algorithm = if(MatchingAlgorithm.exists(ma)) MatchingAlgorithm.withName(ma) else conf.getMatchingAlgorithm
        val budget = if(budgetArg > 0) budgetArg else conf.getBudget
        val ws:WeightStrategy = if(WeightStrategy.exists(wsArg.toString)) WeightStrategy.withName(wsArg) else conf.getWeightingScheme

        algorithm match {
            case MatchingAlgorithm.PROGRESSIVE_GIANT =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.PROGRESSIVE_GIANT)
                ProgressiveGIAnt(source, target, ws, budget, partitioner)
            case MatchingAlgorithm.GEOMETRY_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.GEOMETRY_CENTRIC)
                EntityCentricPrioritization(source, target, ws, budget, partitioner)
            case MatchingAlgorithm.RECIPROCAL_TOPK =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.RECIPROCAL_TOPK)
                ReciprocalTopK(source, target, ws, budget, partitioner)
            case MatchingAlgorithm.TOPK =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.TOPK)
                TopKPairs(source, target, ws, budget, partitioner)
        }
    }
}
