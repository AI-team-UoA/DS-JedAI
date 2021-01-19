package EntityMatching.DistributedMatching

import DataStructures.Entity
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import utils.Constants.MatchingAlgorithm.MatchingAlgorithm
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Constants.{MatchingAlgorithm, WeightStrategy}

object DMFactory {

    val log: Logger = LogManager.getRootLogger

    def getMatchingAlgorithm(matchingAlgorithm: MatchingAlgorithm, source: RDD[(Int, Entity)], target: RDD[(Int, Entity)],
                             partitioner: Partitioner, budget: Int = 0, ws: WeightStrategy = WeightStrategy.JS): DMTrait ={

        matchingAlgorithm match {
            case MatchingAlgorithm.PROGRESSIVE_GIANT =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.PROGRESSIVE_GIANT)
                ProgressiveGIAnt(source, target, ws, budget, partitioner)
            case MatchingAlgorithm.GEOMETRY_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.GEOMETRY_CENTRIC)
                GeometryCentric(source, target, ws, budget, partitioner)
            case MatchingAlgorithm.TOPK =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.TOPK)
                TopKPairs(source, target, ws, budget, partitioner)
            case MatchingAlgorithm.RECIPROCAL_TOPK =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.RECIPROCAL_TOPK)
                ReciprocalTopK(source, target, ws, budget, partitioner)
            case _ =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.GIANT)
                GIAnt(source, target, partitioner)
        }
    }


    def getProgressiveAlgorithm(matchingAlgorithm: MatchingAlgorithm, source: RDD[(Int, Entity)], target: RDD[(Int, Entity)],
                                partitioner: Partitioner, budget: Int = 0, ws: WeightStrategy = WeightStrategy.JS): DMProgressiveTrait ={

        matchingAlgorithm match {
            case MatchingAlgorithm.PROGRESSIVE_GIANT =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.PROGRESSIVE_GIANT)
                ProgressiveGIAnt(source, target, ws, budget, partitioner)
            case MatchingAlgorithm.GEOMETRY_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.GEOMETRY_CENTRIC)
                GeometryCentric(source, target, ws, budget, partitioner)
            case MatchingAlgorithm.RECIPROCAL_TOPK =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.RECIPROCAL_TOPK)
                ReciprocalTopK(source, target, ws, budget, partitioner)
            case MatchingAlgorithm.TOPK =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.TOPK)
                TopKPairs(source, target, ws, budget, partitioner)
        }
    }
}
