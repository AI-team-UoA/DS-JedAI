package geospatialInterlinking.progressive

import dataModel.Entity
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import utils.Constants.ProgressiveAlgorithm.ProgressiveAlgorithm
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Constants.{ProgressiveAlgorithm, WeightStrategy}

object ProgressiveAlgorithmsFactory {


    def get(matchingAlgorithm: ProgressiveAlgorithm, source: RDD[(Int, Entity)], target: RDD[(Int, Entity)],
            partitioner: Partitioner, budget: Int = 0, ws: WeightStrategy = WeightStrategy.JS): ProgressiveGeospatialInterlinkingT ={

        matchingAlgorithm match {
            case ProgressiveAlgorithm.RANDOM =>
                RandomScheduling(source, target, ws, budget, partitioner)
            case ProgressiveAlgorithm.GEOMETRY_CENTRIC =>
                GeometryCentric(source, target, ws, budget, partitioner)
            case ProgressiveAlgorithm.TOPK =>
                TopKPairs(source, target, ws, budget, partitioner)
            case ProgressiveAlgorithm.RECIPROCAL_TOPK =>
                ReciprocalTopK(source, target, ws, budget, partitioner)
            case ProgressiveAlgorithm.PROGRESSIVE_GIANT | _ =>
                ProgressiveGIAnt(source, target, ws, budget, partitioner)
        }
    }
}
