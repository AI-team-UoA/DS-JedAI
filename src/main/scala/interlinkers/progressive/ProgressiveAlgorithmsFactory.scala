package interlinkers.progressive

import model.Entity
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import utils.Constants.ProgressiveAlgorithm.ProgressiveAlgorithm
import utils.Constants.WeightingScheme.WeightingScheme
import utils.Constants.ProgressiveAlgorithm

object ProgressiveAlgorithmsFactory {


    def get(matchingAlgorithm: ProgressiveAlgorithm, source: RDD[(Int, Entity)], target: RDD[(Int, Entity)],
            partitioner: Partitioner, budget: Int = 0, mainWS: WeightingScheme,  secondaryWS: Option[WeightingScheme]):
    ProgressiveInterlinkerT ={

        matchingAlgorithm match {
            case ProgressiveAlgorithm.RANDOM =>
                RandomScheduling(source, target, mainWS, secondaryWS, budget, partitioner)
//            case ProgressiveAlgorithm.GEOMETRY_CENTRIC =>
//                GeometryCentric(source, target, ws, budget, partitioner)
            case ProgressiveAlgorithm.TOPK =>
                TopKPairs(source, target, mainWS, secondaryWS, budget, partitioner)
            case ProgressiveAlgorithm.RECIPROCAL_TOPK =>
                ReciprocalTopK(source, target, mainWS, secondaryWS, budget, partitioner)
            case ProgressiveAlgorithm.DYNAMIC_PROGRESSIVE_GIANT =>
                DynamicProgressiveGIAnt(source, target, mainWS, secondaryWS, budget, partitioner)
            case ProgressiveAlgorithm.PROGRESSIVE_GIANT | _ =>
                ProgressiveGIAnt(source, target, mainWS, secondaryWS, budget, partitioner)
        }
    }
}
