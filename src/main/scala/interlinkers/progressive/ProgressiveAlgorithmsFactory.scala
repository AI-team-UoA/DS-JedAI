package interlinkers.progressive

import model.MBR
import model.entities.Entity
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import utils.Constants.ProgressiveAlgorithm.ProgressiveAlgorithm
import utils.Constants.WeightingFunction.WeightingFunction
import utils.Constants.ProgressiveAlgorithm
import utils.Constants

object ProgressiveAlgorithmsFactory {


    def get(matchingAlgorithm: ProgressiveAlgorithm, source: RDD[(Int, Entity)], target: RDD[(Int, Entity)],
            thetaXY: (Double, Double), partitionBorders: Array[MBR], partitioner: Partitioner, sourceCount: Long,
            budget: Int = 0, mainWF: WeightingFunction, secondaryWF: Option[WeightingFunction],
            ws: Constants.WeightingScheme ):
    ProgressiveInterlinkerT ={

        matchingAlgorithm match {
            case ProgressiveAlgorithm.RANDOM =>
                RandomScheduling(source, target, thetaXY, partitionBorders, sourceCount, mainWF, secondaryWF, budget, partitioner, ws)
            case ProgressiveAlgorithm.TOPK =>
                TopKPairs(source, target, thetaXY, partitionBorders, sourceCount, mainWF, secondaryWF, budget, partitioner, ws)
            case ProgressiveAlgorithm.RECIPROCAL_TOPK =>
                ReciprocalTopK(source, target, thetaXY, partitionBorders, sourceCount, mainWF, secondaryWF, budget, partitioner, ws)
            case ProgressiveAlgorithm.DYNAMIC_PROGRESSIVE_GIANT =>
                DynamicProgressiveGIAnt(source, target, thetaXY, partitionBorders, sourceCount, mainWF, secondaryWF, budget, partitioner, ws)
            case ProgressiveAlgorithm.PROGRESSIVE_GIANT | _ =>
                ProgressiveGIAnt(source, target, thetaXY, partitionBorders, sourceCount, mainWF, secondaryWF, budget, partitioner, ws)
        }
    }
}
