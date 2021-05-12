package interlinkers.progressive

import model.{MBR, TileGranularities}
import model.entities.Entity
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import utils.Constants.ProgressiveAlgorithm.ProgressiveAlgorithm
import utils.Constants.WeightingFunction.WeightingFunction
import utils.Constants.ProgressiveAlgorithm
import utils.Constants

object ProgressiveAlgorithmsFactory {


    def get(matchingAlgorithm: ProgressiveAlgorithm, source: RDD[(Int, Entity)], target: RDD[(Int, Entity)],
            tileGranularities: TileGranularities, partitionBorders: Array[MBR], partitioner: Partitioner, sourceCount: Long,
            budget: Int = 0, mainWF: WeightingFunction, secondaryWF: Option[WeightingFunction],
            ws: Constants.WeightingScheme ):
    ProgressiveInterlinkerT ={

        matchingAlgorithm match {
            case ProgressiveAlgorithm.RANDOM =>
                RandomScheduling(source, target, tileGranularities, partitionBorders, sourceCount, mainWF, secondaryWF, budget, partitioner, ws)
            case ProgressiveAlgorithm.TOPK =>
                TopKPairs(source, target, tileGranularities, partitionBorders, sourceCount, mainWF, secondaryWF, budget, partitioner, ws)
            case ProgressiveAlgorithm.RECIPROCAL_TOPK =>
                ReciprocalTopK(source, target, tileGranularities, partitionBorders, sourceCount, mainWF, secondaryWF, budget, partitioner, ws)
            case ProgressiveAlgorithm.DYNAMIC_PROGRESSIVE_GIANT =>
                DynamicProgressiveGIAnt(source, target, tileGranularities, partitionBorders, sourceCount, mainWF, secondaryWF, budget, partitioner, ws)
            case ProgressiveAlgorithm.PROGRESSIVE_GIANT | _ =>
                ProgressiveGIAnt(source, target, tileGranularities, partitionBorders, sourceCount, mainWF, secondaryWF, budget, partitioner, ws)
        }
    }
}
