//package geospatialInterlinking.progressive
//
//import dataModel.{ComparisonPQ, Entity, MBR, WeightedPairsPQ}
//import org.apache.spark.Partitioner
//import org.apache.spark.rdd.RDD
//import utils.Constants.Relation.Relation
//import utils.Constants.WeightingScheme.WeightingScheme
//import utils.Utils
//
//
//case class GeometryCentric(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))],
//                           thetaXY: (Double, Double), ws: WeightingScheme, budget: Int, sourceCount: Long)
//   extends ProgressiveGeospatialInterlinkingT {
//
//
//    /**
//     * For each target entity we keep only the top K comparisons, according to a weighting scheme.
//     * Then we assign the top K comparisons a common weight, which is their avg
//     * Based on this weight we prioritize their execution.
//     *
//     * @return  an RDD of Intersection Matrices
//     */
//    def prioritize(source: Array[Entity], target: Array[Entity], partition: MBR, relation: Relation): WeightedPairsPQ = {
//        val sourceIndex = index(source)
//        val filterIndices = (b: (Int, Int)) => sourceIndex.contains(b)
//        val k = (math.ceil(budget / target.length).toInt + 1) * 2 // +1 to avoid k=0
//        val targetPQ: ComparisonPQ[Int] = ComparisonPQ[Int](k)
//        val partitionPQ: ComparisonPQ[(Int, Int)] = ComparisonPQ[(Int, Int)](budget)
//
//        target
//            .indices
//            .foreach { j =>
//                var wSum = 0f
//                val e2 = target(j)
//                e2.index(thetaXY, filterIndices)
//                    .foreach { block =>
//                        sourceIndex.get(block)
//                            .filter(i => source(i).filter(e2, relation, block, thetaXY, Some(partition)))
//                            .foreach { i =>
//                                val e1 = source(i)
//                                val w = getWeight(e1, e2)
//                                wSum += w
//                                targetPQ.enqueue(w, i)
//                            }
//                    }
//                if (! targetPQ.isEmpty) {
//                    val pqSize = targetPQ.size()
//                    val topK = targetPQ.dequeueAll.map(_._2)
//                    val weight = wSum / pqSize
//                    partitionPQ.enqueueAll(topK.map(i => ((i, j), weight)))
//                    targetPQ.clear()
//                }
//            }
//        partitionPQ
//    }
//}
//
//
//object GeometryCentric{
//
//    def apply(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)], ws: WeightingScheme, budget: Int, partitioner: Partitioner)
//    : GeometryCentric ={
//        val thetaXY = Utils.getTheta
//        val sourceCount = Utils.getSourceCount
//        val joinedRDD = source.cogroup(target, partitioner)
//        GeometryCentric(joinedRDD, thetaXY, ws, budget, sourceCount)
//    }
//}