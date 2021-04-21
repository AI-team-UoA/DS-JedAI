package interlinkers.progressive

import model._
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import utils.Constants.Relation
import utils.Constants.Relation.Relation
import utils.Constants.WeightingFunction.WeightingFunction
import utils.{Constants, Utils}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class DynamicProgressiveGIAnt(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))], thetaXY: (Double, Double),
                                   mainWF: WeightingFunction, secondaryWF: Option[WeightingFunction], budget: Int,
                                   sourceEntities: Int, ws: Constants.WeightingScheme)
    extends ProgressiveInterlinkerT {


    /**
     * First index source and then for each entity of target, find its comparisons using source's index.
     * Weight the comparisons according to the input weighting scheme and sort them using a PQ.
     *
     * @param partition the MBR: of the partition
     * @param source source
     * @param target target
     * @return a PQ with the top comparisons
     */
    def prioritize(source: Array[Entity], target: Array[Entity], partition: MBR, relation: Relation): ComparisonPQ ={
        val localBudget = (math.ceil(budget*source.length.toDouble/sourceEntities.toDouble)*2).toLong
        val sourceIndex = index(source)
        val filterIndices = (b: (Int, Int)) => sourceIndex.contains(b)
        val pq: DynamicComparisonPQ = DynamicComparisonPQ(localBudget)
        var counter = 0
        // weight and put the comparisons in a PQ
        target
            .indices
            .foreach {j =>
                val t = target(j)
                t.index(thetaXY, filterIndices)
                    .foreach { block =>
                        sourceIndex.get(block)
                            .filter(i => source(i).filter(t, relation, block, thetaXY, Some(partition)))
                            .foreach { i =>
                                val s = source(i)
                                val wp = getWeightedPair(counter, s, i, t, j)
                                pq.enqueue(wp)
                                counter += 1
                            }
                    }
            }
        pq
    }


    override def computeDE9IM(pq: ComparisonPQ, source: Array[Entity], target: Array[Entity]): Iterator[IM] = {
        val sourceCandidates: Map[Int, List[WeightedPair]] = pq.iterator().map(wp => (wp.entityId1, wp)).toList.groupBy(_._1).mapValues(_.map(_._2))
        val targetCandidates: Map[Int, List[WeightedPair]] = pq.iterator().map(wp => (wp.entityId2, wp)).toList.groupBy(_._1).mapValues(_.map(_._2))

        if (!pq.isEmpty)
            Iterator.continually {
                val wp = pq.dequeueHead()
                val s = source(wp.entityId1)
                val t = target(wp.entityId2)
                val im = IM(s, t)
                val isRelated = im.relate
                if (isRelated) {
                    sourceCandidates.getOrElse(wp.entityId1, List()).foreach(wp => pq.dynamicUpdate(wp))
                    targetCandidates.getOrElse(wp.entityId2, List()).foreach(wp => pq.dynamicUpdate(wp))
                }
                im
            }.takeWhile(_ => !pq.isEmpty)
        else Iterator()
    }


    /**
     *  Examine the Relation of the top most related entities based
     *  on the input budget and the Weighting Function
     *  @param relation the relation to examine
     *  @return an RDD of pair of IDs
     */
    override def relate(relation: Relation): RDD[(String, String)] = {
        joinedRDD.filter(j => j._2._1.nonEmpty && j._2._2.nonEmpty)
            .flatMap{ p =>
                val pid = p._1
                val partition = partitionsZones(pid)
                val source = p._2._1.toArray
                val target = p._2._2.toArray

                val pq: DynamicComparisonPQ = prioritize(source, target, partition, relation).asInstanceOf[DynamicComparisonPQ]
                val sourceCandidates: Map[Int, List[WeightedPair]] = pq.iterator().map(wp => (wp.entityId1, wp)).toList.groupBy(_._1).mapValues(_.map(_._2))
                val targetCandidates: Map[Int, List[WeightedPair]] = pq.iterator().map(wp => (wp.entityId2, wp)).toList.groupBy(_._1).mapValues(_.map(_._2))
                if (!pq.isEmpty)
                    Iterator.continually{
                        val wp = pq.dequeueHead()
                        val s = source(wp.entityId1)
                        val t = target(wp.entityId2)
                        val isRelated = s.relate(t, relation)
                        if (isRelated){
                            sourceCandidates.getOrElse(wp.entityId1, List()).foreach(wp => pq.dynamicUpdate(wp))
                            targetCandidates.getOrElse(wp.entityId2, List()).foreach(wp => pq.dynamicUpdate(wp))
                        }
                        (isRelated, (s.originalID, t.originalID))
                    }.filter(_._1).map(_._2)
                else Iterator()
            }
    }


    /**
     * Compute PGR - first weight and perform the comparisons in each partition,
     * then collect them in descending order and compute the progressive True Positives.
     *
     * @param relation the examined relation
     * @return (PGR, total interlinked Geometries (TP), total comparisons)
     */
    override def evaluate(relation: Relation, n: Int = 10, totalQualifiedPairs: Double, takeBudget: Seq[Int]): Seq[(Double, Long, Long, (List[Int], List[Int]))]  ={
        // computes weighted the weighted comparisons
        val matches: RDD[(WeightedPair, Boolean)] = joinedRDD
            .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
            .flatMap { p =>
                val pid = p._1
                val partition = partitionsZones(pid)
                val source = p._2._1.toArray
                val target = p._2._2.toArray

                val pq: DynamicComparisonPQ = prioritize(source, target, partition, relation).asInstanceOf[DynamicComparisonPQ]
                val sourceCandidates: Map[Int, List[WeightedPair]] = pq.iterator().map(wp => (wp.entityId1, wp)).toList.groupBy(_._1).mapValues(_.map(_._2))
                val targetCandidates: Map[Int, List[WeightedPair]] = pq.iterator().map(wp => (wp.entityId2, wp)).toList.groupBy(_._1).mapValues(_.map(_._2))
                if (!pq.isEmpty)
                    Iterator.continually{
                        val wp = pq.dequeueHead()
                        val s = source(wp.entityId1)
                        val t = target(wp.entityId2)
                        val isRelated = relation match {
                            case Relation.DE9IM => IM(s, t).relate
                            case _ => s.relate(t, relation)
                        }
                        if (isRelated){
                            sourceCandidates.getOrElse(wp.entityId1, List()).foreach(wp => pq.dynamicUpdate(wp))
                            targetCandidates.getOrElse(wp.entityId2, List()).foreach(wp => pq.dynamicUpdate(wp))
                        }
                        (wp, isRelated)
                    }.takeWhile(_ => !pq.isEmpty)
                else Iterator()
            }

        var results = mutable.ListBuffer[(Double, Long, Long, (List[Int], List[Int]))]()
        val sorted = matches.takeOrdered(takeBudget.max)
        for(b <- takeBudget){
            // compute AUC prioritizing the comparisons based on their weight
            val sortedPairs = sorted.take(b)
            val verifications = sortedPairs.length
            val step = math.ceil(verifications/n)

            var progressiveQP: Double = 0
            var qp = 0
            val verificationSteps = ListBuffer[Int]()
            val qualifiedPairsSteps = ListBuffer[Int]()

            sortedPairs
                .map(_._2)
                .zipWithIndex
                .foreach{
                    case (r, i) =>
                        if (r) qp += 1
                        progressiveQP += qp
                        if (i % step == 0){
                            qualifiedPairsSteps += qp
                            verificationSteps += i
                        }
                }
            qualifiedPairsSteps += qp
            verificationSteps += verifications
            val qualifiedPairsWithinBudget = if (totalQualifiedPairs < verifications) totalQualifiedPairs else verifications
            val pgr = (progressiveQP/qualifiedPairsWithinBudget)/verifications.toDouble
            results += ((pgr, qp, verifications, (verificationSteps.toList, qualifiedPairsSteps.toList)))
        }
        results
    }
}


/**
 * auxiliary constructor
 */
object DynamicProgressiveGIAnt {

    def apply(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)], wf: WeightingFunction, swf: Option[WeightingFunction] = None,
              budget: Int, partitioner: Partitioner, ws: Constants.WeightingScheme): DynamicProgressiveGIAnt ={
        val thetaXY = Utils.getTheta
        val joinedRDD = source.cogroup(target, partitioner)
        val sourceEntities = Utils.sourceCount
        DynamicProgressiveGIAnt(joinedRDD, thetaXY, wf, swf, budget, sourceEntities.toInt, ws)
    }

}