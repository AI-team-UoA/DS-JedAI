package interlinkers.progressive

import model.{ComparisonPQ, DynamicComparisonPQ, Entity, IM, MBR, WeightedPair}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import utils.Constants.Relation
import utils.Constants.Relation.Relation
import utils.Constants.WeightingScheme.WeightingScheme
import utils.Utils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class DynamicProgressiveGIAnt(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))], thetaXY: (Double, Double),
                                    mainWS: WeightingScheme, secondaryWS: Option[WeightingScheme], budget: Int, sourceEntities: Int)
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
                val e2 = target(j)
                e2.index(thetaXY, filterIndices)
                    .foreach { block =>
                        sourceIndex.get(block)
                            .filter(i => source(i).filter(e2, relation, block, thetaXY, Some(partition)))
                            .foreach { i =>
                                val e1 = source(i)
                                val w = getMainWeight(e1, e2)
                                val secW = getSecondaryWeight(e1, e2)
                                val wp = WeightedPair(counter, i, j, w, secW)
                                pq.enqueue(wp)
                                counter += 1
                            }
                    }
            }
        pq
    }

    /**
     *  Get the DE-9IM of the top most related entities based
     *  on the input budget and the Weighting Scheme
     * @return an RDD of IM
     */
    override def getDE9IM: RDD[IM] ={
        joinedRDD.filter(j => j._2._1.nonEmpty && j._2._2.nonEmpty)
            .flatMap{ p =>
                val pid = p._1
                val partition = partitionsZones(pid)
                val source = p._2._1.toArray
                val target = p._2._2.toArray

                val pq: DynamicComparisonPQ = prioritize(source, target, partition, Relation.DE9IM).asInstanceOf[DynamicComparisonPQ]
                val sourceCandidates: Map[Int, List[WeightedPair]] = pq.iterator().map(wp => (wp.entityId1, wp)).toList.groupBy(_._1).mapValues(_.map(_._2))
                val targetCandidates: Map[Int, List[WeightedPair]] = pq.iterator().map(wp => (wp.entityId2, wp)).toList.groupBy(_._1).mapValues(_.map(_._2))

                if (!pq.isEmpty)
                    Iterator.continually{
                        val wp = pq.dequeueHead()
                        val e1 = source(wp.entityId1)
                        val e2 = target(wp.entityId2)
                        val im = IM(e1, e2)
                        val isRelated = im.relate
                        if (isRelated){
                            sourceCandidates.getOrElse(wp.entityId1, List()).foreach(wp => pq.dynamicUpdate(wp))
                            targetCandidates.getOrElse(wp.entityId2, List()).foreach(wp => pq.dynamicUpdate(wp))
                        }
                        im
                    }.takeWhile(_ => !pq.isEmpty)
                else Iterator()
            }
    }


    /**
     *  Examine the Relation of the top most related entities based
     *  on the input budget and the Weighting Scheme
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
                        val e1 = source(wp.entityId1)
                        val e2 = target(wp.entityId2)
                        val isRelated = e1.relate(e2, relation)
                        if (isRelated){
                            sourceCandidates.getOrElse(wp.entityId1, List()).foreach(wp => pq.dynamicUpdate(wp))
                            targetCandidates.getOrElse(wp.entityId2, List()).foreach(wp => pq.dynamicUpdate(wp))
                        }
                        (isRelated, (e1.originalID, e2.originalID))
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
                        val e1 = source(wp.entityId1)
                        val e2 = target(wp.entityId2)
                        val isRelated = relation match {
                            case Relation.DE9IM => IM(e1, e2).relate
                            case _ => e1.relate(e2, relation)
                        }
                        if (isRelated){
                            sourceCandidates.getOrElse(wp.entityId1, List()).foreach(wp => pq.dynamicUpdate(wp))
                            targetCandidates.getOrElse(wp.entityId2, List()).foreach(wp => pq.dynamicUpdate(wp))
                        }
                        (wp, isRelated)
                    }.takeWhile(_ => !pq.isEmpty)
                else Iterator()
            }.persist(StorageLevel.MEMORY_AND_DISK)

        var results = mutable.ListBuffer[(Double, Long, Long, (List[Int], List[Int]))]()
        for(b <- takeBudget){
            // compute AUC prioritizing the comparisons based on their weight
            val sorted = matches.takeOrdered(b)
            val verifications = sorted.length
            val step = math.ceil(verifications/n)

            var progressiveQP: Double = 0
            var qp = 0
            val verificationSteps = ListBuffer[Int]()
            val qualifiedPairsSteps = ListBuffer[Int]()

            sorted
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
        matches.unpersist()
        results
    }
}


/**
 * auxiliary constructor
 */
object DynamicProgressiveGIAnt {

    def apply(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)], ws: WeightingScheme, sws: Option[WeightingScheme] = None,
              budget: Int, partitioner: Partitioner): DynamicProgressiveGIAnt ={
        val thetaXY = Utils.getTheta
        val joinedRDD = source.cogroup(target, partitioner)
        val sourceEntities = Utils.sourceCount
        DynamicProgressiveGIAnt(joinedRDD, thetaXY, ws, sws, budget, sourceEntities.toInt)
    }

}