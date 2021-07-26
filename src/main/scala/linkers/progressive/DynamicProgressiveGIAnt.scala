package linkers.progressive

import model._
import model.entities.Entity
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants
import utils.configuration.Constants.Relation.Relation
import utils.configuration.Constants.WeightingFunction.WeightingFunction

case class DynamicProgressiveGIAnt(source: Array[Entity], target: Iterable[Entity],
                                   tileGranularities: TileGranularities, partitionBorder: Envelope,
                                   mainWF: WeightingFunction, secondaryWF: Option[WeightingFunction], budget: Int,
                                   totalSourceEntities: Long, ws: Constants.WeightingScheme, totalBlocks: Double)
    extends ProgressiveLinkerT {


    /**
     * First index source and then for each entity of target, find its comparisons using source's index.
     * Weight the comparisons according to the input weighting scheme and sort them using a PQ.
     *
     * @return a PQ with the top comparisons
     */
    def prioritize(relation: Relation): ComparisonPQ ={
        val localBudget = math.ceil(budget*source.length.toDouble/totalSourceEntities.toDouble).toLong
        val sourceIndex = SpatialIndex(source, tileGranularities)
        val pq: DynamicComparisonPQ = DynamicComparisonPQ(localBudget)
        var counter = 0
        val targetAr = target.toArray
        // weight and put the comparisons in a PQ
        targetAr
            .indices
            .foreach {j =>
                val t = targetAr(j)
                val candidates = getAllCandidatesWithIndex(t, sourceIndex, partitionBorder, relation)
                candidates.foreach { case (i, s) =>
                    val wp = weightedPairFactory.createWeightedPair(counter, s, i, t, j)
                    pq.enqueue(wp)
                    counter += 1
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
                val im = s.getIntersectionMatrix(t)
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
    override def relate(relation: Relation): Iterator[(String, String)] = {
        val targetAr = target.toArray
        val pq: DynamicComparisonPQ = prioritize(relation).asInstanceOf[DynamicComparisonPQ]
        val sourceCandidates: Map[Int, List[WeightedPair]] = pq.iterator().map(wp => (wp.entityId1, wp)).toList.groupBy(_._1).mapValues(_.map(_._2))
        val targetCandidates: Map[Int, List[WeightedPair]] = pq.iterator().map(wp => (wp.entityId2, wp)).toList.groupBy(_._1).mapValues(_.map(_._2))
        if (!pq.isEmpty)
            Iterator.continually{
                val wp = pq.dequeueHead()
                val s = source(wp.entityId1)
                val t = targetAr(wp.entityId2)
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