package linkers.progressive

import model.{IM, TileGranularities}
import model.entities.EntityT
import model.structures.{ComparisonPQ, StaticComparisonPQ}
import model.weightedPairs.{WeightedPairFactory, WeightedPairT}
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants
import utils.configuration.Constants.Relation.Relation
import utils.configuration.Constants.WeightingFunction.{CF, WeightingFunction}

import scala.annotation.tailrec

case class EarlyStopping (source: Array[EntityT], target: Iterable[EntityT], tileGranularities: TileGranularities,
                          partitionBorder: Envelope, budget: Int, totalSourceEntities: Long, totalBlocks: Double,
                          batchSize: Int=100, maxViolations: Int=4, precisionLevel: Float=0.18f)
    extends ProgressiveLinkerT {

    // ignored
    val mainWF: WeightingFunction = CF
    val secondaryWF: Option[WeightingFunction] = None

    override val ws: Constants.WeightingScheme = Constants.THIN_MULTI_COMPOSITE
    override val weightedPairFactory: WeightedPairFactory = WeightedPairFactory(mainWF, secondaryWF, ws, tileGranularities, totalBlocks)


    def prioritize(relation: Relation): ComparisonPQ = {
        val targetAr = target.toArray
        val localBudget = math.ceil(budget * source.length.toDouble / totalSourceEntities.toDouble).toLong
        val pq: StaticComparisonPQ = StaticComparisonPQ(localBudget)
        var counter = 0
        // weight and put the comparisons in a PQ
        targetAr
            .indices
            .foreach { j =>
                val t = targetAr(j)
                val candidates = getAllCandidatesWithIndex(t, sourceIndex, partitionBorder)
                candidates.foreach { case (i, s) =>
                    val wp = weightedPairFactory.createWeightedPair(counter, s, i, t, j)
                    pq.enqueue(wp)
                    counter += 1
                }
            }
        pq
    }

    override def computeDE9IM(pq: ComparisonPQ, source: Array[EntityT], target: Array[EntityT]): Iterator[IM] ={

        @tailrec
        def earlyStopping(pairs: Iterator[WeightedPairT], batchMatches: Int, violations: Int, qp: Int, verifications: Int,
                          minimumMatches: Int, acc: List[IM]):List[IM] ={
            if (pairs.hasNext) {
                val wp = pairs.next()
                val tail = pairs
                val s = source(wp.entityId1)
                val t = target(wp.entityId2)
                val im = s.getIntersectionMatrix(t)
                val acc_ = im :: acc

                val verifications_ = verifications + 1
                val qp_ = if (im.relate) qp+1 else qp
                val batchMatches_ = if (im.relate) batchMatches+1 else batchMatches
                if (verifications_ % batchSize == 0) {
                    val minimumMatches_ = if (minimumMatches < 0) math.ceil(precisionLevel * batchMatches).toInt else minimumMatches
                    val violations_ = if (batchMatches_ < minimumMatches_) violations+1 else 0
                    if (violations_ == maxViolations)
                        acc_
                    else
                        earlyStopping(tail, 0, violations_, qp_, verifications_, minimumMatches_, acc_)
                }
                else {
                    earlyStopping(tail, batchMatches_, violations, qp_, verifications_, minimumMatches, acc_)
                }
            }
            else
                acc
        }

        earlyStopping(pq.dequeueAll, 0, 0, 0, 0, -1, Nil).toIterator
    }


    override def relate(relation: Relation): Iterator[(String, String)] = {
        val targetAr = target.toArray

        @tailrec
        def earlyStopping(pairs: Iterator[WeightedPairT], batchMatches: Int, violations: Int, qp: Int, verifications: Int,
                          minimumMatches: Int, acc: List[(String, String)]):List[(String, String)] ={
            if (pairs.hasNext) {
                val wp = pairs.next()
                val tail = pairs
                val s = source(wp.entityId1)
                val t = targetAr(wp.entityId2)
                val relate = s.relate(t, relation)
                val acc_ = if (relate) (s.originalID, t.originalID) :: acc else acc

                val verifications_ = verifications + 1
                val qp_ = if (relate) qp+1 else qp
                val batchMatches_ = if (relate) batchMatches+1 else batchMatches
                if (verifications_ % batchSize == 0) {
                    val minimumMatches_ = if (minimumMatches < 0) math.ceil(precisionLevel * batchMatches).toInt else minimumMatches
                    val violations_ = if (batchMatches_ < minimumMatches_) violations+1 else 0
                    if (violations_ == maxViolations)
                        acc_
                    else
                        earlyStopping(tail, 0, violations_, qp_, verifications_, minimumMatches_, acc_)
                }
                else {
                    earlyStopping(tail, batchMatches_, violations, qp_, verifications_, minimumMatches, acc_)
                }
            }
            else
                acc
        }



        val pq = prioritize(relation)
        earlyStopping(pq.dequeueAll, 0, 0, 0, 0, -1, Nil).toIterator
    }
}
