package geospatialInterlinking.progressive

import dataModel.{Entity, IM, MBR, WeightedPair, WeightedPairsPQ}
import geospatialInterlinking.GeospatialInterlinkingT
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import utils.Constants.Relation.Relation
import utils.Constants.WeightingScheme.WeightingScheme
import utils.Constants.Relation
import utils.Utils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait ProgressiveGeospatialInterlinkingT extends GeospatialInterlinkingT{
    val budget: Int
    val mainWS: WeightingScheme
    val secondaryWS: Option[WeightingScheme]

    def prioritize(source: Array[Entity], target: Array[Entity], partition: MBR, relation: Relation): WeightedPairsPQ

    def getMainWeight(e1: Entity, e2: Entity): Float = Utils.getWeight(e1, e2, mainWS)

    def getSecondaryWeight(e1: Entity, e2: Entity): Float =
        secondaryWS match {
            case Some(ws) => Utils.getWeight(e1, e2, ws)
            case None => 0f
        }

    /**
     *  Get the DE-9IM of the top most related entities based
     *  on the input budget and the Weighting Scheme
     * @return an RDD of IM
     */
    def getDE9IM: RDD[IM] ={
        joinedRDD.filter(j => j._2._1.nonEmpty && j._2._2.nonEmpty)
            .flatMap{ p =>
                val pid = p._1
                val partition = partitionsZones(pid)
                val source = p._2._1.toArray
                val target = p._2._2.toArray

                val pq = prioritize(source, target, partition, Relation.DE9IM)
                if (!pq.isEmpty)
                    pq.dequeueAll.map{ wp =>
                        val e1 = source(wp.entityId1)
                        val e2 = target(wp.entityId2)
                        IM(e1, e2)
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
    def relate(relation: Relation): RDD[(String, String)] = {
        joinedRDD.filter(j => j._2._1.nonEmpty && j._2._2.nonEmpty)
            .flatMap{ p =>
            val pid = p._1
            val partition = partitionsZones(pid)
            val source = p._2._1.toArray
            val target = p._2._2.toArray

            val pq = prioritize(source, target, partition, relation)
            if (!pq.isEmpty)
                pq.dequeueAll.map{ wp =>
                    val e1 = source(wp.entityId1)
                    val e2 = target(wp.entityId2)
                    (e1.relate(e2, relation), (e1.originalID, e2.originalID))
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
    def evaluate(relation: Relation, n: Int = 10, totalQualifiedPairs: Double, takeBudget: Seq[Int]): Seq[(Double, Long, Long, (List[Int], List[Int]))]  ={
        // computes weighted the weighted comparisons
        val matches: RDD[(WeightedPair, Boolean)] = joinedRDD
            .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
            .flatMap { p =>
                val pid = p._1
                val partition = partitionsZones(pid)
                val source = p._2._1.toArray
                val target = p._2._2.toArray

                val pq = prioritize(source, target, partition, relation)
                if (!pq.isEmpty)
                    pq.dequeueAll.map{  wp =>
                        val e1 = source(wp.entityId1)
                        val e2 = target(wp.entityId2)
                        relation match {
                            case Relation.DE9IM => (wp, IM(e1, e2).relate)
                            case _ => (wp, e1.relate(e2, relation))
                        }
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
