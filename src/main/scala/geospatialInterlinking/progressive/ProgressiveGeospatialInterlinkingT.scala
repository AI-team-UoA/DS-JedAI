package geospatialInterlinking.progressive

import dataModel.{Entity, IM, MBR, WeightedPair, WeightedPairsPQ}
import geospatialInterlinking.GeospatialInterlinkingT
import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import utils.Constants.Relation.Relation
import utils.Constants.WeightingScheme.WeightingScheme
import utils.Constants.{Relation, WeightingScheme}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.{ceil, floor, max, min}

trait ProgressiveGeospatialInterlinkingT extends GeospatialInterlinkingT{
    val budget: Int
    val mainWS: WeightingScheme
    val secondaryWS: Option[WeightingScheme]

    lazy val totalBlocks: Double = {
        val globalMinX: Double = partitionsZones.map(p => p.minX / thetaXY._1).min
        val globalMaxX: Double = partitionsZones.map(p => p.maxX / thetaXY._1).max
        val globalMinY: Double = partitionsZones.map(p => p.minY / thetaXY._2).min
        val globalMaxY: Double = partitionsZones.map(p => p.maxY / thetaXY._2).max

        (globalMaxX - globalMinX + 1) * (globalMaxY - globalMinY + 1)
    }

    def prioritize(source: Array[Entity], target: Array[Entity], partition: MBR, relation: Relation): WeightedPairsPQ

    def getMainWeight(e1: Entity, e2: Entity): Float = getWeight(e1, e2, mainWS)

    def getSecondaryWeight(e1: Entity, e2: Entity): Float =
        secondaryWS match {
            case Some(ws) => getWeight(e1, e2, ws)
            case None => 0f
        }

    /**
     * Weight a comparison
     * TODO: ensure that float does not produce issues
     *
     * @param e1        Spatial entity
     * @param e2        Spatial entity
     * @return weight
     */
    def getWeight(e1: Entity, e2: Entity, ws: WeightingScheme): Float = {
        val e1Blocks = (ceil(e1.mbr.maxX/thetaXY._1).toInt - floor(e1.mbr.minX/thetaXY._1).toInt + 1) * (ceil(e1.mbr.maxY/thetaXY._2).toInt - floor(e1.mbr.minY/thetaXY._2).toInt + 1)
        val e2Blocks = (ceil(e2.mbr.maxX/thetaXY._1).toInt - floor(e2.mbr.minX/thetaXY._1).toInt + 1) * (ceil(e2.mbr.maxY/thetaXY._2).toInt - floor(e2.mbr.minY/thetaXY._2).toInt + 1)
        val cb = (min(ceil(e1.mbr.maxX/thetaXY._1), ceil(e2.mbr.maxX/thetaXY._1)).toInt - max(floor(e1.mbr.minX/thetaXY._1), floor(e2.mbr.minX/thetaXY._1)).toInt + 1) *
            (min(ceil(e1.mbr.maxY/thetaXY._2), ceil(e2.mbr.maxY/thetaXY._2)).toInt - max(floor(e1.mbr.minY/thetaXY._2), floor(e2.mbr.minY/thetaXY._2)).toInt + 1)

        ws match {
            case WeightingScheme.MBR_INTERSECTION =>
                val intersectionArea = e1.mbr.getIntersectingMBR(e2.mbr).getArea
                intersectionArea / (e1.mbr.getArea + e2.mbr.getArea - intersectionArea)

            case WeightingScheme.POINTS =>
                1f / (e1.geometry.getNumPoints + e2.geometry.getNumPoints);

            case WeightingScheme.JS =>
                cb / (e1Blocks + e2Blocks - cb)

            case WeightingScheme.PEARSON_X2 =>
                val v1: Array[Long] = Array[Long](cb, (e2Blocks - cb).toLong)
                val v2: Array[Long] = Array[Long]((e1Blocks - cb).toLong, (totalBlocks - (v1(0) + v1(1) + (e1Blocks - cb))).toLong)
                val chiTest = new ChiSquareTest()
                chiTest.chiSquare(Array(v1, v2)).toFloat

            case WeightingScheme.CF | _ =>
                cb.toFloat
        }
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
