package interlinkers.progressive

import java.util.Calendar

import interlinkers.InterlinkerT
import model._
import model.entities.Entity
import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.apache.spark.rdd.RDD
import utils.Constants
import utils.Constants.Relation.Relation
import utils.Constants.WeightingFunction.WeightingFunction
import utils.Constants.{COMPOSITE, HYBRID, Relation, SINGLE, WeightingFunction}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.{ceil, floor, max, min}



trait ProgressiveInterlinkerT extends InterlinkerT{

    val budget: Int
    val mainWF: WeightingFunction
    val secondaryWF: Option[WeightingFunction]
    val ws: Constants.WeightingScheme
    val totalSourceEntities: Long


    /**
     * the number of all blocks in all partitions
     */
    lazy val totalBlocks: Double = {
        val globalMinX: Double = partitionBorders.map(p => p.minX / thetaXY._1).min
        val globalMaxX: Double = partitionBorders.map(p => p.maxX / thetaXY._1).max
        val globalMinY: Double = partitionBorders.map(p => p.minY / thetaXY._2).min
        val globalMaxY: Double = partitionBorders.map(p => p.maxY / thetaXY._2).max

        (globalMaxX - globalMinX + 1) * (globalMaxY - globalMinY + 1)
    }

    /**
     * compute the main weight of a pair of entities
     * @param s source entity
     * @param t target entity
     * @return a weight
     */
    def getMainWeight(s: Entity, t: Entity): Float = getWeight(s, t, mainWF)


    /**
     * compute the secondary weight of a pair of entities, if the secondary scheme was provided
     * @param s source entity
     * @param t target entity
     * @return a weight
     */
    def getSecondaryWeight(s: Entity, t: Entity): Float =
        secondaryWF match {
            case Some(wf) => getWeight(s, t, wf)
            case None => 0f
        }

    /**
     * Weight a pair
     * @param s        Spatial entity
     * @param t        Spatial entity
     * @return weight
     */
    def getWeight(s: Entity, t: Entity, wf: WeightingFunction): Float = {
        val sBlocks = (ceil(s.mbr.maxX/thetaXY._1).toInt - floor(s.mbr.minX/thetaXY._1).toInt + 1) * (ceil(s.mbr.maxY/thetaXY._2).toInt - floor(s.mbr.minY/thetaXY._2).toInt + 1)
        val tBlocks = (ceil(t.mbr.maxX/thetaXY._1).toInt - floor(t.mbr.minX/thetaXY._1).toInt + 1) * (ceil(t.mbr.maxY/thetaXY._2).toInt - floor(t.mbr.minY/thetaXY._2).toInt + 1)
        lazy val cb = (min(ceil(s.mbr.maxX/thetaXY._1), ceil(t.mbr.maxX/thetaXY._1)).toInt - max(floor(s.mbr.minX/thetaXY._1), floor(t.mbr.minX/thetaXY._1)).toInt + 1) *
            (min(ceil(s.mbr.maxY/thetaXY._2), ceil(t.mbr.maxY/thetaXY._2)).toInt - max(floor(s.mbr.minY/thetaXY._2), floor(t.mbr.minY/thetaXY._2)).toInt + 1)

        wf match {
            case WeightingFunction.MBRO =>
                val intersectionArea = s.mbr.getIntersectingMBR(t.mbr).getArea
                val w = intersectionArea / (s.mbr.getArea + t.mbr.getArea - intersectionArea)
                if (!w.isNaN) w else 0f

            case WeightingFunction.ISP =>
                1f / (s.geometry.getNumPoints + t.geometry.getNumPoints);

            case WeightingFunction.JS =>
                cb / (sBlocks + tBlocks - cb)

            case WeightingFunction.PEARSON_X2 =>
                val v1: Array[Long] = Array[Long](cb, (tBlocks - cb).toLong)
                val v2: Array[Long] = Array[Long]((sBlocks - cb).toLong, (totalBlocks - (v1(0) + v1(1) + (sBlocks - cb))).toLong)
                val chiTest = new ChiSquareTest()
                chiTest.chiSquare(Array(v1, v2)).toFloat

            case WeightingFunction.CF | _ =>
                cb.toFloat
        }
    }


    def getWeightedPair(counter: Int, s: Entity, sIndex: Int, t:Entity, tIndex: Int): WeightedPair = {
        ws match {
            case SINGLE =>
                val mw = getWeight(s, t, mainWF)
                MainWP(counter, sIndex, tIndex, mw)
            case COMPOSITE =>
                val mw = getWeight(s, t, mainWF)
                val sw = getSecondaryWeight(s, t)
                CompositeWP(counter, sIndex, tIndex, mw, sw)
            case HYBRID =>
                val mw = getWeight(s, t, mainWF)
                val sw = getSecondaryWeight(s, t)
                HybridWP(counter, sIndex, tIndex, mw, sw)
        }
    }


    /**
     * Compute the  9-IM of the entities of a PQ
     * @param pq a Priority Queue
     * @param source source entities
     * @param target target entities
     * @return an iterator of  IM
     */
    def computeDE9IM(pq: ComparisonPQ, source: Array[Entity], target: Array[Entity]): Iterator[IM] =
        if (!pq.isEmpty)
            pq.dequeueAll.map{ wp =>
                val s = source(wp.entityId1)
                val t = target(wp.entityId2)
                IM(s, t)
            }.takeWhile(_ => !pq.isEmpty)
        else Iterator()


    /**
     *  Get the DE-9IM of the top most related entities based
     *  on the input budget and the Weighting Function
     * @return an RDD of IM
     */
    def getDE9IM: RDD[IM] ={
        joinedRDD.filter(j => j._2._1.nonEmpty && j._2._2.nonEmpty)
            .flatMap{ p =>
                val pid = p._1
                val partition = partitionBorders(pid)
                val source = p._2._1.toArray
                val target = p._2._2.toArray

                val pq = prioritize(source, target, partition, Relation.DE9IM)
               computeDE9IM(pq, source, target)
            }
    }


    /**
     *  Examine the Relation of the top most related entities based
     *  on the input budget and the Weighting Function
     *  @param relation the relation to examine
     *  @return an RDD of pair of IDs
     */
    def relate(relation: Relation): RDD[(String, String)] = {
        joinedRDD.filter(j => j._2._1.nonEmpty && j._2._2.nonEmpty)
            .flatMap{ p =>
                val pid = p._1
                val partition = partitionBorders(pid)
                val source = p._2._1.toArray
                val target = p._2._2.toArray

                val pq = prioritize(source, target, partition, relation)
                if (!pq.isEmpty)
                    pq.dequeueAll.map{ wp =>
                        val s = source(wp.entityId1)
                        val t = target(wp.entityId2)
                        (s.relate(t, relation), (s.originalID, t.originalID))
                    }.filter(_._1).map(_._2)
                else Iterator()
            }
    }


    /**
     * Measure the time for the Scheduling and Verification steps

     * @return the Scheduling, the Verification and the Total Matching times as a Tuple
     */
    def time: (Double, Double, Double) ={
        val rdd = joinedRDD.filter(j => j._2._1.nonEmpty && j._2._2.nonEmpty)

        // execute and time scheduling step
        val schedulingStart = Calendar.getInstance().getTimeInMillis
        val prioritizationResults = rdd.map { p =>
            val pid = p._1
            val partition = partitionBorders(pid)
            val source = p._2._1.toArray
            val target = p._2._2.toArray

            val pq = prioritize(source, target, partition, Relation.DE9IM)
            (pq, source, target)
        }
        // invoke execution
        prioritizationResults.count()
        val schedulingTime = (Calendar.getInstance().getTimeInMillis - schedulingStart) / 1000.0

        // execute and time thw whole matching procedure
        val matchingTimeStart = Calendar.getInstance().getTimeInMillis
        // invoke execution
        countAllRelations
        val matchingTime = (Calendar.getInstance().getTimeInMillis - matchingTimeStart) / 1000.0

        // the verification time is the matching time - the scheduling time
        val verificationTime = matchingTime - schedulingTime

        (schedulingTime, verificationTime, schedulingTime+verificationTime)
    }


    /**
     * Compute PGR - first weight and perform the verifications in parallel,
     * then serialize the results in a descending order. This way we get the global order of the
     * discovery of the Qualifying pairs. and hence we can compute PGR.
     *
     * @param relation the examined relation
     * @return (PGR, total interlinked Geometries (TP), total comparisons)
     */
    def evaluate(relation: Relation, n: Int = 10, totalQualifiedPairs: Double, takeBudget: Seq[Int]): Seq[(Double, Long, Long, (List[Int], List[Int]))]  ={

        // find the weighted pairs (i.e. Filtering & Scheduling Steps)
        val matches: RDD[(WeightedPair, Boolean)] = joinedRDD
            .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
            .flatMap { p =>
                val pid = p._1
                val partition = partitionBorders(pid)
                val source = p._2._1.toArray
                val target = p._2._2.toArray

                val pq = prioritize(source, target, partition, relation)
                if (!pq.isEmpty)
                    pq.dequeueAll.map{  wp =>
                        val s = source(wp.entityId1)
                        val t = target(wp.entityId2)
                        relation match {
                            case Relation.DE9IM => (wp, IM(s, t).relate)
                            case _ => (wp, s.relate(t, relation))
                        }
                    }.takeWhile(_ => !pq.isEmpty)
                else Iterator()
            }

        var results = mutable.ListBuffer[(Double, Long, Long, (List[Int], List[Int]))]()

        // get the global discovery order
        val sorted = matches.takeOrdered(takeBudget.max)
        for (b <- takeBudget){

            val sortedPairs = sorted.take(b)
            // compute AUC prioritizing the comparisons based on their weight
            val verifications = sorted.length
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

    def prioritize(source: Array[Entity], target: Array[Entity], partition: MBR, relation: Relation): ComparisonPQ


}
