package linkers.progressive

import linkers.LinkerT
import model.{IM, weightedPairs}
import model.entities.EntityT
import model.structures.{ComparisonPQ, SpatialIndex}
import model.weightedPairs.WeightedPairFactory
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants
import utils.configuration.Constants.Relation
import utils.configuration.Constants.Relation.Relation
import utils.configuration.Constants.WeightingFunction.WeightingFunction

trait ProgressiveLinkerT extends LinkerT{
    val budget: Int
    val mainWF: WeightingFunction
    val secondaryWF: Option[WeightingFunction]
    val ws: Constants.WeightingScheme
    val totalSourceEntities: Long
    val totalBlocks: Double
    val weightedPairFactory: WeightedPairFactory = WeightedPairFactory(mainWF, secondaryWF, ws, tileGranularities, totalBlocks)


    /**
     *  Given a spatial index, retrieve all candidate geometries and filter based on
     *  spatial criteria
     *
     * @param se target Spatial entity
     * @param index spatial index
     * @param partition current partition
     * @return all candidate geometries of se
     */
    def getAllCandidatesWithIndex(se: EntityT, index: SpatialIndex[EntityT], partition: Envelope): Seq[(Int, EntityT)] ={
        index.index(se)
            .flatMap { block =>
                val blockCandidates = index.getWithIndex(block)
                blockCandidates.filter(candidate => filterVerifications(candidate._2, se, block, partition))
            }
    }

    /**
     * Compute the  9-IM of the entities of a PQ
     * @param pq a Priority Queue
     * @param source source entities
     * @param target target entities
     * @return an iterator of  IM
     */
    def computeDE9IM(pq: ComparisonPQ, source: Array[EntityT], target: Array[EntityT]): Iterator[IM] =
        if (!pq.isEmpty)
            pq.dequeueAll.map{ wp =>
                val s = source(wp.entityId1)
                val t = target(wp.entityId2)
                s.getIntersectionMatrix(t)
            }.takeWhile(_ => !pq.isEmpty)
        else Iterator()


    /**
     *  Get the DE-9IM of the top most related entities based
     *  on the input budget and the Weighting Function
     * @return an RDD of IM
     */
    def getDE9IM: Iterator[IM] ={
        val targetAr = target.toArray
        val pq = prioritize(Relation.DE9IM)
        computeDE9IM(pq, source, targetAr)
    }


    /**
     *  Examine the Relation of the top most related entities based
     *  on the input budget and the Weighting Function
     *  @param relation the relation to examine
     *  @return an RDD of pair of IDs
     */
    def relate(relation: Relation): Iterator[(String, String)] = {
        val targetAr = target.toArray
        val pq = prioritize(relation)
        if (!pq.isEmpty)
            pq.dequeueAll.map{ wp =>
                val s = source(wp.entityId1)
                val t = targetAr(wp.entityId2)
                (s.relate(t, relation), (s.originalID, t.originalID))
            }.filter(_._1).map(_._2)
        else Iterator()
    }



//    /**
//     * Measure the time for the Scheduling and Verification steps
//
//     * @return the Scheduling, the Verification and the Total Matching times as a Tuple
//     */
//    def time: (Double, Double, Double) ={
//        val rdd = joinedRDD.filter(j => j._2._1.nonEmpty && j._2._2.nonEmpty)
//
//        // execute and time scheduling step
//        val schedulingStart = Calendar.getInstance().getTimeInMillis
//        val prioritizationResults = rdd.map { p =>
//            val pid = p._1
//            val partition = partitionBorders(pid)
//            val source = p._2._1.toArray
//            val target = p._2._2.toArray
//
//            val pq = prioritize(source, target, partition, Relation.DE9IM)
//            (pq, source, target)
//        }
//        // invoke execution
//        prioritizationResults.count()
//        val schedulingTime = (Calendar.getInstance().getTimeInMillis - schedulingStart) / 1000.0
//
//        // execute and time thw whole matching procedure
//        val matchingTimeStart = Calendar.getInstance().getTimeInMillis
//        // invoke execution
//        countAllRelations
//        val matchingTime = (Calendar.getInstance().getTimeInMillis - matchingTimeStart) / 1000.0
//
//        // the verification time is the matching time - the scheduling time
//        val verificationTime = matchingTime - schedulingTime
//
//        (schedulingTime, verificationTime, schedulingTime+verificationTime)
//    }
//
//
//    /**
//     * Compute PGR - first weight and perform the verifications in parallel,
//     * then serialize the results in a descending order. This way we get the global order of the
//     * discovery of the Qualifying pairs. and hence we can compute PGR.
//     *
//     * @param relation the examined relation
//     * @return (PGR, total interlinked Geometries (TP), total comparisons)
//     */
//    def evaluate(relation: Relation, n: Int = 10, totalQualifiedPairs: Double, takeBudget: Seq[Int]): Seq[(Double, Long, Long, (List[Int], List[Int]))]  ={
//
//        // find the weighted pairs (i.e. Filtering & Scheduling Steps)
//        val matches: RDD[(WeightedPair, Boolean)] = joinedRDD
//            .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
//            .flatMap { p =>
//                val pid = p._1
//                val partition = partitionBorders(pid)
//                val source = p._2._1.toArray
//                val target = p._2._2.toArray
//
//                val pq = prioritize(source, target, partition, relation)
//                if (!pq.isEmpty)
//                    pq.dequeueAll.map{  wp =>
//                        val s = source(wp.entityId1)
//                        val t = target(wp.entityId2)
//                        relation match {
//                            case Relation.DE9IM => (wp, s.getIntersectionMatrix(t).relate)
//                            case _ => (wp, s.relate(t, relation))
//                        }
//                    }.takeWhile(_ => !pq.isEmpty)
//                else Iterator()
//            }
//
//        var results = mutable.ListBuffer[(Double, Long, Long, (List[Int], List[Int]))]()
//
//        // get the global discovery order
//        val sorted = matches.takeOrdered(takeBudget.max)
//        for (b <- takeBudget){
//
//            val sortedPairs = sorted.take(b)
//            // compute AUC prioritizing the comparisons based on their weight
//            val verifications = sorted.length
//            val step = math.ceil(verifications/n)
//
//            var progressiveQP: Double = 0
//            var qp = 0
//            val verificationSteps = ListBuffer[Int]()
//            val qualifiedPairsSteps = ListBuffer[Int]()
//
//            sortedPairs
//                .map(_._2)
//                .zipWithIndex
//                .foreach{
//                    case (r, i) =>
//                        if (r) qp += 1
//                        progressiveQP += qp
//                        if (i % step == 0){
//                            qualifiedPairsSteps += qp
//                            verificationSteps += i
//                        }
//                }
//            qualifiedPairsSteps += qp
//            verificationSteps += verifications
//            val qualifiedPairsWithinBudget = if (totalQualifiedPairs < verifications) totalQualifiedPairs else verifications
//            val pgr = (progressiveQP/qualifiedPairsWithinBudget)/verifications.toDouble
//            results += ((pgr, qp, verifications, (verificationSteps.toList, qualifiedPairsSteps.toList)))
//        }
//        results
//    }

    def prioritize(relation: Relation): ComparisonPQ

}
