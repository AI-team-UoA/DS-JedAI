package EntityMatching.DistributedMatching

import DataStructures.{ComparisonPQ, Entity, IM, MBB}
import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import utils.Constants.Relation.Relation
import utils.Constants.{Relation, WeightStrategy}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.{ceil, floor, max, min}

trait DMProgressiveTrait extends DMTrait{
    val budget: Int

    def prioritize(source: Array[Entity], target: Array[Entity], partition: MBB, relation: Relation): ComparisonPQ[(Int, Int)]

    /**
     * Weight a comparison
     *
     * @param e1        Spatial entity
     * @param e2        Spatial entity
     * @return weight
     */
    def getWeight(e1: Entity, e2: Entity): Double = {
        val e1Blocks = (ceil(e1.mbb.maxX/thetaXY._1).toInt - floor(e1.mbb.minX/thetaXY._1).toInt + 1) * (ceil(e1.mbb.maxY/thetaXY._2).toInt - floor(e1.mbb.minY/thetaXY._2).toInt + 1).toDouble
        val e2Blocks = (ceil(e2.mbb.maxX/thetaXY._1).toInt - floor(e2.mbb.minX/thetaXY._1).toInt + 1) * (ceil(e2.mbb.maxY/thetaXY._2).toInt - floor(e2.mbb.minY/thetaXY._2).toInt + 1).toDouble
        val cb = (min(ceil(e1.mbb.maxX/thetaXY._1), ceil(e2.mbb.maxX/thetaXY._1)).toInt - max(floor(e1.mbb.minX/thetaXY._1), floor(e2.mbb.minX/thetaXY._1)).toInt + 1) *
            (min(ceil(e1.mbb.maxY/thetaXY._2), ceil(e2.mbb.maxY/thetaXY._2)).toInt - max(floor(e1.mbb.minY/thetaXY._2), floor(e2.mbb.minY/thetaXY._2)).toInt + 1)

        ws match {
            case WeightStrategy.ECBS =>
                cb * math.log10(totalBlocks / e1Blocks) * math.log10(totalBlocks / e2Blocks)

            case WeightStrategy.JS =>
                cb / (e1Blocks + e2Blocks - cb)

            case WeightStrategy.PEARSON_X2 =>
                val v1: Array[Long] = Array[Long](cb, (e2Blocks - cb).toLong)
                val v2: Array[Long] = Array[Long]((e1Blocks - cb).toLong, (totalBlocks - (v1(0) + v1(1) + (e1Blocks - cb))).toLong)
                val chiTest = new ChiSquareTest()
                chiTest.chiSquare(Array(v1, v2))

            case WeightStrategy.CBS | _ =>
                cb.toDouble
        }
    }

    /**
     *  Get the DE-9IM of the top most related entities based
     *  on the input budget and the Weighting strategy
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
                pq.dequeueAll.map{ case (_, (i, j)) =>
                    val e1 = source(i)
                    val e2 = target(j)
                    IM(e1, e2)
                }.takeWhile(_ => !pq.isEmpty)
            else Iterator()
        }
    }


    /**
     *  Examine the Relation of the top most related entities based
     *  on the input budget and the Weighting strategy
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
                pq.dequeueAll.map{ case (_, (i, j)) =>
                    val e1 = source(i)
                    val e2 = target(j)
                    (e1.relate(e2, relation), (e1.originalID, e2.originalID))
                }.filter(_._1).map(_._2)
            else Iterator()
        }
    }


    /**
     * Compute AUC - first weight and perform the comparisons in each partition,
     * then collect them in order and compute the progressive True Positives.
     *
     * @param relation the examined relation
     * @return (AUC, total interlinked Geometries (TP), total comparisons)
     */
    def getAUC(relation: Relation, n: Int = 10, totalQualifiedPairs: Double, takeBudget: Seq[Int]): Seq[(Double, Long, Long, (List[Int], List[Int]))]  ={
       // computes weighted the weighted comparisons
        val matches: RDD[(Double, Boolean)] = joinedRDD
            .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
            .flatMap { p =>
                val pid = p._1
                val partition = partitionsZones(pid)
                val source = p._2._1.toArray
                val target = p._2._2.toArray

                val pq = prioritize(source, target, partition, relation)
                if (!pq.isEmpty)
                    pq.dequeueAll.map{ case (w, (i, j)) =>
                        val e1 = source(i)
                        val e2 = target(j)
                        relation match {
                            case Relation.DE9IM => (w, IM(e1, e2).relate)
                            case _ => (w, e1.relate(e2, relation))
                        }
                    }.takeWhile(_ => !pq.isEmpty)
                else Iterator()
            }.persist(StorageLevel.MEMORY_AND_DISK)

        var results = mutable.ListBuffer[(Double, Long, Long, (List[Int], List[Int]))]()
        for(b <- takeBudget){
            // compute AUC prioritizing the comparisons based on their weight
            val sorted = matches.takeOrdered(b)(Ordering.by[(Double, Boolean), Double](_._1).reverse)
            val counter = sorted.length
            val step = math.ceil(counter/n)

            var progressiveTP: Double = 0
            var TP = 0
            val verifiedPairs = ListBuffer[Int]()
            val qualifiedParis = ListBuffer[Int]()

            sorted
                .map(_._2)
                .zipWithIndex
                .foreach{
                    case (r, i) =>
                        if (r) TP += 1
                        progressiveTP += TP
                        if (i % step == 0){
                            qualifiedParis += TP
                            verifiedPairs += i
                        }
                }
            qualifiedParis += TP
            verifiedPairs += counter
            val qualifiedPairsWithinBudget = if (totalQualifiedPairs < counter) totalQualifiedPairs else counter
            val auc = (progressiveTP/qualifiedPairsWithinBudget)/counter.toDouble
            results += ((auc, TP, counter, (verifiedPairs.toList, qualifiedParis.toList)))
        }
        matches.unpersist()
        results
    }

}
