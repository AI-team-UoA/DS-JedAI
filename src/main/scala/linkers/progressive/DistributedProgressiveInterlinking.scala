package linkers.progressive

import cats.implicits._
import model.entities.EntityT
import model.{IM, TileGranularities}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants
import utils.configuration.Constants.ProgressiveAlgorithm.ProgressiveAlgorithm
import utils.configuration.Constants.Relation.Relation
import utils.configuration.Constants.WeightingFunction.WeightingFunction
import utils.configuration.Constants.{ProgressiveAlgorithm, Relation}
import utils.readers.GridPartitioner

import java.util.Calendar
import scala.collection.mutable.ListBuffer

/**
 * Apply distributed Progressive Interlinking
 * by initializing an RDD with Progressive Linkers in each partition.
 */
object DistributedProgressiveInterlinking {

    /**
     * Initialize a Progressive Linker in each partition
     *
     * @param source source dataset
     * @param target target dataset
     * @param partitionBorders borders of partitions
     * @param theta tile granularities
     * @param progressiveAlgorithm the selected progressive algorithm
     * @param gridPartitioner partitioner
     * @param sourceCount #source entities
     * @param budget    input budget
     * @param mainWF main weighting function
     * @param secondaryWF secondary weighting function
     * @param ws  weighting scheme
     * @return an RDD with progressive linkers in each partition
     */
    def initializeProgressiveLinkers(source: RDD[(Int, EntityT)], target: RDD[(Int, EntityT)],
                                     partitionBorders: Array[Envelope],
                                     theta: TileGranularities,
                                     gridPartitioner: GridPartitioner,
                                     progressiveAlgorithm: ProgressiveAlgorithm,
                                     budget: Int = 0,
                                     sourceCount: Long,
                                     ws: Constants.WeightingScheme,
                                     mainWF: WeightingFunction,
                                     secondaryWF: Option[WeightingFunction],
                                     batchSize: Int=100,
                                     maxViolations: Int=4,
                                     precisionLevel: Float=0.18f
                                      ): RDD[ProgressiveLinkerT] = {

        val totalBlocks = gridPartitioner.computeTotalBlocks(theta)
        val joinedRDD: RDD[(Int, (Iterable[EntityT], Iterable[EntityT]))] = source.cogroup(target, gridPartitioner.hashPartitioner)
        joinedRDD.map { case (pid: Int,  (sourceP: Iterable[EntityT], targetP:Iterable[EntityT])) =>
            val partition = partitionBorders(pid)

            progressiveAlgorithm match {
                case ProgressiveAlgorithm.RANDOM =>
                    RandomScheduling(sourceP.toArray, targetP, theta, partition, mainWF, secondaryWF, budget, sourceCount,
                        ws, totalBlocks)
                case ProgressiveAlgorithm.TOPK =>
                    TopKPairs(sourceP.toArray, targetP, theta, partition, mainWF, secondaryWF, budget, sourceCount, ws,
                        totalBlocks)
                case ProgressiveAlgorithm.RECIPROCAL_TOPK =>
                    ReciprocalTopK(sourceP.toArray, targetP, theta, partition, mainWF, secondaryWF, budget, sourceCount, ws,
                        totalBlocks)
                case ProgressiveAlgorithm.DYNAMIC_PROGRESSIVE_GIANT =>
                    DynamicProgressiveGIAnt(sourceP.toArray, targetP, theta, partition, mainWF, secondaryWF, budget,
                        sourceCount, ws, totalBlocks)
                case ProgressiveAlgorithm.EARLY_STOPPING =>
                    EarlyStoppingLinker(sourceP.toArray, targetP, theta, partition, budget, sourceCount, totalBlocks, batchSize,
                        maxViolations, precisionLevel)
                case ProgressiveAlgorithm.PROGRESSIVE_GIANT | _ =>
                    ProgressiveGIAnt(sourceP.toArray, targetP, theta, partition, mainWF, secondaryWF, budget, sourceCount,
                        ws, totalBlocks)
            }
        }
    }

    /**
     * Compute IM given an RDD of linkers
     * @param linkersRDD an RDD of Linkers
     * @return an RDD of IM
     */
    def computeIM(linkersRDD: RDD[ProgressiveLinkerT]): RDD[IM] =
        linkersRDD.flatMap(linker => linker.getDE9IM)

    /**
     * Compute the number of verifications provided an RDD of linkers
     * @param linkersRDD an RDD of linkers
     * @return the number of Verifications
     */
    def countVerifications(linkersRDD: RDD[ProgressiveLinkerT]): Double =
        linkersRDD.map(linker => linker.countVerification).sum

    /**
     * Find the pairs that satisfy the given relation
     * @param linkersRDD an RDD of linkers
     * @param relation a topological relation
     * @return an RDD of pairs that satisfy the given relation
     */
    def relate(linkersRDD: RDD[ProgressiveLinkerT], relation: Relation): RDD[(String, String)] =
        linkersRDD.flatMap(linker => linker.relate(relation))

    /**
     * Compute all the relations given an RDD of linkers
     * @param linkersRDD an RDD of Linkers
     * @return the number of  all nine topological relations
     */
    def countAllRelations(linkersRDD: RDD[ProgressiveLinkerT]): (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) =
        accumulateIM(computeIM(linkersRDD))

    /**
     * Accumulate the IM of an RDD of IM
     * @param imRDD an RDD of IM
     * @return the number of  all nine topological relations
     */
    def accumulateIM(imRDD: RDD[IM]): (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) =
        imRDD
            .mapPartitions { imIterator => Iterator(accumulate(imIterator)) }
            .treeReduce({ case (im1, im2) => im1 |+| im2 }, 4)

    def take(imRDD: RDD[IM], budget: Int): (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) =
        imRDD.mapPartitions { imIterator => Iterator(accumulate(imIterator)) }.take(budget).reduceLeft(_ |+| _)


    /**
     * Measure the time for the Scheduling and Verification steps
     * @return the Scheduling, the Verification and the Total Matching times as a Tuple
     */
    def time(progressiveLinkersRDD: RDD[ProgressiveLinkerT]): (Double, Double, Double) ={

        // execute and time scheduling step
        val schedulingStart = Calendar.getInstance().getTimeInMillis
        val prioritizationResults = progressiveLinkersRDD.map{ linker => linker.prioritize(Relation.DE9IM)}
        // invoke execution
        prioritizationResults.count()
        val schedulingTime = (Calendar.getInstance().getTimeInMillis - schedulingStart) / 1000.0

        // execute and time thw whole matching procedure
        val matchingTimeStart = Calendar.getInstance().getTimeInMillis
        // invoke execution
        countAllRelations(progressiveLinkersRDD)
        val matchingTime = (Calendar.getInstance().getTimeInMillis - matchingTimeStart) / 1000.0
        // the verification time is the matching time - the scheduling time
        val verificationTime = matchingTime - schedulingTime

        (schedulingTime, verificationTime, schedulingTime+verificationTime)
    }

// TODO comment
    def evaluate(progressiveAlgorithm: ProgressiveAlgorithm, progressiveLinkersRDD: RDD[ProgressiveLinkerT], relation: Relation, n: Int = 10,
                 totalQualifiedPairs: Long, takeBudget: Seq[Int]
                ): Seq[(Double, Int, Int, (List[Int], List[Int]))]={
        progressiveAlgorithm match {
            case  ProgressiveAlgorithm.EARLY_STOPPING =>
                nonProgressiveEvaluate(progressiveLinkersRDD, relation, n, totalQualifiedPairs, takeBudget)
            case _ =>
                progressiveEvaluate(progressiveLinkersRDD, relation, n, totalQualifiedPairs, takeBudget)
        }

    }

    /**
     * Compute PGR - first weight and perform the comparisons in each partition,
     * then collect them in descending order and compute the progressive True Positives.
     *
     * @param relation the examined relation
     * @return (PGR, total interlinked Geometries (TP), total verifications, List[Verifications], List[TP])
     */
    def progressiveEvaluate(progressiveLinkersRDD: RDD[ProgressiveLinkerT], relation: Relation, n: Int = 10,
                 totalQualifiedPairs: Long, takeBudget: Seq[Int]
                         ): Seq[(Double, Int, Int, (List[Int], List[Int]))]={
        // computes weighted the weighted comparisons
        val matches: RDD[Boolean] = progressiveLinkersRDD.flatMap { linker => linker.computeDE9IM(linker.prioritize(relation)).map(_.relate) }
        val sortedPairs = matches.takeOrdered(takeBudget.max)
        evaluatePairs(sortedPairs, takeBudget, n, totalQualifiedPairs)
    }

    def nonProgressiveEvaluate(progressiveLinkersRDD: RDD[ProgressiveLinkerT], relation: Relation, n: Int = 10,
                               totalQualifiedPairs: Long, takeBudget: Seq[Int]
                           ): Seq[(Double, Int, Int, (List[Int], List[Int]))]={
        // computes weighted the weighted comparisons
        val matches: RDD[Boolean] = progressiveLinkersRDD
            .flatMap { linker => linker.computeDE9IM(linker.prioritize(relation)).map(_.relate)}
        val collectedPairs = matches.take(takeBudget.max)
        evaluatePairs(collectedPairs, takeBudget, n, totalQualifiedPairs)
    }

    def evaluatePairs(pairs: Seq[Boolean], budgets: Seq[Int], n: Int, totalQualifiedPairs: Long
                     ):  Seq[(Double, Int, Int, (List[Int], List[Int]))]= {
        budgets.map { b =>
            // compute AUC prioritizing the comparisons based on their weight
            val collectedPairs = pairs.take(b)
            var qp: Int = 0
            var progressiveQP: Double = 0
            val verifications: Int = collectedPairs.length
            val verificationSteps = ListBuffer[Int]()
            val qualifiedPairsSteps = ListBuffer[Int]()
            val step = math.ceil(verifications / n)

            collectedPairs
                .zipWithIndex
                .foreach { case (r, i) =>
                    if (r) qp += 1
                    progressiveQP += qp
                    if (i % step == 0) {
                        qualifiedPairsSteps += qp
                        verificationSteps += i
                    }
                }
            qualifiedPairsSteps += qp
            verificationSteps += verifications
            val qualifiedPairsWithinBudget = if (totalQualifiedPairs < verifications) totalQualifiedPairs else verifications
            val pgr = (progressiveQP / qualifiedPairsWithinBudget) / verifications.toDouble
            (pgr, qp, verifications, (verificationSteps.toList, qualifiedPairsSteps.toList))
        }
    }

    val accumulate: Iterator[IM] => (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) = imIterator => {
        var totalContains: Int = 0
        var totalCoveredBy: Int = 0
        var totalCovers: Int = 0
        var totalCrosses: Int = 0
        var totalEquals: Int = 0
        var totalIntersects: Int = 0
        var totalOverlaps: Int = 0
        var totalTouches: Int = 0
        var totalWithin: Int = 0
        var verifications: Int = 0
        var qualifiedPairs: Int = 0
        imIterator.foreach { im =>
            verifications += 1
            if (im.relate) {
                qualifiedPairs += 1
                if (im.isContains) totalContains += 1
                if (im.isCoveredBy) totalCoveredBy += 1
                if (im.isCovers) totalCovers += 1
                if (im.isCrosses) totalCrosses += 1
                if (im.isEquals) totalEquals += 1
                if (im.isIntersects) totalIntersects += 1
                if (im.isOverlaps) totalOverlaps += 1
                if (im.isTouches) totalTouches += 1
                if (im.isWithin) totalWithin += 1
            }
        }
        (totalContains, totalCoveredBy, totalCovers, totalCrosses, totalEquals, totalIntersects,
            totalOverlaps, totalTouches, totalWithin, verifications, qualifiedPairs)
    }

}
