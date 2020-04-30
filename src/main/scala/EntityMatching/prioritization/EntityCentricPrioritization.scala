package EntityMatching.prioritization

import Blocking.BlockUtils.clean
import DataStructures.{Block, TBlock}
import EntityMatching.Matching.{relate, testMBB}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer


case class EntityCentricPrioritization(totalBlocks: Long, weightingStrategy: String) extends  PrioritizationTrait  {


    /**
     * Based on the weighted comparisons, weight the entities of source. The comparisons
     * of the entities with the higher weights, will be prioritized. Furthermore, for
     * each entity, the comparisons with the higher weight will also be prioritized.
     *
     * @param blocks the input Blocks
     * @param relation the examined relation
     * @param cleaningStrategy  the cleaning strategy
     * @return an RDD containing the IDs of the matches
     */
    def apply(blocks: RDD[Block], relation: String, cleaningStrategy: String): RDD[(Int,Int)] ={

        val weightedComparisonsPerBlock = getWeights(blocks.asInstanceOf[RDD[TBlock]])
            .asInstanceOf[RDD[(Any, ArrayBuffer[Long])]]

        val cleanWeightedComparisonsPerBlock = clean(weightedComparisonsPerBlock, cleaningStrategy)
            .asInstanceOf[RDD[(Long, ArrayBuffer[(Long, Double)])]]
            .filter(_._2.nonEmpty)

        val blocksComparisons = blocks.map(b => (b.id, b))
        cleanWeightedComparisonsPerBlock
            .leftOuterJoin(blocksComparisons)
            .flatMap {
                b =>
                    val comparisonsWeightsMap = b._2._1.toMap
                    val comparisons = b._2._2.get.getComparisons
                    comparisons
                        .filter(c => comparisonsWeightsMap.contains(c.id))
                        .map { c =>
                            val weight = comparisonsWeightsMap(c.id)
                            val env1 = c.entity1.geometry.getEnvelope
                            val env2 = c.entity2.geometry.getEnvelope
                            val normalizedWeight = normalizeWeight(weight, env1, env2)
                            (normalizedWeight, c)
                        }
            }
            .map { case (w, c) => (c.entity1, ArrayBuffer((w, c.entity2)))}
            .reduceByKey(_ ++_ )
            .mapPartitions { iter =>
                iter
                    .toArray
                    .map{ case (e1, weightedTarget) =>
                        val weights = weightedTarget.map(_._1)
                        val weight = weights.sum / weights.size
                        (weight, (e1, weightedTarget))
                    }
                    .sortBy(_._1)(Ordering.Double.reverse)
                    .flatMap{ case (w, (e1, weightedTarget)) =>
                        weightedTarget
                            .sortBy(_._1)(Ordering.Double.reverse)
                            .map(_._2)
                            .filter(e2 => testMBB(e1.mbb, e2.mbb, relation))
                            .filter(e2 => relate(e1.geometry, e2.geometry, relation))
                            .map(e2 => (e1.id, e2.id))
                    }
                    .toIterator
            }

    }


    /**
     * Similar to apply(), but the top comparisons of each entity are executed
     * first in an iterative way
     *
     * @param blocks the input Blocks
     * @param relation the examined relation
     * @param cleaningStrategy  the cleaning strategy
     * @return an RDD containing the IDs of the matches
     **/
    override def iterativeExecution(blocks: RDD[Block], relation: String, cleaningStrategy: String): RDD[(Int,Int)] ={

        val weightedComparisonsPerBlock = getWeights(blocks.asInstanceOf[RDD[TBlock]])
            .asInstanceOf[RDD[(Any, ArrayBuffer[Long])]]

        val cleanWeightedComparisonsPerBlock = clean(weightedComparisonsPerBlock, cleaningStrategy)
            .asInstanceOf[RDD[(Long, ArrayBuffer[(Long, Double)])]]
            .filter(_._2.nonEmpty)

        val blocksComparisons = blocks.map(b => (b.id, b))
        cleanWeightedComparisonsPerBlock
            .leftOuterJoin(blocksComparisons)
            .flatMap {
                b =>
                    val comparisonsWeightsMap = b._2._1.toMap
                    val comparisons = b._2._2.get.getComparisons
                    comparisons
                        .filter(c => comparisonsWeightsMap.contains(c.id))
                        .map { c =>
                            val weight = comparisonsWeightsMap(c.id)
                            val env1 = c.entity1.geometry.getEnvelope
                            val env2 = c.entity2.geometry.getEnvelope
                            val normalizedWeight = normalizeWeight(weight, env1, env2)
                            (normalizedWeight, c)
                        }
            }
            .map { case (w, c) => (c.entity1, ArrayBuffer((w, c.entity2)))}
            .reduceByKey(_ ++_ )
            .mapPartitions { iter =>
                val topComparisons = iter
                    .toArray
                    .map { case (e1, weightedTarget) =>
                        val weights = weightedTarget.map(_._1)
                        val weight = weights.sum / weights.size
                        (weight, (e1, weightedTarget))
                    }
                    .sortBy(_._1)(Ordering.Double.reverse)
                    .map(p => (p._2._1, p._2._2.sortBy(_._1)(Ordering.Double.reverse).map(_._2).toIterator))

                val matches = ArrayBuffer[(Int, Int)]()
                var converged = false
                while (!converged) {
                    converged = true
                    for (c <- topComparisons) {
                        if (c._2.hasNext) {
                            converged = false
                            val (e1, e2) = (c._1, c._2.next())
                            if (testMBB(e1.mbb, e2.mbb, relation))
                                if (relate(e1.geometry, e2.geometry, relation))
                                    matches.append((e1.id, e2.id))
                        }
                    }
                }
                matches.toIterator
            }
    }


    }
