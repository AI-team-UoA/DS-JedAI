package EntityMatching.prioritization

import Blocking.BlockUtils.clean
import DataStructures.{Block, TBlock}
import EntityMatching.Matching.{relate, testMBB}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.{Constants, Utils}

import scala.collection.mutable.ArrayBuffer

case class BlockCentricPrioritization(setTotalBlocks: Long) extends  PrioritizationTrait  {



    def apply(blocks: RDD[Block], relation: String, weightingStrategy: String = Constants.CBS, cleaningStrategy: String = Constants.RANDOM):
    RDD[(Long,Long)] = {

        val weightedComparisonsPerBlock = getWeights(blocks.asInstanceOf[RDD[TBlock]], weightingStrategy)
            .asInstanceOf[RDD[(Any, ArrayBuffer[Long])]]
        val cleanWeightedComparisonsPerBlock = clean(weightedComparisonsPerBlock, cleaningStrategy)
            .asInstanceOf[ RDD[(Long, ArrayBuffer[(Long, Double)])]]
            .filter(_._2.nonEmpty)

        val blocksComparisons = blocks.map(b => (b.id, b))
        val matches = cleanWeightedComparisonsPerBlock.leftOuterJoin(blocksComparisons)
            .map{
                b =>
                    val comparisonsWeightsMap = b._2._1.toMap
                    val comparisons = b._2._2.get.getComparisons
                    val orderedComparisons = comparisons
                        .filter(c => comparisonsWeightsMap.contains(c.id))
                        .map{c =>
                            val weight = comparisonsWeightsMap(c.id)
                            val env1 = c.entity1.geometry.getEnvelope
                            val env2 = c.entity2.geometry.getEnvelope
                            val normalizedWeight = normalizeWeight(weight, env1, env2)
                            (normalizedWeight, c)
                        }
                        .sortBy(_._1)(Ordering.Double.reverse)
                    var matches: ArrayBuffer[(Long,Long)] = ArrayBuffer()
                    for (c <- orderedComparisons) {
                        val (s, t) = (c._2.entity1, c._2.entity2)
                        if (testMBB(s.mbb, t.mbb, relation))
                            if (relate(s.geometry, t.geometry, relation))
                                matches += ((s.id, t.id))
                    }
                    matches
            }
            .flatMap(m => m)
        matches
    }


    /**
     * Weight the comparisons of the blocks and clean the duplicate comparisons
     *
     * @param blocks blocks RDD
     * @param weightingStrategy the weighting strategy
     * @return the weighted comparisons of each block
     */
    def getWeights(blocks: RDD[TBlock], weightingStrategy: String = Constants.CBS): RDD[((Long, Double), ArrayBuffer[Long])] ={
        val sc = SparkContext.getOrCreate()

        val totalBlocksBD =
            if (weightingStrategy == Constants.ECBS) {
                if (totalBlocks == -1) totalBlocks = blocks.count()
                sc.broadcast(totalBlocks)
            }
            else null

        val entitiesBlockMapBD =
            if (weightingStrategy == Constants.ECBS || weightingStrategy == Constants.JS){
                val ce1:RDD[(Long, Long)] = blocks.flatMap(b => b.getSourceIDs.map(id => (b.id, id)))
                val ce2:RDD[(Long, Long)] = blocks.flatMap(b => b.getTargetIDs.map(id => (b.id, id)))
                val ce = ce1.union(ce2)
                    .map(c => (c._1, ArrayBuffer(c._2)))
                    .reduceByKey(_ ++ _)
                    .sortByKey()
                    .map(c => (c._1, c._2.toSet))
                    .collectAsMap()
                sc.broadcast(ce)
            }
            else null

         weightingStrategy match {
            case Constants.ARCS =>
                blocks
                    .map(b => (b.id, b.getComparisonsIDs))
                    .flatMap(b => b._2.map(c => (c, ArrayBuffer((b._1, b._2.size)))))
                    .reduceByKey(_ ++ _)
                    .map(c => ((c._1, c._2.map(b => 1.0/b._2).sum), c._2.map(_._1)))

            case Constants.ECBS =>
                blocks
                    .map(b => (b.id, b.getComparisonsIDs))
                    .flatMap(b => b._2.map(c => (c, ArrayBuffer(b._1 ))))
                    .reduceByKey(_ ++ _)
                    .map(b => (b._1, Utils.inversePairing(b._1), b._2))
                    .map{
                        b =>
                            val blocks = b._3
                            val comparisonID = b._1
                            val (entity1, entity2) = b._2
                            val blocksOfEntity1 = entitiesBlockMapBD.value(entity1.toInt)
                            val blocksOfEntity2 = entitiesBlockMapBD.value(entity2.toInt)
                            val commonBlocks = blocksOfEntity1.intersect(blocksOfEntity2)
                            val weight = commonBlocks.size * math.log10(totalBlocksBD.value / blocksOfEntity1.size) * math.log10(totalBlocksBD.value / blocksOfEntity2.size)
                            ((comparisonID, weight), blocks)
                    }
            case Constants.JS =>
                blocks
                    .map(b => (b.id, b.getComparisonsIDs))
                    .flatMap(b => b._2.map(c => (c, ArrayBuffer(b._1 ))))
                    .reduceByKey(_ ++ _)
                    .map(b => (b._1, Utils.inversePairing(b._1), b._2))
                    .map{
                        b =>
                            val blocks = b._3
                            val comparisonID = b._1
                            val (entity1, entity2) = b._2
                            val blocksOfEntity1 = entitiesBlockMapBD.value(entity1.toInt)
                            val blocksOfEntity2 = entitiesBlockMapBD.value(entity2.toInt)
                            val totalCommonBlocks = blocksOfEntity1.intersect(blocksOfEntity2).size
                            val denominator = blocksOfEntity1.size + blocksOfEntity2.size - totalCommonBlocks
                            val weight = totalCommonBlocks / denominator
                            ((comparisonID, weight), blocks)
                    }
            case Constants.CBS | _ =>
                blocks
                    .map(b => (b.id, b.getComparisonsIDs))
                    .flatMap(b => b._2.map(c => (c, ArrayBuffer(b._1))))
                    .reduceByKey(_ ++ _)
                    .map(c => ((c._1, c._2.length), c._2))
        }
    }

}
