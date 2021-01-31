package EntityMatching.DistributedMatching

import DataStructures.{ComparisonPQ, Entity, IM, MBB}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.spark_project.guava.collect.MinMaxPriorityQueue
import utils.Constants.Relation
import utils.Constants.Relation.Relation
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils


case class GIAnt(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))],
                 thetaXY: (Double, Double), ws: WeightStrategy, budget: Int, sourceCount: Long) extends DMProgressiveTrait {

    /**
     * First index the Source and then use the index to find the comparisons with target's entities.
     * Filter the redundant comparisons using testMBB and RF
     *
     * @param relation the examining relation
     * @return an RDD containing the matching pairs
     */
    override def relate(relation: Relation): RDD[(String, String)] = joinedRDD
        .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty )
        .flatMap { p =>
            val pid = p._1
            val partition = partitionsZones(pid)
            val source: Array[Entity] = p._2._1.toArray
            val target: Iterator[Entity] = p._2._2.toIterator
            val sourceIndex = index(source)
            val filteringFunction = (b: (Int, Int)) => sourceIndex.contains(b)

            target.flatMap{ e2 =>
                e2
                    .index(thetaXY, filteringFunction)
                    .view
                    .flatMap(block => sourceIndex.get(block).map(i => (block, i)))
                    .filter{ case(block, i) => source(i).filter(e2, relation, block, thetaXY, Some(partition))}
                    .filter{ case (_, i) => source(i).relate(e2, relation)}
                    .map(_._2)
                    .map(i => (source(i).originalID, e2.originalID))
                    .force
            }
        }


    /**
     * compute the Intersection Matrix of the input datasets
     * @return an RDD of intersection matrix
     */
    override def getDE9IM: RDD[IM] ={
        joinedRDD.filter(j => j._2._1.nonEmpty && j._2._2.nonEmpty)
            .flatMap { p =>
            val pid = p._1
            val partition = partitionsZones(pid)
            val source: Array[Entity] = p._2._1.toArray
            val target: Iterable[Entity] = p._2._2
            val sourceIndex = index(source)
            val filteringFunction = (b:(Int, Int)) => sourceIndex.contains(b)

           target.flatMap { e2 =>
               e2
                    .index(thetaXY, filteringFunction)
                    .view
                    .flatMap(c => sourceIndex.get(c).map(i => (c, i)))
                    .filter{ case(block, i) => source(i).filter(e2, Relation.DE9IM, block, thetaXY, Some(partition))}
                    .map(_._2)
                    .map(i => IM(source(i), e2))
                    .filter(_.relate)
                    .force
            }
        }
    }

    /**
     * No prioritization, the comparisons are performed in the same
     * order as provided
     * @param source source dataset
     * @param target target dataset
     * @param partition current partition
     * @param relation examining relation
     * @return a PQ containing pseudo-prioritized comparisons
     */
    def prioritize(source: Array[Entity], target: Array[Entity], partition: MBB, relation: Relation): ComparisonPQ[(Int, Int)] ={
        val sourceIndex = index(source)
        val filterIndices = (b: (Int, Int)) => sourceIndex.contains(b)
        val pq: ComparisonPQ[(Int, Int)] = ComparisonPQ[(Int, Int)](budget)

       // put comparisons in the PQ in the provided order
        target
            .indices
            .map {j =>
                val e2 = target(j)
                e2.index(thetaXY, filterIndices)
                    .foreach { block =>
                        sourceIndex.get(block)
                            .filter(i => source(i).filter(e2, relation, block, thetaXY, Some(partition)))
                            .foreach { i =>  pq.enqueue(1d, (i, j)) }
                    }
            }.takeWhile(_ => pq.size() <= budget)
        pq
    }
}

/**
 * auxiliary constructor
 */
object GIAnt{

    def apply(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)], ws: WeightStrategy, budget: Int, partitioner: Partitioner): GIAnt ={
        val thetaXY = Utils.getTheta
        val joinedRDD = source.cogroup(target, partitioner)
        val sourceCount = Utils.getSourceCount
        GIAnt(joinedRDD, thetaXY, ws, budget, sourceCount)
    }
}
