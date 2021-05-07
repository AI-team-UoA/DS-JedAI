package interlinkers

import model.entities.Entity
import model.{IM, MBR}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import utils.Constants.Relation
import utils.Constants.Relation.Relation
import utils.Constants.WeightingFunction.WeightingFunction




case class IndexedJoinInterlinking(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)],
                                   thetaXY: (Double, Double), partitionBorders: Array[MBR]
                                  ) extends InterlinkerT {

    val joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))] = null
    val wf: WeightingFunction = null
    val partitioner = new HashPartitioner(source.getNumPartitions)

    val filteringFunction: ((Int, Entity),  (Int, Entity), (Int, Int), Relation) => Boolean =
        (s: (Int, Entity), t: (Int, Entity), c: (Int, Int), r: Relation) =>
            s._1 == t._1 && s._2.filter(t._2, r, c, thetaXY, Some(partitionBorders(s._1)))


    def indexedJoin(): RDD[((Int, Int), (Iterable[(Int, Entity)], Iterable[(Int, Entity)]))] = {
        val indexedSource: RDD[((Int, Int), Iterable[(Int, Entity)])] = source
            .map(se => (se._2.index(thetaXY), se))
            .flatMap{ case (indices, (pid, se)) => indices.map(i => (i, (pid, se)))}
            .groupByKey(partitioner)

        val indexedTarget: RDD[((Int, Int), Iterable[(Int, Entity)])] = target
            .map(se => (se._2.index(thetaXY), se))
            .flatMap{ case (indices, (pid, se)) => indices.map(i => (i, (pid, se)))}
            .groupByKey(partitioner)

        indexedSource.join(indexedTarget, partitioner)
    }

    /**
     * First index Source and then use index to find the comparisons with the entities of Target.
     * Filter the redundant comparisons using the spatial Filters
     *
     * @param relation the examining relation
     * @return an RDD containing the matching pairs
     */
    def relate(relation: Relation): RDD[(String, String)] =
        indexedJoin()
            .flatMap { case (c: (Int, Int), ( source: Iterable[(Int, Entity)], target: Iterable[(Int, Entity)])) =>
                for (s <- source; t <- target; if filteringFunction(s, t, c, relation) && s._2.relate(t._2, relation))
                    yield (s._2.originalID, t._2.originalID)
            }


    def getDE9IM: RDD[IM] = {
        val indexedSeq = indexedJoin()
        indexedSeq
            .flatMap { case (c: (Int, Int), (source: Iterable[(Int, Entity)], target: Iterable[(Int, Entity)])) =>
                for (s <- source; t <- target; if filteringFunction(s, t, c, Relation.DE9IM))
                    yield IM(s._2, t._2)
            }
    }


}