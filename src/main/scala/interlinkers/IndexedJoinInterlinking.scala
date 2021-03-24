package interlinkers

import model.{Entity, IM}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import utils.Constants.Relation
import utils.Constants.Relation.Relation
import utils.Constants.WeightingScheme.WeightingScheme




case class IndexedJoinInterlinking(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)], thetaXY: (Double, Double)) extends InterlinkerT {

    val joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))] = null
    val ws: WeightingScheme = null
    val partitioner = new HashPartitioner(source.getNumPartitions)

    val filteringFunction: ((Int, Entity),  (Int, Entity), (Int, Int), Relation) => Boolean =
        (e1: (Int, Entity), e2: (Int, Entity), c: (Int, Int), r: Relation) =>
            e1._1 == e2._1 && e1._2.filter(e2._2, r, c, thetaXY, Some(partitionsZones(e1._1)))


    def indexedJoin(): RDD[((Int, Int), (Iterable[(Int, Entity)], Iterable[(Int, Entity)]))] = {
        val indexedSource: RDD[((Int, Int), Iterable[(Int, Entity)])] = source
            .map(se => (se._2.index(thetaXY), se))
            .flatMap{ case (indices, (pid, se)) => indices.map(i => (i, (pid, se)))}
            .groupByKey(partitioner)

        val indexedTarget: RDD[((Int, Int), Iterable[(Int, Entity)])] = target
            .map(se => (se._2.index(thetaXY), se))
            .flatMap{ case (indices, (pid, se)) => indices.map(i => (i, (pid, se)))}
            .groupByKey(partitioner)

        indexedSource.leftOuterJoin(indexedTarget, partitioner)
            .filter(_._2._2.isDefined)
            .map(p => (p._1, (p._2._1, p._2._2.get)))
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
                for (e1 <- source; e2 <- target; if filteringFunction(e1, e2, c, relation) && e1._2.relate(e2._2, relation))
                    yield (e1._2.originalID, e2._2.originalID)
            }


    def getDE9IM: RDD[IM] = {
        val indexedSeq = indexedJoin()
        indexedSeq
            .flatMap { case (c: (Int, Int), (source: Iterable[(Int, Entity)], target: Iterable[(Int, Entity)])) =>
                for (e1 <- source; e2 <- target; if filteringFunction(e1, e2, c, Relation.DE9IM))
                    yield IM(e1._2, e2._2)
            }
    }


}
