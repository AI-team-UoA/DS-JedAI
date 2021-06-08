package interlinkers

import model.entities.Entity
import model.{IM, SpatialIndex, TileGranularities}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants.Relation
import utils.configuration.Constants.Relation.Relation
import utils.configuration.Constants.WeightingFunction.WeightingFunction




case class IndexedJoinInterlinking(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)],
                                   tileGranularities: TileGranularities, partitionBorders: Array[Envelope]
                                  ) extends InterlinkerT {

    val joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))] = null
    val wf: WeightingFunction = null
    val partitioner = new HashPartitioner(source.getNumPartitions)

    val filteringFunction: ((Int, Entity),  (Int, Entity), (Int, Int), Relation) => Boolean =
        (s: (Int, Entity), t: (Int, Entity), c: (Int, Int), r: Relation) =>
            s._1 == t._1 && filterVerifications(s._2, t._2, r, c, partitionBorders(s._1))


    def indexedJoin(): RDD[((Int, Int), (Iterable[(Int, Entity)], Iterable[(Int, Entity)]))] = {
        val index = SpatialIndex[Entity](Array(), tileGranularities)
        val indexedSource: RDD[((Int, Int), Iterable[(Int, Entity)])] = source
            .map(se => (index.index(se._2), se))
            .flatMap{ case (indices, (pid, se)) => indices.map(i => (i, (pid, se)))}
            .groupByKey(partitioner)

        val indexedTarget: RDD[((Int, Int), Iterable[(Int, Entity)])] = target
            .map(se =>  (index.index(se._2), se))
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
                    yield s._2.getIntersectionMatrix(t._2)
            }
    }


}
