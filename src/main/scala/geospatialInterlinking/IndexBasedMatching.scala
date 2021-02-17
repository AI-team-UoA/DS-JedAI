package geospatialInterlinking

import dataModel.{Entity, IM}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import utils.Constants.Relation
import utils.Constants.Relation.Relation
import utils.Constants.WeightStrategy.WeightStrategy

import scala.collection.mutable.ListBuffer

case class IndexBasedMatching(source:RDD[Entity], target:RDD[Entity], thetaXY: (Double, Double)) extends GeospatialInterlinkingT {

    val joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))] = null
    val ws: WeightStrategy = null

    val filteringFunction: ((Entity, Int), (Entity, Int), (Int, Int), Relation) => Boolean =
        (e1: (Entity, Int), e2: (Entity, Int), c: (Int, Int), r: Relation) => e1._2 == e2._2 && e1._1.filter(e2._1, r, c, thetaXY)

    /**
     * First index the Source and then use the index to find the comparisons with target's entities.
     * Filter the redundant comparisons using testMBR and RF
     *
     * @param relation the examining relation
     * @return an RDD containing the matching pairs
     */
    def relate(relation: Relation): RDD[(String, String)] = {

        val indexedSource = source
            .map(se => (se.index(thetaXY), (se, TaskContext.getPartitionId())))
            .flatMap{case (indices, (se, pid)) => indices.map(i => (i, ListBuffer((se, pid))))}
            .reduceByKey(_ ++ _)
        val partitioner = indexedSource.partitioner.get

        val indexedTarget = target
            .map(se => (se.index(thetaXY), (se, TaskContext.getPartitionId())))
            .flatMap{case (indices, (se, pid)) => indices.map(i => (i, ListBuffer((se, pid))))}
            .reduceByKey(partitioner, _ ++ _)

        indexedSource.leftOuterJoin(indexedTarget, partitioner)
            .filter(_._2._2.isDefined)
            .flatMap { case (c: (Int, Int), (s: ListBuffer[(Entity, Int)], optT: Option[ListBuffer[(Entity, Int)]])) =>
                for (e1 <- s; e2 <- optT.get; if filteringFunction(e1, e2, c, relation))
                    yield (e1._1.originalID, e2._1.originalID)
            }
    }



    def getDE9IM: RDD[IM] = {
        val indexedSource = source
            .map(se => (se.index(thetaXY), (se, TaskContext.getPartitionId())))
            .flatMap{case (indices, (se, pid)) => indices.map(i => (i, ListBuffer((se, pid))))}
            .reduceByKey(_ ++ _)
        val partitioner = indexedSource.partitioner.get

        val indexedTarget = target
            .map(se => (se.index(thetaXY), (se, TaskContext.getPartitionId())))
            .flatMap{case (indices, (se, pid)) => indices.map(i => (i, ListBuffer((se, pid))))}
            .reduceByKey(partitioner, _ ++ _)

        indexedSource.leftOuterJoin(indexedTarget, partitioner)
            .filter(_._2._2.isDefined)
            .flatMap { case (c: (Int, Int), (s: ListBuffer[(Entity, Int)], optT: Option[ListBuffer[(Entity, Int)]])) =>
                for (e1 <- s; e2 <- optT.get; if filteringFunction(e1, e2, c, Relation.DE9IM)) yield IM(e1._1, e2._1)
            }
    }


}
