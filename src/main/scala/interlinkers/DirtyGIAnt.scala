package interlinkers

import model.{Entity, IM, MBR, SpatialIndex}
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import utils.Constants.Relation
import utils.Constants.Relation.Relation
import utils.Utils

case class DirtyGIAnt(source:RDD[Entity], thetaXY: (Double, Double)) {

    val partitionsZones: Array[MBR] = SparkContext.getOrCreate().broadcast(Utils.getZones).value

    /**
     * Extract the candidate geometries from source, using spatial index
     * @param e2 target geometry
     * @param source array of source geometries
     * @param index spatial index
     * @param partitionZone examining partition
     * @param relation examining relations
     * @return a sequence of candidate geometries
     */
    def getCandidates(e2: Entity, source: Array[Entity], index: SpatialIndex, partitionZone: MBR, relation: Relation): Seq[Entity] =
        e2.index(thetaXY, index.contains).view
            .flatMap(block => index.get(block).map(i => (block, i)))
            .filter{ case (block, i) => source(i).filter(e2, relation, block, thetaXY, Some(partitionZone))}
            .map{case (_, i) => source(i) }
            .force

    /**
     * index a list of spatial entities
     *
     * @param entities list of spatial entities
     * @return a SpatialIndex
     */
    def index(entities: Array[Entity]): SpatialIndex = {
        val spatialIndex = new SpatialIndex()
        entities.zipWithIndex.foreach { case (se, i) =>
            val indices: Seq[(Int, Int)] = se.index(thetaXY)
            indices.foreach(c => spatialIndex.insert(c, i))
        }
        spatialIndex
    }



    def getDE9IM: RDD[IM] = {
        source
            .mapPartitions { p =>
                val partition = partitionsZones(TaskContext.getPartitionId())
                val source: Array[Entity] = p.toArray
                val sourceIndex = index(source)
                sourceIndex.getIndices.flatMap { b: (Int, Int) =>
                    val candidates = sourceIndex.get(b)
                    for (i <- candidates;
                         j <- candidates
                         if source(i).filter(source(j), Relation.DE9IM, b, thetaXY, Some(partition)))
                        yield{
                            val e1 = source(i)
                            val e2 = source(j)
                            if (i != j) IM(e1, e2)
                            else IM((e1.originalID, e2.originalID), isContains=true, isCovers=true, isCoveredBy=true,
                                isCrosses=false, isEquals=false, isIntersects=false, isOverlaps=false, isTouches=true,
                                isWithin=true)
                        }
                }.toIterator
            }
    }

}