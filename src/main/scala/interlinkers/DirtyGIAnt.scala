package interlinkers

import model.entities.Entity
import model.{IM, MBR, SpatialIndex}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import utils.Constants.Relation
import utils.Constants.Relation.Relation

case class DirtyGIAnt(source:RDD[Entity], partitionBorders: Array[MBR], thetaXY: (Double, Double)) {


    /**
     * Extract the candidate geometries from source, using spatial index
     * @param t target geometry
     * @param source array of source geometries
     * @param index spatial index
     * @param partitionZone examining partition
     * @param relation examining relations
     * @return a sequence of candidate geometries
     */
    def getCandidates(t: Entity, source: Array[Entity], index: SpatialIndex, partitionZone: MBR, relation: Relation): Seq[Entity] =
        t.index(thetaXY, index.contains).view
            .flatMap(block => index.get(block).map(i => (block, i)))
            .filter{ case (block, i) => source(i).filter(t, relation, block, thetaXY, Some(partitionZone))}
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
                val partition = partitionBorders(TaskContext.getPartitionId())
                val source: Array[Entity] = p.toArray
                val sourceIndex = index(source)
                sourceIndex.getIndices.flatMap { b: (Int, Int) =>
                    val candidates = sourceIndex.get(b)
                    for (i <- candidates;
                         j <- candidates
                         if source(i).filter(source(j), Relation.DE9IM, b, thetaXY, Some(partition)))
                        yield{
                            val s = source(i)
                            val t = source(j)
                            IM(s, t)
                        }
                }.toIterator
            }
    }

}