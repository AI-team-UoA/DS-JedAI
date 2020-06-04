package EntityMatching.PartitionMatching

import DataStructures.SpatialEntity
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import utils.{Constants, Utils}
import utils.Readers.SpatialReader

case class PartitionMatching(joinedRDD: RDD[(Int, (Array[SpatialEntity],  Array[SpatialEntity]))],
                             thetaXY: (Double, Double), weightingScheme: String) extends PartitionMatchingTrait {


    /**
     * First index the source and then use the index to find the comparisons with target's entities.
     *
     * @param relation the examining relation
     * @return an RDD containing the matching pairs
     */
    def apply(relation: String): RDD[(String, String)] ={
        adjustPartitionsZones()
        joinedRDD.flatMap { p =>
            val partitionId = p._1
            val source = p._2._1
            val target = p._2._2
            val sourceIndex = index(source, partitionId)

            target
                .map(se => (indexSpatialEntity(se, partitionId), se))
                .flatMap { case (coordsAr: Array[(Int, Int)], se: SpatialEntity) =>
                    coordsAr
                        .filter(c => sourceIndex.contains(c))
                        .flatMap(c => sourceIndex.get(c).map(j => (source(j), se, c)))
                }
                .filter { case (e1: SpatialEntity, e2: SpatialEntity, b: (Int, Int)) =>
                    e1.mbb.testMBB(e2.mbb, relation) && e1.mbb.referencePointFiltering(e2.mbb, b, thetaXY)
                }
                .filter(c => relate(c._1.geometry, c._2.geometry, relation))
                .map(c => (c._1.originalID, c._2.originalID))
        }
    }


}

/**
 * auxiliary constructor
 */
object PartitionMatching{

    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaMsrSTR: String, weightingScheme: String = Constants.NO_USE): PartitionMatching ={
       val thetaXY = Utils.initTheta(source, target, thetaMsrSTR)
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), Array(se))).reduceByKey(SpatialReader.spatialPartitioner, _ ++ _)
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), Array(se))).reduceByKey(SpatialReader.spatialPartitioner, _ ++ _)

        val joinedRDD = sourcePartitions.join(targetPartitions, SpatialReader.spatialPartitioner)
        joinedRDD.setName("JoinedRDD").persist(StorageLevel.MEMORY_AND_DISK)
        PartitionMatching(joinedRDD, thetaXY, weightingScheme)
    }

}
