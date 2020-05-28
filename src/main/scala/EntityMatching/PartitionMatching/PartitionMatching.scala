package EntityMatching.PartitionMatching

import DataStructures.SpatialEntity
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import utils.Constants
import utils.Readers.SpatialReader

case class PartitionMatching(joinedRDD: RDD[(Int, (List[SpatialEntity],  List[SpatialEntity]))],
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
                    e1.mbb.testMBB(e2.mbb, relation) && e1.mbb.referencePointFiltering(e2.mbb, b)
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
       val thetaXY = initTheta(source, target, thetaMsrSTR)
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), List(se))).reduceByKey(_ ++ _)
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), List(se))).reduceByKey(_ ++ _)

        val joinedRDD = sourcePartitions.join(targetPartitions, SpatialReader.spatialPartitioner)
        PartitionMatching(joinedRDD, thetaXY, weightingScheme)
    }


    /**
     * initialize theta based on theta measure
     */
    def initTheta(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaMsrSTR: String): (Double, Double) ={
        val thetaMsr: RDD[(Double, Double)] = source
            .union(target)
            .map {
                sp =>
                    val env = sp.geometry.getEnvelopeInternal
                    (env.getHeight, env.getWidth)
            }
            .setName("thetaMsr")
            .cache()

        var thetaX = 1d
        var thetaY = 1d
        thetaMsrSTR match {
            case Constants.MIN =>
                // filtering because there are cases that the geometries are perpendicular to the axes
                // and have width or height equals to 0.0
                thetaX = thetaMsr.map(_._1).filter(_ != 0.0d).min
                thetaY = thetaMsr.map(_._2).filter(_ != 0.0d).min
            case Constants.MAX =>
                thetaX = thetaMsr.map(_._1).max
                thetaY = thetaMsr.map(_._2).max
            case Constants.AVG =>
                val length = thetaMsr.count
                thetaX = thetaMsr.map(_._1).sum() / length
                thetaY = thetaMsr.map(_._2).sum() / length
            case _ =>
        }
        (thetaX, thetaY)
    }

}
