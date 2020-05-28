package EntityMatching.PartitionMatching


import DataStructures.SpatialEntity
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import utils.Constants
import utils.Readers.SpatialReader


case class ComparisonCentricPrioritization(joinedRDD: RDD[(Int, (List[SpatialEntity], List[SpatialEntity]))],
                                           thetaXY: (Double, Double), weightingScheme: String) extends PartitionMatchingTrait {


    /**
     * First index source and then for each entity of target, find its comparisons from source's index.
     * Weight the comparisons based the weighting scheme and then perform them in a ascending way.
     *
     * For the weighting, for each entity of target we construct a matrix that contains the frequencies
     * of each entity of source - i.e. the no of common blocks
     *
     * @param relation the examining relation
     * @return an RDD containing the matching pairs
     */
    def apply(relation: String): RDD[(String, String)] ={
        init()
        joinedRDD.flatMap { p =>
            val partitionId = p._1
            val source = p._2._1
            val target = p._2._2
            val sourceIndex = index(source, partitionId)

            target
                .map(e2 => (indexSpatialEntity(e2, partitionId), e2))
                .flatMap { case (coordsAr: Array[(Int, Int)], e2: SpatialEntity) =>
                    val sIndices = coordsAr
                        .filter(c => sourceIndex.contains(c))
                        .map(c => (c, sourceIndex.get(c)))
                    val frequency = Array.fill(source.size)(0)
                    sIndices.flatMap(_._2).foreach(i => frequency(i) += 1)

                    sIndices.flatMap { case (c, indices) =>
                        indices.map(i => (source(i), frequency(i)))
                            .filter { case (e1, _) => e1.mbb.testMBB(e2.mbb, relation) && e1.mbb.referencePointFiltering(e2.mbb, c) }
                            .map { case (e1, f) => (getWeight(f, e1, e2), (e1, e2)) }
                    }
                }
        }
        .sortByKey(ascending = false)
        .map(_._2)
        .filter(c => relate(c._1.geometry, c._2.geometry, relation))
        .map(c => (c._1.originalID, c._2.originalID))
    }
}

object ComparisonCentricPrioritization {

    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaMsrSTR: String, weightingScheme: String): ComparisonCentricPrioritization ={
        val thetaXY = initTheta(source, target, thetaMsrSTR)
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), List(se))).reduceByKey(_ ++ _)
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), List(se))).reduceByKey(_ ++ _)

        val joinedRDD = sourcePartitions.join(targetPartitions, SpatialReader.spatialPartitioner)
        ComparisonCentricPrioritization(joinedRDD, thetaXY, weightingScheme)
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
