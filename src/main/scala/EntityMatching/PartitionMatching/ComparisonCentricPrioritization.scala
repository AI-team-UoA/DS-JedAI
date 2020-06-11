package EntityMatching.PartitionMatching


import DataStructures.SpatialEntity
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import utils.{Constants, Utils}
import utils.Readers.SpatialReader


case class ComparisonCentricPrioritization(joinedRDD: RDD[(Int, (Iterable[SpatialEntity], Iterable[SpatialEntity]))],
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
        joinedRDD.flatMap { p =>
            val partitionId = p._1
            val source = p._2._1.toArray
            val target = p._2._2.toIterator
            val sourceIndex = index(source, partitionId)

            target
                .map(e2 => (e2.index(thetaXY, zoneCheck(partitionId)), e2))
                .flatMap { case (coordsAr: Array[(Int, Int)], e2: SpatialEntity) =>
                    val sIndices = coordsAr
                        .filter(c => sourceIndex.contains(c))
                        .map(c => (c, sourceIndex.get(c)))
                    val frequency = Array.fill(source.length)(0)
                    sIndices.flatMap(_._2).foreach(i => frequency(i) += 1)

                    sIndices.flatMap { case (c, indices) =>
                        indices.map(i => (source(i), frequency(i)))
                            .filter { case (e1, _) => e1.mbb.testMBB(e2.mbb, relation) && e1.mbb.referencePointFiltering(e2.mbb, c, thetaXY) }
                            .map { case (e1, f) => (getWeight(f, e1, e2), (e1, e2)) }
                    }
                }
        }
        .sortByKey(ascending = false)
        .map(_._2)
        .filter(c => c._1.relate(c._2, relation))
        .map(c => (c._1.originalID, c._2.originalID))
    }
}


/**
 * auxiliary constructor
 */
object ComparisonCentricPrioritization {

    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaMsrSTR: String,
              weightingScheme: String = Constants.NO_USE): ComparisonCentricPrioritization ={
        val thetaXY = Utils.initTheta(source, target, thetaMsrSTR)
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), se))
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), se))

        val joinedRDD = sourcePartitions.cogroup(targetPartitions, SpatialReader.spatialPartitioner)
        ComparisonCentricPrioritization(joinedRDD, thetaXY, weightingScheme)
    }

}
