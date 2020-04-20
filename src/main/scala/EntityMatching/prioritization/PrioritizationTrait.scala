package EntityMatching.prioritization

import DataStructures.{Block, TBlock}
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.rdd.RDD


import scala.collection.mutable.ArrayBuffer

trait PrioritizationTrait extends Serializable{
    val totalBlocks: Long
    val weightingStrategy: String

    def normalizeWeight(weight: Double, entity1: Geometry, entity2:Geometry): Double = weight/(entity1.getArea * entity2.getArea)

    def apply(blocks: RDD[Block], relation: String, cleanStrategy: String): RDD[(Long,Long)]

    def getWeights(blocks: RDD[TBlock]): RDD[((Long, Double), ArrayBuffer[Long])]

}
