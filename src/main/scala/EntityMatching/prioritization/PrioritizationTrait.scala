package EntityMatching.prioritization

import DataStructures.{Block, TBlock}
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.rdd.RDD


import scala.collection.mutable.ArrayBuffer

trait PrioritizationTrait extends Serializable{
    var totalBlocks: Long = -1

    /**
     * Set totalBlocks
     * @param total_blocks no Blocks
     */
    def setTotalBlocks(total_blocks: Long): Unit = totalBlocks = total_blocks

    def normalizeWeight(weight: Double, entity1: Geometry, entity2:Geometry): Double = weight/(entity1.getArea * entity2.getArea)

    def apply(blocks: RDD[Block], relation: String, weightingStrategy: String, cleanStrategy: String): RDD[(Long,Long)]

    def getWeights(blocks: RDD[TBlock], weightingStrategy: String): RDD[((Long, Double), ArrayBuffer[Long])]

}
