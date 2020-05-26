package EntityMatching.BlockBasedAlgorithms

import DataStructures.Block
import breeze.linalg.{DenseVector, SparseVector}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.Constants


/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

// TODO consider placing relation in apply
case class BlockCentricPrioritization(blocks: RDD[Block], d: (Int, Int), totalBlocks: Long,
                                      weightingScheme: String) extends BlockMatchingTrait  {


    var commonBlocksSMBD: Broadcast[Array[SparseVector[Int]]] = _
    var entitiesBlocksBD: Broadcast[(DenseVector[Int], DenseVector[Int])] = _
    var arcsWeightsBD:  Broadcast[Array[SparseVector[Double]]] = _

    /**
     * Construct and broadcast an array of sparse vectors containing the no common blocks
     * per pair of entities. The rows indicate the source entities and the columns indicate
     * target's entities
     *
     * @param sc Spark context
     */
    def setCommonBlocks(sc: SparkContext): Unit ={
        val commonBlocksSM:Array[SparseVector[Int]] = blocks.flatMap{
            b =>
                val sIdDs = b.getSourceIDs
                val tIDs = b.getTargetIDs.to[Array]
                val (minS, maxS) = (sIdDs.min, sIdDs.max)
                val sourceSet = sIdDs.toSet
                val onesSV = new SparseVector[Int](tIDs, Array.fill(tIDs.length)(1), d._2)
                val zerosSV = SparseVector.zeros[Int](d._2)

                (minS to maxS).map(id => if (sourceSet.contains(id)) (id, onesSV)  else (id, zerosSV) )
        }
        .reduceByKey( _ + _ )
        .map(_._2)
        .collect()

        commonBlocksSMBD = sc.broadcast(commonBlocksSM)
    }


    /**
     * Construct and broadcast dense vectors where each value indicates the
     * no block the respective entity of the index is inside.
     * @param sc Spark context
     */
    def setEntitiesBlocks(sc: SparkContext): Unit ={
        val entitiesBlocks = blocks.map {
            b =>
                val sourceBlocks = DenseVector.zeros[Int](d._1)
                val targetBlocks = DenseVector.zeros[Int](d._2)
                b.getSourceIDs.foreach(i => sourceBlocks.update(i, sourceBlocks(i) + 1))
                b.getTargetIDs.foreach(j => targetBlocks.update(j, targetBlocks(j) + 1))
                (sourceBlocks, targetBlocks)
        }
            .reduce((a, b) => (a._1 + b._1, a._2 + b._2))
        entitiesBlocksBD = sc.broadcast(entitiesBlocks)
    }

    /**
     * set ARCS weights
     */
    def setARCSWeights(sc: SparkContext): Unit ={
        val arcsWeights = blocks.flatMap{
            b =>
                val w = 1d / b.getTotalComparisons
                val sIdDs = b.getSourceIDs
                val tIDs = b.getTargetIDs.to[Array]
                val (minS, maxS) = (sIdDs.min, sIdDs.max)
                val sourceSet = sIdDs.toSet
                val sv = new SparseVector[Double](tIDs, Array.fill(tIDs.length)(w), d._2)
                val zerosSV = SparseVector.zeros[Double](d._2)

                (minS to maxS).map(id => if (sourceSet.contains(id)) (id, sv)  else (id, zerosSV) )
        }
        .reduceByKey( _ + _ )
        .map(_._2)
        .collect()

        arcsWeightsBD = sc.broadcast(arcsWeights)
    }

    /**
     * Initialize all auxiliary structures based on weighting scheme
     */
    def init(): Unit ={
        val sc: SparkContext = SparkContext.getOrCreate()
        weightingScheme match {
            case Constants.ARCS =>
                setARCSWeights(sc)
            case Constants.ECBS | Constants.JS =>
                setCommonBlocks(sc)
                setEntitiesBlocks(sc)
            case Constants.CBS =>
               setCommonBlocks(sc)
        }
    }

    /**
     * Get blocks' comparisons and filter the unnecessary ones. Then weight the comparisons
     * based on an block centric way and the weighting scheme. Then sort and execute the
     * comparisons.
     *
     * @return an RDD containing the IDs of the matches
     */
    def apply(relation: String): RDD[(String, String)] ={
        init()
        // TODO normalize weights
        blocks.flatMap(b => b.getFilteredComparisons(relation))
            .map(c => (getWeights(c.entity1.id, c.entity2.id), c))
            .sortByKey(ascending = false)
            .map(_._2)
            .filter(c => relate(c.entity1.geometry, c.entity2.geometry, relation))
            .map(c => (c.entity1.originalID, c.entity2.originalID))
    }


    /**
     * Weight the comparisons of blocks and clean the duplicate comparisons.
     * The accepted weighing strategies are CBS(default), ECBS, ARCS and JS
     *
     * @return the weighted comparisons of each block
     */
    def getWeights(e1: Int, e2: Int): Double ={

        weightingScheme match {
            case Constants.ARCS =>
                arcsWeightsBD.value(e1)(e2)
            case Constants.ECBS =>
                val e1Blocks = entitiesBlocksBD.value._1(e1)
                val e2Blocks = entitiesBlocksBD.value._2(e2)
                val cb = commonBlocksSMBD.value(e1)(e2)
                cb * math.log10(totalBlocks / e1Blocks) * math.log10(totalBlocks/e2Blocks)
            case Constants.JS =>
                val e1Blocks = entitiesBlocksBD.value._1(e1)
                val e2Blocks = entitiesBlocksBD.value._2(e2)
                val cb = commonBlocksSMBD.value(e1)(e2)
                cb / (e1Blocks + e2Blocks - cb)
            case Constants.CBS | _ =>
                commonBlocksSMBD.value(e1)(e2)
        }
    }

}

object BlockCentricPrioritization {

    // auxiliary constructors
    def apply(blocks: RDD[Block], d: (Int, Int), weightingScheme : String): BlockCentricPrioritization ={
        BlockCentricPrioritization(blocks, d, blocks.count(), weightingScheme)
    }

    def apply(blocks: RDD[Block], weightingScheme : String): BlockCentricPrioritization ={
        val d1 = blocks.map(b => b.getSourceIDs.toSet).reduce(_ ++ _).size
        val d2 = blocks.map(b => b.getTargetIDs.toSet).reduce(_ ++ _).size
        BlockCentricPrioritization(blocks, (d1, d2), blocks.count(), weightingScheme)
    }
}
