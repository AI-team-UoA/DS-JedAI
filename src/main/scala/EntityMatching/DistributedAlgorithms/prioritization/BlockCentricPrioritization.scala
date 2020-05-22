package EntityMatching.DistributedAlgorithms.prioritization

import Blocking.BlockUtils.clean
import DataStructures.{Block, Comparison}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.{Constants, Utils}
import breeze.linalg.{SparseVector, DenseVector}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.distributed.IndexedRow

import scala.collection.mutable.ArrayBuffer

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */


case class BlockCentricPrioritization(blocks: RDD[Block], relation: String, d: (Int, Int), totalBlocks: Long,
                                      weightingScheme: String) extends DistributedProgressiveMatching  {

    var commonBlocksSMBD: Broadcast[Array[SparseVector[Int]]] = _
    var entitiesBlocksBD: Broadcast[(DenseVector[Int], DenseVector[Int])] = _

    // TODO comments
    def init(): Unit ={
        val sc: SparkContext = SparkContext.getOrCreate()
        // TODO: init values according to weightingScheme
        val commonBlocksM = Array.fill(d._1)(SparseVector.zeros[Int](d._2))
        val commonBlocksSM = blocks.flatMap{
            b =>
                val sIdDs = b.getSourceIDs
                val tIDs = b.getTargetIDs.to[Array]

                val sv = new SparseVector[Int](tIDs, Array.fill(tIDs.length)(1), d._2)
                sIdDs.map(id => (id, sv))
        }
        .reduceByKey( _ + _ )
        .collect()
        commonBlocksSM.foreach { case(id, sv) => commonBlocksM.update(id, sv) }
        commonBlocksSMBD = sc.broadcast(commonBlocksM)

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
     * For each block first calculate the weight of each comparison, and also
     * calculate to which block each comparison will be assigned to (clean). Then
     * after joining the weighted comparisons to each block, normalize the weights
     * and order descending way. Perform first the comparison with greater weights.
     *
     * During the matching, first test the relation to geometries's MBBs and then
     * performs the relation to the geometries
     *
     * @return an RDD containing the IDs of the matches
     *
     */// TODO fix description
    def apply(): RDD[(String, String)] ={
        init()
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
     * @return the weighted comparisons of each block //CMNT instead of returning blocks for each weighted comparisons, return just a block based on th chooseBlock
     */
    def getWeights(e1: Int, e2: Int): Double ={

        weightingScheme match {
            case Constants.ARCS =>
               0d // TODO ARCS 1/comparisons per block - of the blocks they belong to see
                  // https://app.assembla.com/spaces/jedaispatial/subversion/source/HEAD/trunk/src/progressive/BlockCentricPrioritization.java
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


    def apply(blocks: RDD[Block], relation: String, d: (Int, Int), weightingScheme : String): BlockCentricPrioritization ={
        BlockCentricPrioritization(blocks, relation, d, blocks.count(), weightingScheme)
    }

    def apply(blocks: RDD[Block], relation: String, weightingScheme : String): BlockCentricPrioritization ={
        val d1 = blocks.map(b => b.getSourceIDs.toSet).reduce(_ ++ _).size
        val d2 = blocks.map(b => b.getTargetIDs.toSet).reduce(_ ++ _).size
        BlockCentricPrioritization(blocks, relation, (d1, d2), blocks.count(), weightingScheme)
    }
}
