package EntityMatching.DistributedAlgorithms

import DataStructures.{Block, MBB, TBlock}
import com.vividsolutions.jts.geom.Geometry
import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.{Constants, Utils}

import scala.collection.mutable.ArrayBuffer

trait DistributedMatchingTrait extends Serializable{
    val totalBlocks: Long
    val weightingStrategy: String

    def apply(blocks: RDD[Block], relation: String, cleaningStrategy: String): RDD[(Int,Int)]


    /**
     * check the relation between two geometries
     *
     * @param sourceGeom geometry from source set
     * @param targetGeometry geometry from target set
     * @param relation requested relation
     * @return whether the relation is true
     */
    def relate(sourceGeom: Geometry, targetGeometry: Geometry, relation: String): Boolean ={
        relation match {
            case Constants.CONTAINS => sourceGeom.contains(targetGeometry)
            case Constants.INTERSECTS => sourceGeom.intersects(targetGeometry)
            case Constants.CROSSES => sourceGeom.crosses(targetGeometry)
            case Constants.COVERS => sourceGeom.covers(targetGeometry)
            case Constants.COVEREDBY => sourceGeom.coveredBy(targetGeometry)
            case Constants.OVERLAPS => sourceGeom.overlaps(targetGeometry)
            case Constants.TOUCHES => sourceGeom.touches(targetGeometry)
            case Constants.DISJOINT => sourceGeom.disjoint(targetGeometry)
            case Constants.EQUALS => sourceGeom.equals(targetGeometry)
            case Constants.WITHIN => sourceGeom.within(targetGeometry)
            case _ => false
        }
    }


    /**
     *  check relation among MBBs
     *
     * @param s MBB from source
     * @param t MBB form target
     * @param relation requested relation
     * @return whether the relation is true
     */
    def testMBB(s:MBB, t:MBB, relation: String): Boolean ={
        relation match {
            case Constants.CONTAINS | Constants.COVERS =>
                s.contains(t)
            case Constants.WITHIN | Constants.COVEREDBY =>
                s.within(t)
            case Constants.INTERSECTS | Constants.CROSSES | Constants.OVERLAPS =>
                s.intersects(t)
            case Constants.TOUCHES => s.touches(t)
            case Constants.DISJOINT => s.disjoint(t)
            case Constants.EQUALS => s.equals(t)
            case _ => false
        }
    }


    def normalizeWeight(weight: Double, entity1: Geometry, entity2:Geometry): Double ={
        val area1 = entity1.getArea
        val area2 = entity2.getArea
        if (area1 == 0 || area2 == 0 ) weight
        else weight/(entity1.getArea * entity2.getArea)
    }


    /**
     * Weight the comparisons of blocks and clean the duplicate comparisons.
     * The accepted weighing strategies are CBS(default), ECBS, JS and PEARSON_X
     *
     * @param blocks blocks RDD
     * @return the weighted comparisons of each block
     */
    def getWeights(blocks: RDD[TBlock]): RDD[((Long, Double), ArrayBuffer[Long])] ={
        val sc = SparkContext.getOrCreate()
        val totalBlocksBD = sc.broadcast(totalBlocks)

        val entitiesBlockMapBD =
            if (weightingStrategy != Constants.CBS ){
                val ce1:RDD[(Int, Long)] = blocks.flatMap(b => b.getSourceIDs.map(id => (id, b.id)))
                val ce2:RDD[(Int, Long)] = blocks.flatMap(b => b.getTargetIDs.map(id => (id, b.id)))
                val ce = ce1.union(ce2)
                    .map(c => (c._1, ArrayBuffer(c._2)))
                    .reduceByKey(_ ++ _)
                    .map(c => (c._1, c._2.toSet))
                    .collectAsMap()
                sc.broadcast(ce)
            }
            else null

        weightingStrategy match {
            case Constants.ECBS =>
                blocks
                    .map(b => (b.id, b.getComparisonsPairs))
                    .flatMap(b => b._2.map(pair => (pair, ArrayBuffer(b._1))))
                    .reduceByKey(_ ++ _)
                    .map{ c=>
                        val (sourceID, targetID) = c._1
                        val blockIDs = c._2
                        val comparisonID = Utils.bijectivePairing(sourceID, targetID)
                        val e1Blocks = entitiesBlockMapBD.value(sourceID)
                        val e2Blocks = entitiesBlockMapBD.value(targetID)
                        val commonBlocks = e1Blocks.intersect(e2Blocks)
                        val weight = commonBlocks.size * math.log10(totalBlocksBD.value/e1Blocks.size) * math.log10(totalBlocksBD.value/e2Blocks.size)
                        ((comparisonID, weight), blockIDs)
                    }
            case Constants.JS =>
                blocks
                    .map(b => (b.id, b.getComparisonsPairs))
                    .flatMap(b => b._2.map(pair => (pair, ArrayBuffer(b._1))))
                    .reduceByKey(_ ++ _)
                    .map { c =>
                        val (sourceID, targetID) = c._1
                        val blockIDs = c._2
                        val comparisonID = Utils.bijectivePairing(sourceID, targetID)
                        val e1Blocks = entitiesBlockMapBD.value(sourceID)
                        val e2Blocks = entitiesBlockMapBD.value(targetID)
                        val commonBlocks = e1Blocks.intersect(e2Blocks)
                        val denominator = e1Blocks.size + e2Blocks.size - commonBlocks.size
                        val weight = commonBlocks.size / denominator
                        ((comparisonID, weight), blockIDs)
                    }
            case Constants.PEARSON_X2 =>
                blocks
                    .map(b => (b.id, b.getComparisonsPairs))
                    .flatMap(b => b._2.map(pair => (pair, ArrayBuffer(b._1))))
                    .reduceByKey(_ ++ _)
                    .map{ c =>
                        val (sourceID, targetID) = c._1
                        val blockIDs = c._2
                        val comparisonID = Utils.bijectivePairing(sourceID, targetID)
                        val e1Blocks = entitiesBlockMapBD.value(sourceID)
                        val e2Blocks = entitiesBlockMapBD.value(targetID)
                        val commonBlocks = e1Blocks.intersect(e2Blocks)

                        val v1: Array[Long] = Array[Long](commonBlocks.size, e2Blocks.size - commonBlocks.size)
                        val v2: Array[Long] = Array[Long](e1Blocks.size - commonBlocks.size, totalBlocksBD.value - (v1(0) + v1(1) +(e1Blocks.size - commonBlocks.size)) )

                        val chiTest = new ChiSquareTest()
                        val weight = chiTest.chiSquare(Array(v1, v2))
                        ((comparisonID, weight), blockIDs)
                    }
            case Constants.CBS| _ =>
                blocks
                    .map(b => (b.id, b.getComparisonsPairs))
                    .flatMap(b => b._2.map(pair => (pair, ArrayBuffer(b._1))))
                    .reduceByKey(_ ++ _)
                    .map{ c =>
                        val (sourceID, targetID) = c._1
                        val blockIDs = c._2
                        val comparisonID = Utils.bijectivePairing(sourceID, targetID)
                        val e1Blocks = entitiesBlockMapBD.value(sourceID)
                        val e2Blocks = entitiesBlockMapBD.value(targetID)
                        val commonBlocks = e1Blocks.intersect(e2Blocks)
                        ((comparisonID, commonBlocks.size.toDouble), blockIDs)
                    }
        }
    }

    def iterativeExecution(blocks: RDD[Block], relation: String, cleaningStrategy: String): RDD[(Int,Int)] ={ null}
}
