package EntityMatching.PartitionMatching

import DataStructures.{MBB, SpatialEntity, SpatialIndex}
import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation
import utils.Constants.WeightStrategy
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils

trait PartitionMatchingTrait {

    val orderByWeight: Ordering[(Double, (SpatialEntity, SpatialEntity))] = Ordering.by[(Double, (SpatialEntity, SpatialEntity)), Double](_._1).reverse

    val joinedRDD: RDD[(Int, (Iterable[SpatialEntity], Iterable[SpatialEntity]))]
    val thetaXY: (Double, Double)
    val ws: WeightStrategy

    val partitionsZones: Array[MBB] = Utils.getZones
    val spaceEdges: MBB = Utils.getSpaceEdges

    val totalBlocks: Double = {
        val globalMinX = joinedRDD.flatMap(p => p._2._1.map(_.mbb.minX)).min()
        val globalMaxX = joinedRDD.flatMap(p => p._2._1.map(_.mbb.maxX)).max()
        val globalMinY = joinedRDD.flatMap(p => p._2._1.map(_.mbb.minY)).min()
        val globalMaxY = joinedRDD.flatMap(p => p._2._1.map(_.mbb.maxY)).max()
        (globalMaxX - globalMinX + 1) * (globalMaxY - globalMinY + 1)
    }

    /**
     * Check if the block is inside the zone of the partition.
     * If the block is on the edges of the partition (hence can belong to many partitions),
     * then it is assigned to the upper-right partition. Also the case of the edges of the
     * space are considered.
     *
     * @param pid    partition's id to get partition's zone
     * @param b coordinates of block
     * @return true if the coords are inside the zone
     */
    def zoneCheck(pid: Int)(b: (Int, Int)): Boolean = {

        // the block is inside its partition
        if (partitionsZones(pid).minX < b._1 && partitionsZones(pid).maxX > b._1 && partitionsZones(pid).minY < b._2 && partitionsZones(pid).maxY > b._2)
            true
        // the block is on the edges of the partitions
        else {
            // we are in the top-right corner - no other partition can possible claiming it
            if (spaceEdges.maxX == b._1 && spaceEdges.maxY == b._2)
                true
            // we are in the right edge of the whole space
            else if (spaceEdges.maxX == b._1)
                partitionsZones(pid).minY < b._2 + 0.5 && partitionsZones(pid).maxY > b._2 + 0.5
            // we are in the top edge of the whole space
            else if (spaceEdges.maxY == b._2)
                partitionsZones(pid).minX < b._1 + 0.5 && partitionsZones(pid).maxX > b._1 + 0.5
            // the partition does not touches the edges of space - so we just see if the examined block is in the partition
            else {
                (partitionsZones(pid).minX < b._1 + 0.5 && partitionsZones(pid).maxX > b._1 + 0.5) &&
                    (partitionsZones(pid).minY < b._2 + 0.5 && partitionsZones(pid).maxY > b._2 + 0.5)
            }
        }
    }

    /**
     * index a list of spatial entities
     *
     * @param entities list of spatial entities
     * @param pid      partition's id to get partition's zone
     * @return a SpatialIndex
     */
    def index(entities: Array[SpatialEntity], pid: Int): SpatialIndex = {
        val spatialIndex = new SpatialIndex()
        entities.zipWithIndex.foreach { case (se, index) =>
            val indices: Array[(Int, Int)] = se.index(thetaXY, zoneCheck(pid))
            indices.foreach(i => spatialIndex.insert(i, index))
        }
        spatialIndex
    }

    /**
     * Weight a comparison
     *
     * @param frequency total comparisons of e1 with e2
     * @param e1        Spatial entity
     * @param e2        Spatial entity
     * @return weight
     */
    def getWeight(frequency: Int, e1: SpatialEntity, e2: SpatialEntity): Double = {
        ws match {
            case WeightStrategy.ECBS =>
                val e1Blocks = (e1.mbb.maxX - e1.mbb.minX + 1) * (e1.mbb.maxY - e1.mbb.minY + 1)
                val e2Blocks = (e2.mbb.maxX - e2.mbb.minX + 1) * (e2.mbb.maxY - e2.mbb.minY + 1)
                frequency * math.log10(totalBlocks / e1Blocks) * math.log10(totalBlocks / e2Blocks)

            case WeightStrategy.JS =>
                val e1Blocks = (e1.mbb.maxX - e1.mbb.minX + 1) * (e1.mbb.maxY - e1.mbb.minY + 1)
                val e2Blocks = (e2.mbb.maxX - e2.mbb.minX + 1) * (e2.mbb.maxY - e2.mbb.minY + 1)
                frequency / (e1Blocks + e2Blocks - frequency)

            case WeightStrategy.PEARSON_X2 =>
                val e1Blocks = (e1.mbb.maxX - e1.mbb.minX + 1) * (e1.mbb.maxY - e1.mbb.minY + 1)
                val e2Blocks = (e2.mbb.maxX - e2.mbb.minX + 1) * (e2.mbb.maxY - e2.mbb.minY + 1)

                val v1: Array[Long] = Array[Long](frequency, (e2Blocks - frequency).toLong)
                val v2: Array[Long] = Array[Long]((e1Blocks - frequency).toLong, (totalBlocks - (v1(0) + v1(1) + (e1Blocks - frequency))).toLong)

                val chiTest = new ChiSquareTest()
                chiTest.chiSquare(Array(v1, v2))

            case WeightStrategy.CBS | _ =>
                frequency.toDouble
        }
    }

    def apply(relation: Relation): RDD[(String, String)]

    def applyWithBudget(relation: Relation, budget: Int): Long = {
        val comparisons = apply(relation)
        comparisons.take(budget).length
    }
}


