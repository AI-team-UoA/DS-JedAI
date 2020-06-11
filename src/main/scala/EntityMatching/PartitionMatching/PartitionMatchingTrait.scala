package EntityMatching.PartitionMatching

import DataStructures.{MBB, SpatialEntity, SpatialIndex}
import EntityMatching.MatchingTrait
import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.apache.spark.rdd.RDD
import utils.Constants
import utils.Readers.SpatialReader

trait PartitionMatchingTrait extends MatchingTrait {

    val orderByWeight: Ordering[(Double, (SpatialEntity, SpatialEntity))] = Ordering.by[(Double, (SpatialEntity, SpatialEntity)), Double](_._1).reverse

    val joinedRDD: RDD[(Int, (Iterable[SpatialEntity], Iterable[SpatialEntity]))]
    val thetaXY: (Double, Double)
    val weightingScheme: String
    var totalBlocks: Double = 0
    var partitionsZones: Array[MBB] = SpatialReader.partitionsZones
    val spaceEdges: MBB = {
        val minX = SpatialReader.partitionsZones.map(p => p.minX).min
        val maxX = SpatialReader.partitionsZones.map(p => p.maxX).max
        val minY = SpatialReader.partitionsZones.map(p => p.minY).min
        val maxY = SpatialReader.partitionsZones.map(p => p.maxY).max
        MBB(maxX, minX, maxY, minY)
    }

    /**
     * set partition zones
     *
     * @param boundaries array of mbb that defines the partitions
     */
    def setPartitionsZones(boundaries: Array[MBB]): Unit = partitionsZones = boundaries

    /**
     * check if the coords is inside the zone of the partition
     * // TODO fix the description
     *
     * @param pid    partition's id to get partition's zone
     * @param coords coordinates of block
     * @return true if the coords are inside the zone
     */
    def zoneCheck(pid: Int, coords: (Int, Int)): Boolean = {

        // the block is inside its partition
        if (partitionsZones(pid).minX < coords._1 && partitionsZones(pid).maxX > coords._1 && partitionsZones(pid).minY < coords._2 && partitionsZones(pid).maxY > coords._2)
            true
        // the block is on the edges of the partitions
        else {
            // we are in the top-right corner - no other partition can possible claiming it
            if (spaceEdges.maxX == coords._1 && spaceEdges.maxY == coords._2)
                true
            // we are in the right edge of the whole space
            else if (spaceEdges.maxX == coords._1)
                partitionsZones(pid).minY < coords._2 + 0.5 && partitionsZones(pid).maxY > coords._2 + 0.5
            // we are in the top edge of the whole space
            else if (spaceEdges.maxY == coords._2)
                partitionsZones(pid).minX < coords._1 + 0.5 && partitionsZones(pid).maxX > coords._1 + 0.5
            // the partition does not touches the edges of space - so we just see if the examined block is in the partition
            else {
                (partitionsZones(pid).minX < coords._1 + 0.5 && partitionsZones(pid).maxX > coords._1 + 0.5) &&
                    (partitionsZones(pid).minY < coords._2 + 0.5 && partitionsZones(pid).maxY > coords._2 + 0.5)
            }
        }
    }

    /**
     * adjust the coordinates of the partition based on theta
     * // TODO: adjust space edges
     */
    def adjustPartitionsZones(): Unit = {
        val (thetaX, thetaY) = thetaXY

        partitionsZones = partitionsZones.map(mbb => {
            val maxX = math.ceil(mbb.maxX / thetaX).toInt
            val minX = math.floor(mbb.minX / thetaX).toInt
            val maxY = math.ceil(mbb.maxY / thetaY).toInt
            val minY = math.floor(mbb.minY / thetaY).toInt

            MBB(maxX, minX, maxY, minY)
        })
    }

    /**
     * adjust partition and find the max no total blocks
     */
    def init(): Unit = {
        adjustPartitionsZones()
        val globalMinX = joinedRDD.flatMap(p => p._2._1.map(_.mbb.minX)).min()
        val globalMaxX = joinedRDD.flatMap(p => p._2._1.map(_.mbb.maxX)).max()
        val globalMinY = joinedRDD.flatMap(p => p._2._1.map(_.mbb.minY)).min()
        val globalMaxY = joinedRDD.flatMap(p => p._2._1.map(_.mbb.maxY)).max()
        totalBlocks = (globalMaxX - globalMinX + 1) * (globalMaxY - globalMinY + 1)
    }

    /**
     * index a spatial entity. Only indexes inside the partition zones will be returned
     *
     * @param se  spatial entity
     * @param pid partition's id to get partition's zone
     * @return array of coordinates
     */
    def indexSpatialEntity(se: SpatialEntity, pid: Int): Array[(Int, Int)] = {
        val (thetaX, thetaY) = thetaXY

        val maxX = math.ceil(se.mbb.maxX / thetaX).toInt
        val minX = math.floor(se.mbb.minX / thetaX).toInt
        val maxY = math.ceil(se.mbb.maxY / thetaY).toInt
        val minY = math.floor(se.mbb.minY / thetaY).toInt

        (for (x <- minX to maxX; y <- minY to maxY; if zoneCheck(pid, (x, y))) yield (x, y)).toArray
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
            val indices: Array[(Int, Int)] = indexSpatialEntity(se, pid)
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
        weightingScheme match {
            case Constants.ECBS =>
                val e1Blocks = (e1.mbb.maxX - e1.mbb.minX + 1) * (e1.mbb.maxY - e1.mbb.minY + 1)
                val e2Blocks = (e2.mbb.maxX - e2.mbb.minX + 1) * (e2.mbb.maxY - e2.mbb.minY + 1)
                frequency * math.log10(totalBlocks / e1Blocks) * math.log10(totalBlocks / e2Blocks)

            case Constants.JS =>
                val e1Blocks = (e1.mbb.maxX - e1.mbb.minX + 1) * (e1.mbb.maxY - e1.mbb.minY + 1)
                val e2Blocks = (e2.mbb.maxX - e2.mbb.minX + 1) * (e2.mbb.maxY - e2.mbb.minY + 1)
                frequency / (e1Blocks + e2Blocks - frequency)

            case Constants.PEARSON_X2 =>
                val e1Blocks = (e1.mbb.maxX - e1.mbb.minX + 1) * (e1.mbb.maxY - e1.mbb.minY + 1)
                val e2Blocks = (e2.mbb.maxX - e2.mbb.minX + 1) * (e2.mbb.maxY - e2.mbb.minY + 1)

                val v1: Array[Long] = Array[Long](frequency, (e2Blocks - frequency).toLong)
                val v2: Array[Long] = Array[Long]((e1Blocks - frequency).toLong, (totalBlocks - (v1(0) + v1(1) + (e1Blocks - frequency))).toLong)

                val chiTest = new ChiSquareTest()
                chiTest.chiSquare(Array(v1, v2))

            case Constants.CBS | _ =>
                frequency.toDouble
        }
    }

    def apply(relation: String): RDD[(String, String)]
}


