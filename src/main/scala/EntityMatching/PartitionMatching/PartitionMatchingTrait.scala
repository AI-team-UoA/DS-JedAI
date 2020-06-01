package EntityMatching.PartitionMatching

import DataStructures.{MBB, SpatialEntity, SpatialIndex}
import EntityMatching.MatchingTrait
import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.apache.spark.rdd.RDD
import utils.Constants
import utils.Readers.SpatialReader

trait PartitionMatchingTrait extends MatchingTrait {

    val joinedRDD: RDD[(Int, (Array[SpatialEntity], Array[SpatialEntity]))]
    val thetaXY: (Double, Double)
    val weightingScheme: String
    var partitionsZones: Array[MBB] = SpatialReader.partitionsZones
    var totalBlocks: Double = 0

    /**
     * set partition zones
     *
     * @param boundaries array of mbb that defines the partitions
     */
    def setPartitionsZones(boundaries: Array[MBB]): Unit = partitionsZones = boundaries

    /**
     * check if the coords is inside the zone of the partition
     *
     * @param pid    partition's id to get partition's zone
     * @param coords coordinates of block
     * @return true if the coords are inside the zone
     */
    def zoneCheck(pid: Int, coords: (Int, Int)): Boolean = partitionsZones(pid).minX <= coords._1 && partitionsZones(pid).maxX >= coords._1 &&
        partitionsZones(pid).minY <= coords._2 && partitionsZones(pid).maxY >= coords._2

    /**
     * adjust the coordinates of the partition based on theta
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


