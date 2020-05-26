package EntityMatching.PartitionMatching


import DataStructures.{MBB, SpatialEntity, SpatialIndex}
import EntityMatching.MatchingTrait
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import utils.Constants
import utils.Readers.SpatialReader
import org.apache.commons.math3.stat.inference.ChiSquareTest

case class ComparisonCentricPrioritization(joinedRDD: RDD[(Int, (List[SpatialEntity], List[SpatialEntity]))], weightingScheme: String, thetaXY: (Double, Double)) extends MatchingTrait{

    var totalBlocks: Double = _
    var partitionsZones: Array[MBB]  = SpatialReader.partitionsZones

    def setPartitionsZones(boundaries: Array[MBB]): Unit = partitionsZones = boundaries

    def zoneCheck(zone: MBB, coords: (Int, Int)): Boolean =  zone.minX <= coords._1 && zone.maxX >= coords._1 &&
        zone.minY <= coords._2 && zone.maxY >= coords._2


    def init() : Unit ={
        val (thetaX, thetaY) = thetaXY
        partitionsZones = partitionsZones.map(mbb =>{
            val maxX = math.ceil(mbb.maxX / thetaX).toInt
            val minX = math.floor(mbb.minX / thetaX).toInt
            val maxY = math.ceil(mbb.maxY / thetaY).toInt
            val minY = math.floor(mbb.minY / thetaY).toInt

            MBB(maxX, minX, maxY, minY)
        })
        val globalMinX = joinedRDD.flatMap(p => p._2._1.map(_.mbb.minX)).min()
        val globalMaxX = joinedRDD.flatMap(p => p._2._1.map(_.mbb.maxX)).max()
        val globalMinY = joinedRDD.flatMap(p => p._2._1.map(_.mbb.minY)).min()
        val globalMaxY = joinedRDD.flatMap(p => p._2._1.map(_.mbb.maxY)).max()
        totalBlocks = (globalMaxX - globalMinX + 1) * (globalMaxY - globalMinY + 1)
    }


    def indexSpatialEntity(se: SpatialEntity, zone: MBB): Array[(Int, Int)] = {
        val (thetaX, thetaY) = thetaXY

        val maxX = math.ceil(se.mbb.maxX / thetaX).toInt
        val minX = math.floor(se.mbb.minX / thetaX).toInt
        val maxY = math.ceil(se.mbb.maxY / thetaY).toInt
        val minY = math.floor(se.mbb.minY / thetaY).toInt

        (for (x <- minX to maxX; y <- minY to maxY; if zoneCheck(zone, (x, y))) yield (x, y)).toArray
    }


    def index(entities: List[SpatialEntity], zone: MBB): SpatialIndex = {
        val spatialIndex = new SpatialIndex()

        entities
            .zipWithIndex
            .foreach { case (se, index) =>
                val indices: Array[(Int, Int)] = indexSpatialEntity(se, zone)
                indices.foreach(i => spatialIndex.insert(i, index))
            }
        spatialIndex
    }

    def getWeight(frequency: Int, e1: SpatialEntity, e2: SpatialEntity): Double ={
        weightingScheme match {
            case Constants.ECBS =>
                val e1Blocks = (e1.mbb.maxX - e1.mbb.minX + 1) * (e1.mbb.maxY - e1.mbb.minY + 1)
                val e2Blocks = (e2.mbb.maxX - e2.mbb.minX + 1) * (e2.mbb.maxY - e2.mbb.minY + 1)
                frequency * math.log10( totalBlocks/ e1Blocks) * math.log10(totalBlocks / e2Blocks)

            case Constants.JS =>
                val e1Blocks = (e1.mbb.maxX - e1.mbb.minX + 1) * (e1.mbb.maxY - e1.mbb.minY + 1)
                val e2Blocks = (e2.mbb.maxX - e2.mbb.minX + 1) * (e2.mbb.maxY - e2.mbb.minY + 1)
                frequency / (e1Blocks + e2Blocks - frequency)

            case Constants.PEARSON_X2 =>
                val e1Blocks = (e1.mbb.maxX - e1.mbb.minX + 1) * (e1.mbb.maxY - e1.mbb.minY + 1)
                val e2Blocks = (e2.mbb.maxX - e2.mbb.minX + 1) * (e2.mbb.maxY - e2.mbb.minY + 1)

                val v1: Array[Long] = Array[Long](frequency, (e2Blocks - frequency).toLong)
                val v2: Array[Long] = Array[Long]((e1Blocks - frequency).toLong, (totalBlocks - (v1(0) + v1(1) +(e1Blocks - frequency))).toLong )

                val chiTest = new ChiSquareTest()
                chiTest.chiSquare(Array(v1, v2))

            case Constants.CBS| _ =>
                frequency.toDouble
        }
    }


    def apply(relation: String): RDD[(String, String)] ={
        init()
        joinedRDD.flatMap { p =>
            val partitionId = p._1
            val source = p._2._1
            val target = p._2._2
            val zone =  partitionsZones(partitionId)
            val sourceIndex = index(source, zone)

            target
                .map(e2 => (indexSpatialEntity(e2, zone), e2))
                .flatMap { case (coordsAr: Array[(Int, Int)], e2: SpatialEntity) =>
                    val sIndices = coordsAr
                        .filter(c => sourceIndex.contains(c))
                        .map(c => (c, sourceIndex.get(c)))
                    val frequency = Array.fill(source.size)(0)
                    sIndices.flatMap(_._2).foreach(i => frequency(i) += 1)

                    sIndices.flatMap{ case(c, indices) =>
                        indices.map(i => (source(i), frequency(i)))
                        .filter{ case(e1, f) =>  e1.mbb.testMBB(e2.mbb, relation) && e1.mbb.referencePointFiltering(e2.mbb, c)}
                        .map{ case(e1, f) => (getWeight(f, e1, e2), (e1, e2))}
                    }
                }
                .sortBy(_._1)(Ordering.Double.reverse)
                .map(_._2)
                .filter(c => relate(c._1.geometry, c._2.geometry, relation))
                .map(c => (c._1.originalID, c._2.originalID))
        }
    }




}

object ComparisonCentricPrioritization {

    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], weightingScheme: String,  thetaMsrSTR: String = Constants.NO_USE): ComparisonCentricPrioritization ={
        val thetaXY = initTheta(source, target, thetaMsrSTR)
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), List(se))).reduceByKey(_ ++ _)
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), List(se))).reduceByKey(_ ++ _)

        val joinedRDD = sourcePartitions.join(targetPartitions, SpatialReader.spatialPartitioner)
        ComparisonCentricPrioritization(joinedRDD, weightingScheme, thetaXY)
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
            // WARNING: small or big values of theta may affect negatively the indexing procedure
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
