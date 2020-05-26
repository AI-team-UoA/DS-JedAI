package EntityMatching.PartitionMatching

import DataStructures.{MBB, SpatialEntity}
import EntityMatching.MatchingTrait
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import utils.Constants
import utils.Readers.SpatialReader

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class PartitionMatching(joinedRDD: RDD[(Int, (List[SpatialEntity], List[SpatialEntity]))], weightingScheme: String,
                             thetaXY: (Double, Double)) extends MatchingTrait{

    var partitionsZones: Array[MBB]  = SpatialReader.partitionsZones

    def setPartitionsZones(boundaries: Array[MBB]): Unit = partitionsZones = boundaries

    def zoneCheck(zone: MBB, coords: (Int, Int)): Boolean =  zone.minX <= coords._1 && zone.maxX >= coords._1 &&
        zone.minY <= coords._2 && zone.maxY >= coords._2


    def adjustPartitionsZones() : Unit ={
        val (thetaX, thetaY) = thetaXY

        partitionsZones = partitionsZones.map(mbb =>{
            val maxX = math.ceil(mbb.maxX / thetaX).toInt
            val minX = math.floor(mbb.minX / thetaX).toInt
            val maxY = math.ceil(mbb.maxY / thetaY).toInt
            val minY = math.floor(mbb.minY / thetaY).toInt

            MBB(maxX, minX, maxY, minY)
        })
    }



    def indexSpatialEntity(se: SpatialEntity, zone: MBB): Array[(Int, Int)] = {
        val (thetaX, thetaY) = thetaXY

        val maxX = math.ceil(se.mbb.maxX / thetaX).toInt
        val minX = math.floor(se.mbb.minX / thetaX).toInt
        val maxY = math.ceil(se.mbb.maxY / thetaY).toInt
        val minY = math.floor(se.mbb.minY / thetaY).toInt

        (for (x <- minX to maxX; y <- minY to maxY; if zoneCheck(zone, (x, y))) yield (x, y)).toArray
    }


    def index(entities: List[SpatialEntity], zone: MBB): mutable.HashMap[Int, mutable.HashMap[Int, mutable.ListBuffer[Int]]]
    = {
        val sourceIndex: mutable.HashMap[Int, mutable.HashMap[Int, mutable.ListBuffer[Int]]] = mutable.HashMap()

        entities
            .zipWithIndex
            .foreach { case (se, index) =>
                val indices: Array[(Int, Int)] = indexSpatialEntity(se, zone)
                indices
                    .foreach { i =>
                        if (sourceIndex.contains(i._1))
                            if (sourceIndex(i._1).contains(i._2))
                                sourceIndex(i._1)(i._2) += index
                            else
                                sourceIndex(i._1).put(i._2, ListBuffer(index))
                        else {
                            val l = ListBuffer[Int](index)
                            val h = mutable.HashMap[Int, ListBuffer[Int]]()
                            h.put(i._2, l)
                            sourceIndex.put(i._1, h)
                        }
                    }
            }
        sourceIndex
    }



    def apply(relation: String): RDD[(String, String)] ={
        adjustPartitionsZones()
        joinedRDD.flatMap { p =>
            val partitionId = p._1
            val source = p._2._1
            val target = p._2._2
            val zone =  partitionsZones(partitionId)
            val sourceIndex = index(source, zone)

            target
                .map(se => (indexSpatialEntity(se, zone), se))
                .flatMap { case (coordsAr: Array[(Int, Int)], se: SpatialEntity) =>
                    coordsAr
                        .filter(c => sourceIndex.contains(c._1))
                        .filter(c => sourceIndex(c._1).contains(c._2))
                        .flatMap(c => sourceIndex(c._1)(c._2).map(j => (source(j), se, c)))
                }
                .filter { case (e1: SpatialEntity, e2: SpatialEntity, b: (Int, Int)) =>
                    e1.mbb.testMBB(e2.mbb, relation) && e1.mbb.referencePointFiltering(e2.mbb, b)
                }
                .filter(c => relate(c._1.geometry, c._2.geometry, relation))
                .map(c => (c._1.originalID, c._2.originalID))
        }
    }




}

object PartitionMatching{

    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], weightingScheme: String,  thetaMsrSTR: String = Constants.NO_USE): PartitionMatching ={
       val thetaXY = initTheta(source, target, thetaMsrSTR)
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), List(se))).reduceByKey(_ ++ _)
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), List(se))).reduceByKey(_ ++ _)

        val joinedRDD = sourcePartitions.join(targetPartitions, SpatialReader.spatialPartitioner)
        PartitionMatching(joinedRDD, weightingScheme, thetaXY)
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
