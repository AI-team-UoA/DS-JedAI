package EntityMatching.PartitionMatching

import DataStructures.{MBB, SpatialEntity, SpatialIndex}
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import utils.{Constants, Utils}
import utils.Readers.SpatialReader

case class PartitionMatching(joinedRDD: RDD[(Int, (Array[SpatialEntity],  Array[SpatialEntity]))],
                             thetaXY: (Double, Double), weightingScheme: String) extends PartitionMatchingTrait {
//case class PartitionMatching(source: RDD[SpatialEntity], target: RDD[SpatialEntity], thetaXY: (Double, Double), weightingScheme: String) {
//    var partitionsZones: Array[MBB] = SpatialReader.partitionsZones

    /**
     * First index the source and then use the index to find the comparisons with target's entities.
     *
     * @param relation the examining relation
     * @return an RDD containing the matching pairs
     */
    def apply(relation: String): RDD[(String, String)] ={
        adjustPartitionsZones()
        joinedRDD.flatMap { p =>
            val partitionId = p._1
            val source = p._2._1
            val target = p._2._2
            val sourceIndex = index(source, partitionId)

            target
                .map(se => (indexSpatialEntity(se, partitionId), se))
                .flatMap { case (coordsAr: Array[(Int, Int)], se: SpatialEntity) =>
                    coordsAr
                        .filter(c => sourceIndex.contains(c))
                        .flatMap(c => sourceIndex.get(c).map(j => (source(j), se, c)))
                }
                .filter { case (e1: SpatialEntity, e2: SpatialEntity, b: (Int, Int)) =>
                    e1.mbb.testMBB(e2.mbb, relation) && e1.mbb.referencePointFiltering(e2.mbb, b, thetaXY)
                }
                .filter(c => relate(c._1.geometry, c._2.geometry, relation))
                .map(c => (c._1.originalID, c._2.originalID))
        }
    }

    /*def apply(relation: String): RDD[(String, String)] ={
       adjustPartitionsZones()
       val sourceRDD = source.mapPartitions {
           sIter =>
               val sourceAr = sIter.toArray
               val pid = TaskContext.getPartitionId()
               val sourceIndex = index(sourceAr, pid)
               Iterator((pid, (sourceIndex, sourceAr)))
           }
           .filter(_._2._2.nonEmpty)
           .setName("SourceIndex").persist(StorageLevel.MEMORY_AND_DISK)

       val sIndexRDD = sourceRDD.map(s => (s._1, s._2._1.asKeys))
       val targetRDD = target.mapPartitions {
           tIter =>
               val targetAr = tIter.toArray
               val pid = TaskContext.getPartitionId()
               Iterator((pid, targetAr))
            }
           .filter(_._2.nonEmpty)
           .leftOuterJoin(sIndexRDD)
           .filter(_._2._2.isDefined)
           .map {
               t =>
                   val pid = t._1
                   val targetAr = t._2._1
                   val sourceIndex = t._2._2.get
                   val filteredEntities =
                       targetAr
                           .map(se => (indexSpatialEntity(se, pid), se))
                           .filter(t => t._1.exists(c => sourceIndex.contains(c._1) && sourceIndex(c._1).contains(c._2)))
                           .map(_._2)
                           .toIterator
                   (pid, filteredEntities)
           }

       sourceRDD.leftOuterJoin(targetRDD, SpatialReader.spatialPartitioner)
           .filter(_._2._2.isDefined)
           .flatMap{
               case( pid, joined) =>
                   val sourceAr = joined._1._2
                   val targetIter = joined._2.get
                   val sourceIndex = joined._1._1
                   targetIter
                       .map(se => (indexSpatialEntity(se, pid), se))
                       .flatMap { case (coordsAr: Array[(Int, Int)], se: SpatialEntity) =>
                           coordsAr
                               .filter(c => sourceIndex.contains(c))
                               .flatMap(c => sourceIndex.get(c).map(j => (sourceAr(j), se, c)))
                       }
                       .filter { case (e1: SpatialEntity, e2: SpatialEntity, b: (Int, Int)) =>
                           e1.mbb.testMBB(e2.mbb, relation) && e1.mbb.referencePointFiltering(e2.mbb, b, thetaXY)
                       }
                       .filter(c => relate(c._1.geometry, c._2.geometry, relation))
                       .map(c => (c._1.originalID, c._2.originalID))
           }
   }*/

/*

    // -----
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

    def index(entities: Array[SpatialEntity], pid: Int): SpatialIndex = {
        val spatialIndex = new SpatialIndex()
        entities.zipWithIndex.foreach { case (se, index) =>
            val indices: Array[(Int, Int)] = indexSpatialEntity(se, pid)
            indices.foreach(i => spatialIndex.insert(i, index))
        }
        spatialIndex
    }

    def indexSpatialEntity(se: SpatialEntity, pid: Int): Array[(Int, Int)] = {
        val (thetaX, thetaY) = thetaXY

        val maxX = math.ceil(se.mbb.maxX / thetaX).toInt
        val minX = math.floor(se.mbb.minX / thetaX).toInt
        val maxY = math.ceil(se.mbb.maxY / thetaY).toInt
        val minY = math.floor(se.mbb.minY / thetaY).toInt

        (for (x <- minX to maxX; y <- minY to maxY; if zoneCheck(pid, (x, y))) yield (x, y)).toArray
    }

    def zoneCheck(pid: Int, coords: (Int, Int)): Boolean = partitionsZones(pid).minX <= coords._1 && partitionsZones(pid).maxX >= coords._1 &&
        partitionsZones(pid).minY <= coords._2 && partitionsZones(pid).maxY >= coords._2

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
    }*/
}

/**
 * auxiliary constructor
 */
object PartitionMatching{

    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaMsrSTR: String, weightingScheme: String = Constants.NO_USE): PartitionMatching ={
       val thetaXY = Utils.initTheta(source, target, thetaMsrSTR)
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), Array(se))).reduceByKey(SpatialReader.spatialPartitioner, _ ++ _)
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), Array(se))).reduceByKey(SpatialReader.spatialPartitioner, _ ++ _)

        val joinedRDD = sourcePartitions.join(targetPartitions, SpatialReader.spatialPartitioner)
        //joinedRDD.setName("JoinedRDD").persist(StorageLevel.MEMORY_AND_DISK)
        PartitionMatching(joinedRDD, thetaXY, weightingScheme)
    }


   /* def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaMsrSTR: String, weightingScheme: String = Constants.NO_USE): PartitionMatching ={
        val thetaXY = Utils.initTheta(source, target, thetaMsrSTR)

        PartitionMatching(source, target, thetaXY, weightingScheme)
    }*/



}
