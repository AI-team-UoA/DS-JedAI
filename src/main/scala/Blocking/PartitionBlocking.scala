package Blocking

import DataStructures.{Block, MBB, SpatialEntity}
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.Utils

import scala.collection.mutable.ArrayBuffer

case class PartitionBlocking(source: RDD[SpatialEntity], target: RDD[SpatialEntity], thetaXY: (Double,Double))
    extends  Blocking with Serializable {

    var partitionsZones: Array[MBB] = Array()
    def setPartitionsZones(boundaries: Array[MBB]): Unit = partitionsZones = boundaries

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



    /**
     * index a spatial entities set. If acceptedBlocks is provided then the entities will be assigned
     * only to blocks that exist in the accepted blocks set
     *
     * @param spatialEntitiesRDD the set to index
     * @param acceptedBlocks the accepted blocks that the set can be indexed to
     * @return an Array of block ids for each spatial entity
     */
    def index(spatialEntitiesRDD: RDD[SpatialEntity], acceptedBlocks: Set[(Int, Int)] = Set()): RDD[((Int, Int), ArrayBuffer[SpatialEntity])] ={
        val acceptedBlocksBD = SparkContext.getOrCreate().broadcast(acceptedBlocks)
        broadcastMap += ("acceptedBlocks" -> acceptedBlocksBD.asInstanceOf[Broadcast[Any]])
        spatialEntitiesRDD.mapPartitions { seIter =>
            val partitionZone = partitionsZones(TaskContext.getPartitionId())
            val acceptedBlocks = acceptedBlocksBD.value
            val p = seIter.toArray.map(se => (indexSpatialEntity(se, acceptedBlocks), se)) // WARNING: wrong no matches
                .map(b => (b._1.filter( coords => partitionZone.minX <= coords._1 && partitionZone.maxX >= coords._1 &&
                                                  partitionZone.minY <= coords._2 && partitionZone.maxY >= coords._2 ), b._2))
                .flatMap(b => b._1.map(id => (id, ArrayBuffer[SpatialEntity](b._2))))
                .groupBy( _._1)
                .map(p => (p._1, p._2.flatMap(ar => ar._2).to[ArrayBuffer]))
            p.toIterator
        }
    }

    /**
     * Apply indexing
     * @return RDD of blocks
     */
    override def apply(): RDD[Block] = {
        adjustPartitionsZones()

        val sourceIndex = index(source)
        val sourceBlocks: Set[(Int, Int)] = sourceIndex.map(b => Set(b._1)).reduce(_++_)
        val targetIndex = index(target, sourceBlocks)

        val blocksIndex: RDD[((Int, Int), (Option[ArrayBuffer[SpatialEntity]], ArrayBuffer[SpatialEntity]))] =
            targetIndex.rightOuterJoin(sourceIndex) // right outer join, in order to shuffle the small dataset
        // construct blocks from indexes
        blocksIndex
            .filter(_._2._1.isDefined)
            .map { block =>
                val blockCoords = block._1
                val sourceIndex = block._2._2
                val targetIndex = block._2._1.get
                Block(blockCoords, sourceIndex, targetIndex)
            }
    }


}

object PartitionBlocking {
    def apply(source: RDD[SpatialEntity], target: RDD[SpatialEntity], thetaMsrSTR: String): PartitionBlocking={
        val thetaXY = Utils.initTheta(source, target, thetaMsrSTR)
        PartitionBlocking(source, target, thetaXY)
    }
}

