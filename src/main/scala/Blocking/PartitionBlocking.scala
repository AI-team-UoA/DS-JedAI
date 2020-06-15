package Blocking

import DataStructures.{Block, MBB, SpatialEntity}
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.Constants.ThetaOption.ThetaOption
import utils.Utils

import scala.collection.mutable.ArrayBuffer

case class PartitionBlocking(source: RDD[SpatialEntity], target: RDD[SpatialEntity], thetaXY: (Double,Double))
    extends  Blocking with Serializable {

    val partitionsZones: Array[MBB] = Utils.getZones
    val spaceEdges: MBB = Utils.getSpaceEdges

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
    def zoneCheck(pid: Int, b: (Int, Int)): Boolean = {

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
            val pid = TaskContext.getPartitionId()
            val acceptedBlocks = acceptedBlocksBD.value
            val blocks: Iterator[(Array[(Int, Int)], SpatialEntity)] =
                if (acceptedBlocks.nonEmpty)
                    seIter.map(se => (se.index(thetaXY).filter(acceptedBlocks.contains), se)) // todo check iterator!
                else
                    seIter.map(se => (se.index(thetaXY), se))

            blocks
                .toArray
                .map(b => (b._1.filter(zoneCheck(pid, _)), b._2))
                .flatMap(b => b._1.map(id => (id, Array[SpatialEntity](b._2))))
                .groupBy(_._1)
                .map(p => (p._1, p._2.flatMap(ar => ar._2).to[ArrayBuffer]))
                .toIterator
        }
    }

    /**
     * Apply indexing
     * @return RDD of blocks
     */
    override def apply(): RDD[Block] = {

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
    def apply(source: RDD[SpatialEntity], target: RDD[SpatialEntity], thetaOption: ThetaOption): PartitionBlocking={
        val thetaXY = Utils.initTheta(source, target, thetaOption)
        PartitionBlocking(source, target, thetaXY)
    }
}

