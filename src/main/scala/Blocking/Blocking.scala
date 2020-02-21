package Blocking

import DataStructures.{Block, SpatialEntity}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
trait Blocking {
	var source: RDD[SpatialEntity]
	var target: RDD[SpatialEntity]

	var broadcastMap: Map[String, Broadcast[Any]] = Map()


	/**
	 * indexing spatial entities into blocks
	 *
	 * @param spatialEntitiesRDD the set to index
	 * @param acceptedBlocks the accepted blocks that the set can be indexed to
	 * @return an Array of block ids for each spatial entity
	 */
	def index(spatialEntitiesRDD: RDD[SpatialEntity], acceptedBlocks: Set[(Int, Int)] = Set()): RDD[((Int, Int), ArrayBuffer[SpatialEntity])]

	/**
	 * apply blocking
	 * @return
	 */
	def apply(): RDD[Block] ={
		val sourceIndex = index(source)
		val sourceBlocks: Set[(Int, Int)] = sourceIndex.map(b => Set(b._1)).reduce(_++_)

		val targetIndex = index(target, sourceBlocks)

		val blocksIndex: RDD[((Int, Int), (ArrayBuffer[SpatialEntity], Option[ArrayBuffer[SpatialEntity]]))] = sourceIndex.leftOuterJoin(targetIndex)

		// construct blocks from indexes
		val blocksRDD = blocksIndex
			.filter(b => b._2._2.isDefined)
			.map { block =>
				val blockCoords = block._1
				val sourceIndexSet = block._2._1.toSet
				val targetIndexSet = block._2._2.get.toSet
				Block(blockCoords, sourceIndexSet, targetIndexSet)
			}
			.setName("BlocksRDD")
		blocksRDD
	}
}
