package Blocking

import DataStructures.{Block, SpatialEntity}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * @author George MAndilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
trait Blocking {
	var source: RDD[SpatialEntity]
	var target: RDD[SpatialEntity]

	var broadcastMap: Map[String, Broadcast[Any]] = Map()

	def index(spatialEntitiesRDD: RDD[SpatialEntity], acceptedBlocks: Set[(Int, Int)] = Set()): RDD[((Int, Int), Array[Int])]

	def apply(): RDD[Block] ={
		val sourceIndex = index(source)
		val sourceBlocks: Set[(Int, Int)] = sourceIndex.map(b => Set(b._1)).reduce(_++_)

		val targetIndex = index(target, sourceBlocks)

		val blocksIndex: RDD[((Int, Int), (Array[Int], Option[Array[Int]]))] = sourceIndex.leftOuterJoin(targetIndex)
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
