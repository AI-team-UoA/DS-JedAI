package Blocking

import DataStructures.{Block, Entity}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD


/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
trait Blocking {
	val source: RDD[Entity]
	val target: RDD[Entity]
	val thetaXY: (Double, Double)

	var broadcastMap: Map[String, Broadcast[Any]] = Map()


	def index(entitiesRDD: RDD[Entity], acceptedBlocks: Set[(Int, Int)] = Set()): RDD[((Int, Int), Array[Entity])]


	/**
	 * apply blocking
	 * @return an RDD of Blocks
	 */
	def apply(): RDD[Block] ={
		val sourceIndex = index(source)
		val sourceBlocks: Set[(Int, Int)] = sourceIndex.map(b => Set(b._1)).reduce(_++_)
		val targetIndex = index(target, sourceBlocks)

		val blocksIndex: RDD[((Int, Int), (Option[Array[Entity]], Array[Entity]))] =
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
