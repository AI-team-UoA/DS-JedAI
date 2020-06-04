package Blocking

import DataStructures.{Block, LightBlock, SpatialEntity}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
trait 	Blocking {
	val source: RDD[SpatialEntity]
	val target: RDD[SpatialEntity]
	val thetaXY: (Double, Double)

	var broadcastMap: Map[String, Broadcast[Any]] = Map()


	/**
	 * indexing a spatial entity into blocks using the RADON blocking procedure
	 *
	 * @param se a spatialEntity to index
	 * @param acceptedBlocks the accepted blocks that the set can be indexed to
	 * @return an array of blocks
	 */
	def indexSpatialEntity(se: SpatialEntity, acceptedBlocks: Set[(Int, Int)] = Set()): IndexedSeq[(Int, Int)] = {

		val (thetaX, thetaY) = thetaXY
		if (se.crossesMeridian) {
			val (westernMBB, easternMBB) = se.mbb.splitOnMeridian

			val wmbb_maxX = math.ceil(westernMBB.maxX / thetaX).toInt
			val wmbb_minX = math.floor(westernMBB.minX / thetaX).toInt
			val wmbb_maxY = math.ceil(westernMBB.maxY / thetaY).toInt
			val wmbb_minY = math.floor(westernMBB.minY / thetaY).toInt

			val embb_maxX = math.ceil(easternMBB.maxX / thetaX).toInt
			val embb_minX = math.floor(easternMBB.minX / thetaX).toInt
			val embb_maxY = math.ceil(easternMBB.maxY / thetaY).toInt
			val embb_minY = math.floor(easternMBB.minY / thetaY).toInt

			if (acceptedBlocks.nonEmpty) {
				val western = for (x <- wmbb_minX to wmbb_maxX; y <- wmbb_minY to wmbb_maxY; if acceptedBlocks.contains((x, y))) yield (x, y)
				val eastern = for (x <- embb_minX to embb_maxX; y <- embb_minY to embb_maxY; if acceptedBlocks.contains((x, y))) yield (x, y)
				eastern ++ western
			}
			else {
				val western = for (x <- wmbb_minX to wmbb_maxX; y <- wmbb_minY to wmbb_maxY) yield (x, y)
				val eastern = for (x <- embb_minX to embb_maxX; y <- embb_minY to embb_maxY) yield (x, y)
				eastern ++ western
			}
		}
		else {
			val maxX = math.ceil(se.mbb.maxX / thetaX).toInt
			val minX = math.floor(se.mbb.minX / thetaX).toInt
			val maxY = math.ceil(se.mbb.maxY / thetaY).toInt
			val minY = math.floor(se.mbb.minY / thetaY).toInt

			if (acceptedBlocks.nonEmpty)
				for (x <- minX to maxX; y <- minY to maxY; if acceptedBlocks.contains((x, y))) yield (x, y)
			else
				for (x <- minX to maxX; y <- minY to maxY) yield (x, y)
		}
	}

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
	 * @return an RDD of Blocks
	 */
	def apply(): RDD[Block] ={
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

	/**
	 * the ids of block is defined by their index after sorting them
	 * based on the no comparisons
	 * @return an RDD of Blocks
	 */
	def applyWithSort(): RDD[Block] ={
		val sourceIndex = index(source)
		val sourceBlocks: Set[(Int, Int)] = sourceIndex.map(b => Set(b._1)).reduce(_++_)
		val targetIndex = index(target, sourceBlocks)

		val blocksIndex: RDD[((Int, Int), (ArrayBuffer[SpatialEntity], Option[ArrayBuffer[SpatialEntity]]))] = targetIndex.leftOuterJoin(sourceIndex)

		// construct blocks from indexes
		blocksIndex
			.filter(_._2._2.isDefined)
			.map { block =>
				val blockCoords = block._1
				val sourceIndex = block._2._1
				val targetIndex = block._2._2.get
				(sourceIndex.size * targetIndex.size, (blockCoords, sourceIndex, targetIndex))
			}
    		.sortByKey()
			.zipWithIndex()
			.map{ case (b: (Int, ((Int, Int), ArrayBuffer[SpatialEntity], ArrayBuffer[SpatialEntity])), id: Long) =>
				val blockCoords = b._2._1
				val targetIndex = b._2._2
				val sourceIndex = b._2._3
				Block(id, blockCoords, sourceIndex, targetIndex)
			}
	}

	def apply(liTarget: Boolean = true): RDD[LightBlock] = {null}

}
