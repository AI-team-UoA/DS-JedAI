package Algorithms

import DataStructures.{Block, SpatialEntity}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.Constant

class RADON	(var sourceRDD: RDD[SpatialEntity], var targetRDD: RDD[SpatialEntity], var relation: String, theta_msr: String, entitiesSeparator: Int = -1) extends  Serializable
{
	var blocksRDD: RDD[Block] = _
	var swapped = false
	var broadcastMap: Map[String, Broadcast[Any]] = Map()

	def initTheta(): Unit ={
		val thetaMsr: RDD[(Double, Double)] = sourceRDD
			.union(targetRDD)
			.map {
				sp =>
					val env = sp.geometry.getEnvelopeInternal
					(env.getHeight, env.getHeight)
			}
    		.setName("thetaMsr")
    		.cache()

		var thetaX = 1d
		var thetaY = 1d
		theta_msr match {
			// WARNING: small or big values of theta may affect negatively the indexing procedure
			case Constant.MIN =>
				// filtering because there are cases that the geometries are perpendicular to the axes
				// and have width or height equals to 0.0
				thetaX = thetaMsr.map(_._1).filter(_ != 0.0d).min
				thetaY = thetaMsr.map(_._2).filter(_ != 0.0d).min
			case Constant.MAX =>
				thetaX = thetaMsr.map(_._1).max
				thetaY = thetaMsr.map(_._2).max
			case Constant.AVG =>
				val length = thetaMsr.count
				thetaX = thetaMsr.map(_._1).sum() / length
				thetaY = thetaMsr.map(_._2).sum() / length
			case _ =>
		}
		val broadcastedTheta = SparkContext.getOrCreate().broadcast((thetaX, thetaY))
		broadcastMap += ("theta" -> broadcastedTheta.asInstanceOf[Broadcast[Any]])
		thetaMsr.unpersist()
	}


	def getETH(seRDD: RDD[SpatialEntity]): Double ={
		getETH(seRDD, seRDD.count())
	}

	def getETH(seRDD: RDD[SpatialEntity], count: Double): Double ={
		val denom = 1/count
		val coords_sum = seRDD
			.map(se => (se.mbb.maxX - se.mbb.minX, se.mbb.maxY - se.mbb.minY))
    		.fold((0, 0)) { case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2) }

		val eth = count * ( (denom * coords_sum._1) * (denom * coords_sum._2) )
		eth
	}


	def swappingStrategy(): Unit= {
		val sourceETH = getETH(sourceRDD)
		val targetETH = getETH(targetRDD)

		if (targetETH < sourceETH){
			swapped = true
			val temp = sourceRDD
			sourceRDD = targetRDD
			targetRDD = temp

			relation =
				relation match {
				case Constant.WITHIN => Constant.CONTAINS
				case Constant.CONTAINS => Constant.WITHIN
				case Constant.COVERS => Constant.COVEREDBY
				case Constant.COVEREDBY => Constant.COVERS;
				case _ => relation
			}
		}
	}

	def index(spatialEntitiesRDD: RDD[SpatialEntity], acceptedBlocks: Set[(Int, Int)] = Set()): RDD[((Int, Int), Array[Int])] ={
		val blocks = spatialEntitiesRDD
			.map {
			se =>
				val (thetaX, thetaY) = broadcastMap("theta").value.asInstanceOf[(Double, Double)]
				val seID = se.id
				var blockIDs: Array[(Int, Int)] = Array()

				// Split on Meridian and index on eastern and western mbb
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

					(wmbb_minX to wmbb_maxX).map(x => (wmbb_minY to wmbb_maxY).map(y => blockIDs :+= (x, y)))

					(embb_minX to embb_maxX).map(x => (embb_minY to embb_maxY).map(y => blockIDs :+= (x, y)))
				}
				else {
					val maxX = math.ceil(se.mbb.maxX / thetaX).toInt
					val minX = math.floor(se.mbb.minX / thetaX).toInt
					val maxY = math.ceil(se.mbb.maxY / thetaY).toInt
					val minY = math.floor(se.mbb.minY / thetaY).toInt

					(minX to maxX).map(x => (minY to maxY).map(y => blockIDs :+= (x, y)))
				}
				(blockIDs, seID)
			}

			if (acceptedBlocks.nonEmpty) {
				// filters out blocks that don't contain entities of the source
				val acceptedBlocksBD = SparkContext.getOrCreate().broadcast(acceptedBlocks)
				broadcastMap += ("acceptedBlocks" -> acceptedBlocksBD.asInstanceOf[Broadcast[Any]])
				blocks.flatMap(p => p._1.filter(acceptedBlocksBD.value.contains).map(blockID => (blockID, Array(p._2)))).reduceByKey(_ ++ _)
			}
			else blocks.flatMap(p => p._1.map(blockID => (blockID, Array(p._2)))).reduceByKey(_++_)
	}


	def sparseSpaceTiling(): RDD[Block] = {
		initTheta()
		swappingStrategy()

		val sourceIndex = index(sourceRDD)
		val sourceBlocks: Set[(Int, Int)] = sourceIndex.map(b => Set(b._1)).reduce(_++_)

		val targetIndex = index(targetRDD, sourceBlocks)

		val blocksIndex: RDD[((Int, Int), (Array[Int], Option[Array[Int]]))] = sourceIndex.leftOuterJoin(targetIndex)
		blocksRDD = blocksIndex
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
