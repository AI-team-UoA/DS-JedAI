package Algorithms

import DataStructures.{SpatialEntity, MBB}
import org.apache.spark.rdd.RDD
import utils.Constant

class RADON	(var sourceRDD: RDD[SpatialEntity], var targetRDD: RDD[SpatialEntity], var relation: String) {

	var swapped = false

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

	// TODO learn about the theta value of RADON
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


	def sparseSpaceTiling(): Unit = {
		swappingStrategy()

		val blocks = sourceRDD
			.map {
				se =>
					val seID = se.id
					var blockIDs: Array[(Int, Int)] = Array()

					// Split on Meridian and index on eastern and western mbb
					if (se.crossesMeridian) {
						val (westernMBB, easternMBB) = se.mbb.splitOnMeridian

						val wmbb_maxX = math.ceil(westernMBB.maxX).toInt
						val wmbb_minX = math.ceil(westernMBB.minX).toInt
						val wmbb_maxY = math.ceil(westernMBB.maxY).toInt
						val wmbb_minY = math.ceil(westernMBB.minY).toInt

						val embb_maxX = math.ceil(easternMBB.maxX).toInt
						val embb_minX = math.ceil(easternMBB.minX).toInt
						val embb_maxY = math.ceil(easternMBB.maxY).toInt
						val embb_minY = math.ceil(easternMBB.minY).toInt

						(wmbb_minX to wmbb_maxX).map(x => (wmbb_minY to wmbb_maxY).map(y => blockIDs :+= (x, y)))

						(embb_minX to embb_maxX).map(x => (embb_minY to embb_maxY).map(y => blockIDs :+= (x, y)))
					}
					else {
						val maxX = math.ceil(se.mbb.maxX).toInt
						val minX = math.ceil(se.mbb.minX).toInt
						val maxY = math.ceil(se.mbb.maxY).toInt
						val minY = math.ceil(se.mbb.minY).toInt

						(minX to maxX).map(x => (minY to maxY).map(y => blockIDs :+= (x, y)))
					}
					(blockIDs, seID)
			}
    		.flatMap(p => p._1.map(blockID => (blockID, Array(p._2))))
			.reduceByKey(_++_)
    		.count()
		}
}
