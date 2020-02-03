package Blocking

import DataStructures.GeoProfile
import org.apache.spark.rdd.RDD
import utils.Constant

class RADON	(var sourceRDD: RDD[GeoProfile], var targetRDD: RDD[GeoProfile], var relation: String) {

	var swapped = false

	def getETH(gpRDD: RDD[GeoProfile]): Double ={
		getETH(gpRDD, gpRDD.count())
	}

	def getETH(gpRDD: RDD[GeoProfile], count: Double): Double ={
		val denom = 1/count
		val coords_sum = gpRDD
			.map(gp => (gp.maxX - gp.minX, gp.maxY - gp.minY))
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
				gp =>
					val gpID = gp.id
					var blockIDs: Array[(Int, Int)] = Array()
					val maxX = math.ceil(gp.maxX).toInt
					val minX = math.ceil(gp.minX).toInt
					val maxY = math.ceil(gp.maxY).toInt
					val minY = math.ceil(gp.minY).toInt


					// TODO split if it crossed meridian
					// MBBIndex westernPart = new MBBIndex(minLatIndex, (int) Math.floor(-180d * thetaX), maxLatIndex,	minLongIndex, g, p + "<}W", p);
					//MBBIndex easternPart = new MBBIndex(minLatIndex, maxLongIndex, maxLatIndex, (int) Math.ceil(180 * thetaX), g, p + "<}E", p);


					(minX to maxX).map(x => (minY to maxY).map(y => blockIDs :+= (x, y)))
					(blockIDs, gpID)
			}
    		.flatMap(p => p._1.map(blockID => (blockID, Array(p._2))))
			.reduceByKey(_++_)
		print()
		}
}
