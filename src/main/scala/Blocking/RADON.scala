package Blocking

import DataStructures.GeoProfile
import org.apache.spark.rdd.RDD

object RADON {

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
}
