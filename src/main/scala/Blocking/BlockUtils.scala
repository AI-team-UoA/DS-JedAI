package Blocking

import DataStructures.TBlock
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.{Constants, Utils}

import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer


/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
object BlockUtils {


	def clean(blocksPerComparison: RDD[(Any, ArrayBuffer[Long])], cleanStrategy: String = Constants.RANDOM): RDD[(Long, ArrayBuffer[Any])] = {
		cleanStrategy match {
			case Constants.RANDOM =>
				blocksPerComparison
					.mapPartitions { cbIter =>
						val randomGen = new scala.util.Random
						cbIter.map (cb => (cb._2(randomGen.nextInt(cb._2.length)), ArrayBuffer(cb._1)))
					}
					.reduceByKey(_ ++ _)
			case  Constants.MIN|_ =>
				blocksPerComparison
					.map(cb => (cb._2.min, ArrayBuffer(cb._1)))
					.reduceByKey(_ ++ _)
		}
	}


	/**
	 * Remove duplicate comparisons from blocks by applying the cleaning strategy
	 *
	 * @param blocks an RDD of Blocks
	 * @param strategy decide to which block each comparison will be assigned to
	 * @return an RDD of Comparisons
	 */
	def cleanBlocks(blocks: RDD[TBlock], strategy: String = Constants.RANDOM): RDD[(Long, HashSet[Long])] ={
		val blocksPerComparison = blocks
				.map(b => (b.id, b.getComparisonsIDs))
				.flatMap(b => b._2.map(c => (c, ArrayBuffer(b._1))))
				.reduceByKey(_ ++ _)

		val comparisonsPerBlock = clean(blocksPerComparison.asInstanceOf[RDD[(Any, ArrayBuffer[Long])]], strategy).asInstanceOf[RDD[(Long, ArrayBuffer[Long])]]
		comparisonsPerBlock.filter(_._2.nonEmpty).map(b => (b._1, b._2.to[HashSet]))
	}

}
