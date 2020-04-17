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
	private val sc = SparkContext.getOrCreate()
	var totalBlocks: Long = -1

	/**
	 * Set totalBlocks
	 * @param total_blocks no Blocks
	 */
	def setTotalBlocks(total_blocks: Long): Unit = totalBlocks = total_blocks

	def clean(blocksPerComparison: RDD[(Any, ArrayBuffer[Long])], cleanStrategy: String): RDD[(Long, ArrayBuffer[Any])] = {
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


	/**
	 * Weight the comparisons of the blocks and clean the duplicate comparisons
	 *
	 * @param blocks blocks RDD
	 * @param weightStrategy the weighting strategy
	 * @param cleanStrategy the cleaning strategy
	 * @return the weighted comparisons of each block
	 */
	def weightComparisons(blocks: RDD[TBlock], weightStrategy: String = Constants.CBS, cleanStrategy: String = Constants.RANDOM): RDD[(Long, ArrayBuffer[(Long, Double)])] ={
		val totalBlocksBD =
			if (weightStrategy == Constants.ECBS) {
				if (totalBlocks == -1) totalBlocks = blocks.count()
				sc.broadcast(totalBlocks)
			}
			else null

		val entitiesBlockMapBD =
			if (weightStrategy != Constants.ECBS)
				null
			else{
				val ce1:RDD[(Long, Long)] = blocks.flatMap(b => b.getSourceIDs)
				val ce2:RDD[(Long, Long)] = blocks.flatMap(b => b.getTargetIDs)
				val ce = ce1.union(ce2)
					.map(c => (c._1, ArrayBuffer(c._2)))
					.reduceByKey(_ ++ _)
					.sortByKey()
    				.map(c => (c._1, c._2.toSet))
					.collectAsMap()
				sc.broadcast(ce)
			}

		val blocksPerWeightedComparison: RDD[((Long, Double), ArrayBuffer[Long])] = weightStrategy match {
			case Constants.ARCS =>
				blocks
					.map(b => (b.id, b.getComparisonsIDs))
					.flatMap(b => b._2.map(c => (c, ArrayBuffer((b._1, b._2.size)))))
					.reduceByKey(_ ++ _)
					.map(c => ((c._1, c._2.map(b => 1.0/b._2).sum), c._2.map(_._1)))

			case Constants.ECBS =>
				blocks
					.map(b => (b.id, b.getComparisonsIDs))
					.flatMap(b => b._2.map(c => (c, ArrayBuffer(b._1 ))))
					.reduceByKey(_ ++ _)
					.map(b => (b._1, Utils.inversePairing(b._1), b._2))
					.map{
						b =>
							val blocks = b._3
							val comparisonID = b._1
							val (entity1, entity2) = b._2
							val blocksOfEntity1 = entitiesBlockMapBD.value(entity1.toInt)
							val blocksOfEntity2 = entitiesBlockMapBD.value(entity2.toInt)
							val commonBlocks = blocksOfEntity1.intersect(blocksOfEntity2)
							val weight = commonBlocks.size * math.log10(totalBlocksBD.value / blocksOfEntity1.size) * math.log10(totalBlocksBD.value / blocksOfEntity2.size)
							((comparisonID, weight), blocks)
					}
			case Constants.JS =>
				blocks
					.map(b => (b.id, b.getComparisonsIDs))
					.flatMap(b => b._2.map(c => (c, ArrayBuffer(b._1 ))))
					.reduceByKey(_ ++ _)
					.map(b => (b._1, Utils.inversePairing(b._1), b._2))
    				.map{
						b =>
							val blocks = b._3
							val comparisonID = b._1
							val (entity1, entity2) = b._2
							val blocksOfEntity1 = entitiesBlockMapBD.value(entity1.toInt)
							val blocksOfEntity2 = entitiesBlockMapBD.value(entity2.toInt)
							val totalCommonBlocks = blocksOfEntity1.intersect(blocksOfEntity2).size
							val denominator = blocksOfEntity1.size + blocksOfEntity2.size - totalCommonBlocks
							val weight = totalCommonBlocks / denominator
							((comparisonID, weight), blocks)
					}
			case Constants.CBS | _ =>
				blocks
					.map(b => (b.id, b.getComparisonsIDs))
					.flatMap(b => b._2.map(c => (c, ArrayBuffer(b._1))))
					.reduceByKey(_ ++ _)
					.map(c => ((c._1, c._2.length), c._2))
		}

		val weightedComparisonsPerBlock = clean(blocksPerWeightedComparison.asInstanceOf[RDD[(Any, ArrayBuffer[Long])]], cleanStrategy)
			.asInstanceOf[ RDD[(Long, ArrayBuffer[(Long, Double)])]]
		weightedComparisonsPerBlock.filter(_._2.nonEmpty)
	}

	def normalizeWeight(weight: Double, entity1: Geometry, entity2:Geometry): Double = weight/(entity1.getArea * entity2.getArea)


}
