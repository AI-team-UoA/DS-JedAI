package Blocking

import DataStructures.{LightBlock, SpatialEntity}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.Constants

import scala.collection.mutable.ArrayBuffer


case class LightRADON(var source: RDD[SpatialEntity], var target: RDD[SpatialEntity] = null, theta_msr: String = Constants.NO_USE ) extends Blocking with Serializable {

    /**
     * initialize theta based on theta measure
     */
    def initTheta(): Unit ={
        val thetaMsr: RDD[(Double, Double)] = source
            .union(target)
            .map {
                sp =>
                    val env = sp.geometry.getEnvelopeInternal
                    (env.getHeight, env.getWidth)
            }
            .setName("thetaMsr")
            .cache()

        var thetaX = 1d
        var thetaY = 1d
        theta_msr match {
            // WARNING: small or big values of theta may affect negatively the indexing procedure
            case Constants.MIN =>
                // filtering because there are cases that the geometries are perpendicular to the axes
                // and have width or height equals to 0.0
                thetaX = thetaMsr.map(_._1).filter(_ != 0.0d).min
                thetaY = thetaMsr.map(_._2).filter(_ != 0.0d).min
            case Constants.MAX =>
                thetaX = thetaMsr.map(_._1).max
                thetaY = thetaMsr.map(_._2).max
            case Constants.AVG =>
                val length = thetaMsr.count
                thetaX = thetaMsr.map(_._1).sum() / length
                thetaY = thetaMsr.map(_._2).sum() / length
            case _ =>
        }
        val broadcastedTheta = SparkContext.getOrCreate().broadcast((thetaX, thetaY))
        broadcastMap += ("theta" -> broadcastedTheta.asInstanceOf[Broadcast[Any]])
        thetaMsr.unpersist()
    }



    /**
     * index a spatial entities set. If acceptedBlocks is provided then the entities will be assigned
     * to blocks that exist in the accepted blocks
     *
     * @param spatialEntitiesRDD the set to index
     * @param acceptedBlocks the accepted blocks that the set can be indexed to
     * @return an Array of block ids for each spatial entity
     */
    def index(spatialEntitiesRDD: RDD[SpatialEntity], acceptedBlocks: Set[(Int, Int)] = Set()): RDD[((Int, Int), ArrayBuffer[SpatialEntity])] ={
        val acceptedBlocksBD = SparkContext.getOrCreate().broadcast(acceptedBlocks)
        broadcastMap += ("acceptedBlocks" -> acceptedBlocksBD.asInstanceOf[Broadcast[Any]])
        spatialEntitiesRDD.map { se =>
            val (thetaX, thetaY) = broadcastMap("theta").value.asInstanceOf[(Double, Double)]

            val blockIDs = {
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

                    if (acceptedBlocksBD.value.nonEmpty) {
                        val western =  for (x <- wmbb_minX to wmbb_maxX; y <- wmbb_minY to wmbb_maxY; if acceptedBlocksBD.value.contains((x, y))) yield (x, y)
                        val eastern = for (x <- embb_minX to embb_maxX; y <- embb_minY to embb_maxY; if acceptedBlocksBD.value.contains((x, y))) yield (x, y)
                        eastern ++ western
                    }
                    else{
                        val western =  for (x <- wmbb_minX to wmbb_maxX; y <- wmbb_minY to wmbb_maxY) yield (x, y)
                        val eastern = for (x <- embb_minX to embb_maxX; y <- embb_minY to embb_maxY) yield (x, y)
                        eastern ++ western
                    }
                }
                else {
                    val maxX = math.ceil(se.mbb.maxX / thetaX).toInt
                    val minX = math.floor(se.mbb.minX / thetaX).toInt
                    val maxY = math.ceil(se.mbb.maxY / thetaY).toInt
                    val minY = math.floor(se.mbb.minY / thetaY).toInt

                    if (acceptedBlocksBD.value.nonEmpty)
                        for (x <- minX to maxX; y <- minY to maxY; if acceptedBlocksBD.value.contains((x, y))) yield (x, y)
                    else
                        for (x <- minX to maxX; y <- minY to maxY) yield (x, y)
                }
            }
            (blockIDs, se)
        }
            .flatMap(b => b._1.map(id => (id, ArrayBuffer[SpatialEntity](b._2) ))).reduceByKey(_ ++ _ )
    }

    /**
     * index a spatial entities set. If acceptedBlocks is provided then the entities will be assigned
     * to blocks that exist in the accepted blocks
     *
     * @param spatialEntitiesRDD the set to index
     * @param acceptedBlocks the accepted blocks that the set can be indexed to
     * @return an Array of block ids for each spatial entity
     */
    def lightIndex(spatialEntitiesRDD: RDD[SpatialEntity], acceptedBlocks: Set[(Int, Int)] = Set()): RDD[((Int, Int), ArrayBuffer[Int])] ={
        val acceptedBlocksBD = SparkContext.getOrCreate().broadcast(acceptedBlocks)
        broadcastMap += ("acceptedBlocks" -> acceptedBlocksBD.asInstanceOf[Broadcast[Any]])
        spatialEntitiesRDD.map { se =>
            val (thetaX, thetaY) = broadcastMap("theta").value.asInstanceOf[(Double, Double)]

            val blockIDs = {
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

                    if (acceptedBlocksBD.value.nonEmpty) {
                        val western =  for (x <- wmbb_minX to wmbb_maxX; y <- wmbb_minY to wmbb_maxY; if acceptedBlocksBD.value.contains((x, y))) yield (x, y)
                        val eastern = for (x <- embb_minX to embb_maxX; y <- embb_minY to embb_maxY; if acceptedBlocksBD.value.contains((x, y))) yield (x, y)
                        eastern ++ western
                    }
                    else{
                        val western =  for (x <- wmbb_minX to wmbb_maxX; y <- wmbb_minY to wmbb_maxY) yield (x, y)
                        val eastern = for (x <- embb_minX to embb_maxX; y <- embb_minY to embb_maxY) yield (x, y)
                        eastern ++ western
                    }
                }
                else {
                    val maxX = math.ceil(se.mbb.maxX / thetaX).toInt
                    val minX = math.floor(se.mbb.minX / thetaX).toInt
                    val maxY = math.ceil(se.mbb.maxY / thetaY).toInt
                    val minY = math.floor(se.mbb.minY / thetaY).toInt

                    if (acceptedBlocksBD.value.nonEmpty)
                        for (x <- minX to maxX; y <- minY to maxY; if acceptedBlocksBD.value.contains((x, y))) yield (x, y)
                    else
                        for (x <- minX to maxX; y <- minY to maxY) yield (x, y)
                }
            }
            (blockIDs, se)
        }
        .flatMap(b => b._1.map(id => (id, ArrayBuffer[Int](b._2.id) ))).reduceByKey(_ ++ _ )
    }


    override def apply(liTarget: Boolean = true): RDD[LightBlock] = {
        initTheta()

        val blocksIndex: RDD[((Int, Int), (ArrayBuffer[SpatialEntity], Option[ArrayBuffer[Int]]))] =
            if (liTarget){
                val sIndex = index(source)
                val allowedBlocks: Set[(Int, Int)] = sIndex.map(b => Set(b._1)).reduce(_++_)
                val tIndex = lightIndex(target, allowedBlocks)
                sIndex.leftOuterJoin(tIndex)
            }
            else{
                val sIndex = lightIndex(source)
                val allowedBlocks: Set[(Int, Int)] = sIndex.map(b => Set(b._1)).reduce(_++_)
                val tIndex = index(target, allowedBlocks)
                tIndex.leftOuterJoin(sIndex)
            }

        val blocksRDD:RDD[LightBlock] = blocksIndex
            .filter(b => b._2._2.isDefined)
            .map { block =>
                val blockCoords = block._1
                val sourceIndexSet = block._2._1.toSet
                val targetIndexSet: Set[Int] = block._2._2.get.toSet
                LightBlock(blockCoords, sourceIndexSet, targetIndexSet)
            }
            .setName("BlocksRDD")
        blocksRDD
    }

}
