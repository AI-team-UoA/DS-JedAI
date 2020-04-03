package Blocking

import DataStructures.{LightBlock, SpatialEntity}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.Constants

import scala.collection.mutable.ArrayBuffer


/**
 *  Similar to RADON blocking algorithms, but blocks entities inside LightBlocks
 *
 * @param source source set as RDD
 * @param target target set as RDD
 * @param thetaMsrSTR theta measure
 **/
case class LightRADON(var source: RDD[SpatialEntity], var target: RDD[SpatialEntity] = null, thetaMsrSTR: String = Constants.NO_USE ) extends Blocking with Serializable {

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
        spatialEntitiesRDD.mapPartitions { seIter =>
            val thetaMsr = broadcastMap("theta").value.asInstanceOf[(Double, Double)]
            val acceptedBlocks = acceptedBlocksBD.value
            seIter.map(se => (indexSpatialEntity(se, acceptedBlocks, thetaMsr), se))
        }
        .flatMap(b => b._1.map(id => (id, ArrayBuffer[SpatialEntity](b._2) ))).reduceByKey(_ ++ _ )
    }

    /**
     * index a spatial entities set. If acceptedBlocks is provided then the entities will be assigned
     * to blocks that exist in the accepted blocks.
     * The difference is that instead of storing the whole entities, it just saves their id
     *
     * @param spatialEntitiesRDD the set to index
     * @param acceptedBlocks the accepted blocks that the set can be indexed to
     * @return an Array of block ids for each spatial entity
     */
    def lightIndex(spatialEntitiesRDD: RDD[SpatialEntity], acceptedBlocks: Set[(Int, Int)] = Set()): RDD[((Int, Int), ArrayBuffer[Int])] ={
        val acceptedBlocksBD = SparkContext.getOrCreate().broadcast(acceptedBlocks)
        broadcastMap += ("acceptedBlocks" -> acceptedBlocksBD.asInstanceOf[Broadcast[Any]])
        spatialEntitiesRDD.mapPartitions { seIter =>
            val thetaMsr = broadcastMap("theta").value.asInstanceOf[(Double, Double)]
            val acceptedBlocks = acceptedBlocksBD.value
            seIter.map(se => (indexSpatialEntity(se, acceptedBlocks, thetaMsr), se))
        }
        .flatMap(b => b._1.map(id => (id, ArrayBuffer[Int](b._2.id) ))).reduceByKey(_ ++ _ )
    }


    /**
     * blocks SpatialEntities inside LightBlocks
     * @param liTarget if liTarget is true then the blocks will contain only the ids of target instead of
     *                 the entities, else this will happen for source.
     * @return an RDD of LightBlocks
     */
    override def apply(liTarget: Boolean = true): RDD[LightBlock] = {
        initTheta(thetaMsrSTR)

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
