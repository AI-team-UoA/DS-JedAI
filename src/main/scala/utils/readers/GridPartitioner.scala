package utils.readers

import model.TileGranularities
import model.entities._
import org.apache.sedona.core.enums.GridType
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Envelope, Geometry}
import utils.configuration.Constants
import utils.geometryUtils.EnvelopeOp

import scala.collection.JavaConverters._

case class GridPartitioner(baseRDD: SpatialRDD[Geometry], partitions: Int, gt: Constants.GridType.GridType = Constants.GridType.QUADTREE) {

    lazy val gridType: org.apache.sedona.core.enums.GridType = gt match {
        case Constants.GridType.KDBTREE => GridType.KDBTREE
        case _ => GridType.QUADTREE
    }

    val spatialPartitioner: SpatialPartitioner = {
        baseRDD.analyze()
        if (partitions > 0)
            baseRDD.spatialPartitioning(gridType, partitions)
        else
            baseRDD.spatialPartitioning(gridType)
        baseRDD.getPartitioner
    }

    lazy val approximateCount: Long = baseRDD.approximateTotalCount

    lazy val hashPartitioner: HashPartitioner = new HashPartitioner(spatialPartitioner.numPartitions)

    lazy val partitionBorders: Seq[Envelope] = spatialPartitioner.getGrids.asScala


    def getPartitionsBorders(theta: TileGranularities): Array[Envelope] = getBorders(partitionBorders.map(env => EnvelopeOp.adjust(env, theta)))

    def getPartitionsBorders: Array[Envelope] = getBorders(partitionBorders)

    def getBorders(envelopes: Seq[Envelope]): Array[Envelope] ={
        // get overall borders
        val globalMinX: Double = envelopes.map(p => p.getMinX).min
        val globalMaxX: Double = envelopes.map(p => p.getMaxX).max
        val globalMinY: Double = envelopes.map(p => p.getMinY).min
        val globalMaxY: Double = envelopes.map(p => p.getMaxY).max

        // make them integers - filtering is discrete
        val spaceMinX = math.floor(globalMinX).toInt - 1
        val spaceMaxX = math.ceil(globalMaxX).toInt + 1
        val spaceMinY = math.floor(globalMinY).toInt - 1
        val spaceMaxY = math.ceil(globalMaxY).toInt + 1

        envelopes.map { env =>
            val minX = if (env.getMinX == globalMinX) spaceMinX else env.getMinX
            val maxX = if (env.getMaxX == globalMaxX) spaceMaxX else env.getMaxX
            val minY = if (env.getMinY == globalMinY) spaceMinY else env.getMinY
            val maxY = if (env.getMaxY == globalMaxY) spaceMaxY else env.getMaxY
            new Envelope(minX, maxX, minY, maxY)
        }.toArray
    }

    /**
     *  Transform a Spatial RDD into an RDD of entities and spatial partition based
     *  on the built spatial partitioner
     *
     * @param srdd  rdd to transform
     * @param geometry2entity a function that transforms a geometry to an entity
     * @return a spatially distributed RDD of entities
     */
    def distributeAndTransform(srdd: SpatialRDD[Geometry], geometry2entity: Geometry => EntityT): RDD[(Int, EntityT)] = {

        val partitionedRDD: RDD[(Int, Geometry)] = srdd.rawSpatialRDD.rdd
            .flatMap(geom => spatialPartitioner.placeObject(geom).asScala.map(i => (i._1.toInt, geom)))
            .partitionBy(hashPartitioner)
        val entitiesRDD: RDD[(Int, EntityT)] = partitionedRDD.map{case (pid, geom) => (pid, geometry2entity(geom))}
        entitiesRDD
    }

    /**
     * the number of all blocks in all partitions
     */
    def computeTotalBlocks(theta: TileGranularities): Double ={

        val globalMinX: Double = partitionBorders.map(p => p.getMinX / theta.x).min
        val globalMaxX: Double = partitionBorders.map(p => p.getMaxX / theta.x).max
        val globalMinY: Double = partitionBorders.map(p => p.getMinY / theta.y).min
        val globalMaxY: Double = partitionBorders.map(p => p.getMaxY / theta.y).max

        (globalMaxX - globalMinX + 1) * (globalMaxY - globalMinY + 1)
    }
}