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

case class GridPartitioner(source: SpatialRDD[Geometry], partitions: Int, gt: Constants.GridType.GridType = Constants.GridType.QUADTREE) {

    lazy val gridType: org.apache.sedona.core.enums.GridType = gt match {
        case Constants.GridType.KDBTREE => GridType.KDBTREE
        case _ => GridType.QUADTREE
    }

    val spatialPartitioner: SpatialPartitioner = {
        source.analyze()
        if (partitions > 0)
            source.spatialPartitioning(gridType, partitions)
        else
            source.spatialPartitioning(gridType)
        source.getPartitioner
    }

    lazy val approximateCount: Long = source.approximateTotalCount

    lazy val hashPartitioner: HashPartitioner = new HashPartitioner(spatialPartitioner.numPartitions)

    lazy val partitionBorders: Seq[Envelope] = spatialPartitioner.getGrids.asScala


    def getPartitionsBorders(thetaOpt: Option[TileGranularities]): Array[Envelope] ={
        val partitionEnvelopes = thetaOpt match {
            case Some(theta) => partitionBorders.map(env => EnvelopeOp.adjust(env, theta))
            case None        => partitionBorders
        }

        // get overall borders
        val globalMinX: Double = partitionEnvelopes.map(p => p.getMinX).min
        val globalMaxX: Double = partitionEnvelopes.map(p => p.getMaxX).max
        val globalMinY: Double = partitionEnvelopes.map(p => p.getMinY).min
        val globalMaxY: Double = partitionEnvelopes.map(p => p.getMaxY).max

        // make them integers - filtering is discrete
        val spaceMinX = math.floor(globalMinX).toInt - 1
        val spaceMaxX = math.ceil(globalMaxX).toInt + 1
        val spaceMinY = math.floor(globalMinY).toInt - 1
        val spaceMaxY = math.ceil(globalMaxY).toInt + 1

        partitionEnvelopes.map { env =>
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
     * @param entityType type of entity to transform to
     * @return a spatially distributed RDD of entities
     */
    def distributeAndTransform(srdd: SpatialRDD[Geometry], entityType: EntityType): RDD[(Int, Entity)] = {

        val partitionedRDD: RDD[(Int, Geometry)] = srdd.rawSpatialRDD.rdd
            .flatMap(geom => spatialPartitioner.placeObject(geom).asScala.map(i => (i._1.toInt, geom)))
            .partitionBy(hashPartitioner)

        val transformationF = entityType.transform
        val entitiesRDD: RDD[(Int, Entity)] = partitionedRDD.map{case (pid, geom) => (pid, transformationF(geom))}
        entitiesRDD
    }
}