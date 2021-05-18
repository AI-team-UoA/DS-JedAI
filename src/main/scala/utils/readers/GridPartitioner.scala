package utils.readers

import model.TileGranularities
import model.entities._
import org.apache.sedona.core.enums.GridType
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.locationtech.jts.geom.{Envelope, Geometry}
import utils.Constants
import utils.configurationParser.DatasetConfigurations

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


    def getAdjustedPartitionsBorders(tilesGranularities: TileGranularities): Array[Envelope] ={
        val adjustedEnvs = partitionBorders.map(env => EnvelopeOp.adjust(env, tilesGranularities))

        // get overall borders
        val globalMinX: Double = adjustedEnvs.map(p => p.getMinX).min
        val globalMaxX: Double = adjustedEnvs.map(p => p.getMaxX).max
        val globalMinY: Double = adjustedEnvs.map(p => p.getMinY).min
        val globalMaxY: Double = adjustedEnvs.map(p => p.getMaxY).max

        // make them integers - filtering is discrete
        val spaceMinX = math.floor(globalMinX).toInt - 1
        val spaceMaxX = math.ceil(globalMaxX).toInt + 1
        val spaceMinY = math.floor(globalMinY).toInt - 1
        val spaceMaxY = math.ceil(globalMaxY).toInt + 1

        adjustedEnvs.map { env =>
            val minX = if (env.getMinX == globalMinX) spaceMinX else env.getMinX
            val maxX = if (env.getMaxX == globalMaxX) spaceMaxX else env.getMaxX
            val minY = if (env.getMinY == globalMinY) spaceMinY else env.getMinY
            val maxY = if (env.getMaxY == globalMaxY) spaceMaxY else env.getMaxY
            new Envelope(minX, maxX, minY, maxY)
        }.toArray
    }

    /**
     *  Loads a dataset into Spatial Partitioned RDD. The partitioner
     *  is defined by the first dataset (i.e. the source dataset)
     * @param dc dataset configuration
     * @return a spatial partitioned rdd
     */
    def transform(srdd: SpatialRDD[Geometry], dc: DatasetConfigurations): RDD[(Int, Entity)] = {
        val withTemporal = dc.dateField.isDefined

        // create Spatial or SpatioTemporal entities
        val rdd: RDD[Entity] =
            if(!withTemporal)
                srdd.rawSpatialRDD.rdd.map( geom =>  SpatialEntity(geom.getUserData.asInstanceOf[String].split("\t")(0), geom))
            else
                srdd.rawSpatialRDD.rdd.mapPartitions{ geomIterator =>
                    val pattern = dc.datePattern.get
                    val formatter = DateTimeFormat.forPattern(pattern)
                    geomIterator.map{ geom =>
                            val userdata = geom.getUserData.asInstanceOf[String].split("\t")
                            val realID = userdata(0)
                            val dateStr = userdata(1)
                            val date: DateTime = formatter.parseDateTime(dateStr)
                            val dateStr_ = date.toString(Constants.defaultDatePattern)
                            SpatioTemporalEntity(realID, geom, dateStr_)
                    }
                }

        distribute(rdd)
    }


    /**
     *  Loads a dataset into Spatial Partitioned RDD. The partitioner
     *  is defined by the first dataset (i.e. the source dataset)
     * @param dc dataset configuration
     * @return a spatial partitioned rdd
     */
    def transformAndFragment(srdd: SpatialRDD[Geometry], dc: DatasetConfigurations)(f: Geometry => Seq[Geometry]): RDD[(Int, Entity)] = {

        val rdd: RDD[Entity] =
            srdd.rawSpatialRDD.rdd.map( geom =>  FragmentedEntity(geom.getUserData.asInstanceOf[String].split("\t")(0), geom)(f))
        distribute(rdd)
    }


    def distribute(rdd: RDD[Entity]): RDD[(Int, Entity)] =
        rdd.flatMap(se => spatialPartitioner.placeObject(se.geometry).asScala.map(i => (i._1.toInt, se)))
            .partitionBy(hashPartitioner)

}