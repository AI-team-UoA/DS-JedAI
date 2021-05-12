package utils.readers

import model.entities.{Entity, FragmentedEntity, SpatialEntity, SpatioTemporalEntity}
import model.{MBR, TileGranularities}
import org.apache.sedona.core.enums.GridType
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.locationtech.jts.geom.Geometry
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

    lazy val hashPartitioner: HashPartitioner = new HashPartitioner(spatialPartitioner.numPartitions)

    lazy val partitionBorders: Seq[MBR] = spatialPartitioner.getGrids.asScala.map(e => MBR(e.getMaxX, e.getMinX, e.getMaxY, e.getMinY))


    def getAdjustedBordersOfMBR(tilesGranularities: TileGranularities): Array[MBR] ={
        val adjustedMBRs = partitionBorders.map(_.adjust(tilesGranularities))

        // get overall borders
        val globalMinX: Double = adjustedMBRs.map(p => p.minX).min
        val globalMaxX: Double = adjustedMBRs.map(p => p.maxX).max
        val globalMinY: Double = adjustedMBRs.map(p => p.minY).min
        val globalMaxY: Double = adjustedMBRs.map(p => p.maxY).max

        // make them integers - filtering is discrete
        val spaceMinX = math.floor(globalMinX).toInt - 1
        val spaceMaxX = math.ceil(globalMaxX).toInt + 1
        val spaceMinY = math.floor(globalMinY).toInt - 1
        val spaceMaxY = math.ceil(globalMaxY).toInt + 1

        adjustedMBRs.map { mbr =>
            val minX = if (mbr.minX == globalMinX) spaceMinX else mbr.minX
            val maxX = if (mbr.maxX == globalMaxX) spaceMaxX else mbr.maxX
            val minY = if (mbr.minY == globalMinY) spaceMinY else mbr.minY
            val maxY = if (mbr.maxY == globalMaxY) spaceMaxY else mbr.maxY
            MBR(maxX, minX, maxY, minY)
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