package utils.readers

import model.{Entity, MBR, SpatialEntity, SpatioTemporalEntity}
import org.apache.sedona.core.enums.GridType
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.locationtech.jts.geom.Geometry
import utils.{Constants, DatasetConfigurations}

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


    /**
     *  Loads a dataset into Spatial Partitioned RDD. The partitioner
     *  is defined by the first dataset (i.e. the source dataset)
     * @param dc dataset configuration
     * @return a spatial partitioned rdd
     */
    def distribute(srdd: SpatialRDD[Geometry], dc: DatasetConfigurations): RDD[(Int, Entity)] = {
        val withTemporal = dc.dateField.isDefined

        // remove empty, invalid geometries and geometry collections
        val filteredGeometriesRDD = srdd.rawSpatialRDD.rdd
            .map{ geom =>
                val userdata = geom.getUserData.asInstanceOf[String].split("\t")
                (geom, userdata)
            }
            .filter{case (g, _) => !g.isEmpty && g.isValid && g.getGeometryType != "GeometryCollection"}

        // create Spatial or SpatioTemporal entities
        val entitiesRDD: RDD[Entity] =
            if(!withTemporal)
                filteredGeometriesRDD.map{ case (geom, userdata) =>  SpatialEntity(userdata(0), geom)}
            else
                filteredGeometriesRDD.mapPartitions{ geomIterator =>
                    val pattern = dc.datePattern.get
                    val formatter = DateTimeFormat.forPattern(pattern)
                    geomIterator.map{
                        case (geom, userdata) =>
                            val realID = userdata(0)
                            val dateStr = userdata(1)
                            val date: DateTime = formatter.parseDateTime(dateStr)
                            val dateStr_ = date.toString(Constants.defaultDatePattern)
                            SpatioTemporalEntity(realID, geom, dateStr_)
                    }
                }

        // redistribute based on spatial index
        entitiesRDD
            .flatMap(se => spatialPartitioner.placeObject(se.geometry).asScala.map(i => (i._1.toInt, se)))
            .partitionBy(hashPartitioner)
    }

}