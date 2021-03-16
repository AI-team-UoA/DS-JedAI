package utils.readers

import com.vividsolutions.jts.geom.Geometry
import dataModel.{Entity, MBR, SpatialEntity, SpatioTemporalEntity}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import utils.Constants.FileTypes
import utils.{Constants, DatasetConfigurations}

import scala.collection.JavaConverters._

trait Reader {

    val sourceDc: DatasetConfigurations
    val partitions: Int
    val gt: Constants.GridType.GridType

    lazy val gridType: GridType = gt match {
        case Constants.GridType.KDBTREE => GridType.KDBTREE
        case _ => GridType.QUADTREE
    }

    // spatial RDD of source
    lazy val spatialRDD: SpatialRDD[Geometry] = load(sourceDc)

    // spatial partitioner defined by the source spatial RDD
    lazy val spatialPartitioner: SpatialPartitioner = {
        spatialRDD.analyze()
        if (partitions > 0) spatialRDD.spatialPartitioning(gridType, partitions) else spatialRDD.spatialPartitioning(gridType)
        spatialRDD.getPartitioner
    }

    // the final partitioner - because the transformation of SRDD into RDD does not preserve partitioning
    // we partitioning using HashPartitioning with the spatial indexes as keys
    lazy val partitioner = new HashPartitioner(spatialPartitioner.numPartitions)

    lazy val partitionsZones: Array[MBR] =
        spatialPartitioner.getGrids.asScala.map(e => MBR(e.getMaxX, e.getMinX, e.getMaxY, e.getMinY)).toArray


    def load(dc: DatasetConfigurations) : SpatialRDD[Geometry]


    /**
     *  Loads a dataset into Spatial Partitioned RDD. The partitioner
     *  is defined by the first dataset (i.e. the source dataset)
     *
     * @param dc dataset configuration
     * @return a spatial partitioned rdd
     */
    def spatialLoad(dc: DatasetConfigurations = sourceDc): RDD[(Int, Entity)] = {
        val srdd = if (dc == sourceDc) spatialRDD else load(dc)
        val sp = SparkContext.getOrCreate().broadcast(spatialPartitioner)

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
        // redistribute based on spatial partitioner
        entitiesRDD
            .flatMap(se => sp.value.placeObject(se.geometry).asScala.map(i => (i._1.toInt, se)))
            .partitionBy(partitioner)
    }
}


object Reader {
    def apply(sourceDc: DatasetConfigurations, partitions: Int, gt: Constants.GridType.GridType = Constants.GridType.QUADTREE) : Reader = {
            val extension = sourceDc.getExtension
            extension match {
            case FileTypes.CSV | FileTypes.TSV => CSVReader(sourceDc, partitions, gt)
            case FileTypes.SHP | FileTypes.GEOJSON => GeospatialReader(sourceDc, partitions, gt)
            case FileTypes.NTRIPLES | FileTypes.TURTLE | FileTypes.RDFXML | FileTypes.RDFJSON => RDFGraphReader(sourceDc, partitions, gt)
            case _ =>  null
        }
    }
}
