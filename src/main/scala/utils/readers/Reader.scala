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
import utils.Constants.FileTypes
import utils.{Constants, DatasetConfigurations}

import scala.collection.JavaConverters._

case class Reader(partitions: Int, gt: Constants.GridType.GridType, printStats: Boolean = false) {

    var counter: Long = 0

    /**
     * The transformation of an SRDD into RDD does not preserve partitioning.
     * Hence we use a spatial partitioner to spatially index the geometries and then
     * we partition using a HashPartitioner and the spatial indexes as the partition keys
     */
    var spatialPartitioner: SpatialPartitioner = _
    var partitioner: HashPartitioner = _
    lazy val partitionsZones: Array[MBR] = spatialPartitioner.getGrids.asScala.map(e => MBR(e.getMaxX, e.getMinX, e.getMaxY, e.getMinY)).toArray

    val gridType: org.apache.sedona.core.enums.GridType = gt match {
        case Constants.GridType.KDBTREE => GridType.KDBTREE
        case _ => GridType.QUADTREE
    }

    /**
     * Extract the geometries from the input configurations
     * As a side-effect, it also counts the geometries, if requested
     * @param dc dataset configuration
     * @return the geometries as a SpatialRDD
     */
    def extract(dc: DatasetConfigurations) : SpatialRDD[Geometry] = {
        val extension = dc.getExtension
        val rdd = extension match {
            case FileTypes.CSV | FileTypes.TSV => CSVReader.extract(dc)
            case FileTypes.SHP | FileTypes.GEOJSON => GeospatialReader.extract(dc)
            case FileTypes.NTRIPLES | FileTypes.TURTLE | FileTypes.RDFXML | FileTypes.RDFJSON => RDFGraphReader.extract(dc)
        }
        if (printStats) counter = rdd.rawSpatialRDD.count()
        rdd
    }

    /**
     * Load source dataset, the dataset which will initialize the partitioners
     * @param dc dc dataset configuration
     * @return an RDD of pairs of partition index and entities
     */
    def loadSource(dc: DatasetConfigurations): RDD[(Int, Entity)] ={
        val sourceRDD = extract(dc)
        sourceRDD.analyze()
        if (partitions > 0)
            sourceRDD.spatialPartitioning(gridType, partitions)
        else
            sourceRDD.spatialPartitioning(gridType)
        spatialPartitioner = sourceRDD.getPartitioner
        partitioner = new HashPartitioner(spatialPartitioner.numPartitions)
        distribute(sourceRDD, dc)
    }

    /**
     * Load the input dataset. If the loadSource has not been called, it will result to
     * a NullPointerException.
     * @param dc dc dataset configuration
     * @return an RDD of pairs of partition index and entities
     */
    def load(dc: DatasetConfigurations): Either[java.lang.Throwable, RDD[(Int, Entity)]] ={
        val rdd = extract(dc)
        try {
            Right(distribute(rdd, dc))
        } catch {
            case ex: Throwable =>Left(ex)
        }
    }

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
            .partitionBy(partitioner)
    }
}