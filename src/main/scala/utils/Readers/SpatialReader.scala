package utils.Readers

import DataStructures.{MBB, SpatialEntity}
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import utils.{Constants, DatasetConfigurations}

import scala.collection.JavaConverters._

case class SpatialReader(sourceDc: DatasetConfigurations, partitions: Int, gt: Constants.GridType.GridType = Constants.GridType.QUADTREE) {

    lazy val gridType: GridType = gt match {
        case Constants.GridType.KDBTREE => GridType.KDBTREE
        case Constants.GridType.QUADTREE => GridType.QUADTREE
    }

    lazy val spatialRDD: SpatialRDD[Geometry] = load(sourceDc)

    lazy val spatialPartitioner: SpatialPartitioner = getSpatialPartitioner(spatialRDD)

    val partitioner = new HashPartitioner(partitions)

    lazy val partitionsZones: Array[MBB] = gridType match {
        case GridType.QUADTREE =>
            spatialRDD.partitionTree.getLeafZones.asScala.map(qr => MBB(qr.getEnvelope)).toArray
        case GridType.KDBTREE =>
            spatialRDD.getPartitioner.getGrids.asScala.map(e => MBB(e.getMaxX, e.getMinX, e.getMaxY, e.getMinY)).toArray
        }


    def load(dc: DatasetConfigurations): SpatialRDD[Geometry] ={
        val extension = dc.path.toString.split("\\.").last
        extension match {
            case "csv" =>
                loadCSV(dc.path, dc.realIdField, dc.geometryField, header = true )
            case "tsv" =>
                loadTSV(dc.path, dc.realIdField, dc.geometryField, header = true )
            case "shp" =>
                loadSHP(dc.path, dc.realIdField, dc.geometryField)
            case _ =>
                null
        }
    }

    def loadCSV(filepath: String, realIdField: String, geometryField: String, header: Boolean):SpatialRDD[Geometry] =
        loadDelimitedFile(filepath, realIdField, geometryField, ",", header)

    def loadTSV(filepath: String, realIdField: String, geometryField: String, header: Boolean): SpatialRDD[Geometry] =
        loadDelimitedFile(filepath, realIdField, geometryField, "\t", header)

    /**
     * Loads a delimited file
     * @param filepath path to the delimited text file
     * @param realIdField instances' unique id
     * @param geometryField geometry field
     * @param delimiter delimiter
     * @param header if first row contains the headers
     * @return a spatial RDD
     */
    def loadDelimitedFile(filepath: String, realIdField: String, geometryField: String, delimiter: String, header: Boolean): SpatialRDD[Geometry] ={
        val conf = new SparkConf()
        conf.set("spark.serializer", classOf[KryoSerializer].getName)
        conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        val sc = SparkContext.getOrCreate(conf)
        val spark = SparkSession.getActiveSession.get

        GeoSparkSQLRegistrator.registerAll(spark)

        val inputDF = spark.read.format("csv")
            .option("delimiter", delimiter)
            .option("quote", "\"")
            .option("header", header)
            .load(filepath)
            .filter(col(realIdField).isNotNull)
            .filter(col(geometryField).isNotNull)
            .filter(! col(geometryField).contains("EMPTY"))

        inputDF.createOrReplaceTempView("GEOMETRIES")

        val geometryQuery = """SELECT ST_GeomFromWKT(GEOMETRIES.""" + geometryField + """) AS WKT,  GEOMETRIES.""" + realIdField + """ AS REAL_ID FROM GEOMETRIES""".stripMargin
        val spatialDF = spark.sql(geometryQuery)
        val srdd = new SpatialRDD[Geometry]
        srdd.rawSpatialRDD = Adapter.toRdd(spatialDF)
        srdd
    }

    /**
     * Loads a shapefile
     * @param filepath path to the SHP file
     * @param realIdField instances' unique id
     * @param geometryField geometry field
     * @return a spatial RDD
     */
    def loadSHP(filepath: String, realIdField: String, geometryField: String): SpatialRDD[Geometry] ={
        val conf = new SparkConf()
        conf.set("spark.serializer", classOf[KryoSerializer].getName)
        conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        val sc = SparkContext.getOrCreate(conf)

        val parentFolder = filepath.substring(0, filepath.lastIndexOf("/"))
        val srdd = ShapefileReader.readToGeometryRDD(sc, parentFolder)
        val idIndex = srdd.fieldNames.indexOf(realIdField)

        // keep only the id from user data
        srdd.rawSpatialRDD =  srdd.rawSpatialRDD.rdd.map{ g =>
            g.setUserData(g.getUserData.toString.split("\t")(idIndex))
            g
        }

        // filter records with valid geometries and ids
        srdd.rawSpatialRDD =  srdd.rawSpatialRDD.rdd.filter(g => ! (g.isEmpty || g == null || g.getUserData.toString == ""))
        srdd
    }


    def getSpatialPartitioner(srdd: SpatialRDD[Geometry]): SpatialPartitioner ={
        srdd.analyze()
        if (partitions > 0) srdd.spatialPartitioning(gridType, partitions) else srdd.spatialPartitioning(gridType)
        srdd.getPartitioner
    }


    def load2PartitionedRDD(dc: DatasetConfigurations = sourceDc): RDD[(Int, SpatialEntity)] = {
        val srdd = if (dc == sourceDc) spatialRDD else load(dc)
        val sp = SparkContext.getOrCreate().broadcast(spatialPartitioner)
        srdd.rawSpatialRDD.rdd
            .map{ geom =>
                val ids = geom.getUserData.asInstanceOf[String].split("\t")
                val realID = ids(0)
                (geom, realID)
            }
            .filter{case (g, _) => !g.isEmpty && g.isValid && g.getGeometryType != "GeometryCollection"}
            .map{ case(g, realId) =>  SpatialEntity(realId, g.asInstanceOf[Geometry])}
            .flatMap(se => sp.value.placeObject(se.geometry).asScala.map(i => (i._1.toInt, se)))
            .partitionBy(partitioner)
    }

}
