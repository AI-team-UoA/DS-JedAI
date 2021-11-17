package utils.readers

import org.apache.sedona.core.formatMapper.GeoJsonReader
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{Geometry, GeometryFactory}
import org.locationtech.jts.io.WKTReader
import utils.configuration.Constants.FileTypes
import utils.configuration.Constants.FileTypes.FileTypes
import utils.configuration.DatasetConfigurations

import java.io.File
import scala.util.{Success, Try}

object Reader {

    val conf = new SparkConf()
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
    val sc: SparkContext = SparkContext.getOrCreate(conf)
    val spark: SparkSession = SparkSession.getActiveSession.get
    SedonaSQLRegistrator.registerAll(spark)

    def getExtension(path: String): FileTypes = FileTypes.withName(path.split("\\.").last)

    /**
     * Load input as Spatial RDD
     * @param dc dataset configuration
     * @return the geometries as a SpatialRDD
     */
    def read(dc: DatasetConfigurations) : SpatialRDD[Geometry] = {
        val path: String = dc.path
        readFile(path, dc.realIdField, dc.geometryField, dc.dateField)
    }

    def readFile(path: String, realIdField: Option[String], geometryField: String, dateField: Option[String]): SpatialRDD[Geometry]={
        val file = new File(path)
        if(!file.isDirectory) {
            val extension = getExtension(path)
            val srdd = extension match {
                case FileTypes.CSV =>
                    loadDelimitedFile(path, realIdField.getOrElse("0"), geometryField, dateField, ",")
                case FileTypes.TSV =>
                    loadDelimitedFile(path, realIdField.getOrElse("0"), geometryField, dateField, "\t")
                case FileTypes.GEOJSON =>
                    loadGeoJSON(path, realIdField.getOrElse("id"), dateField)
                case FileTypes.SHP =>
                    loadSHP(path, realIdField.getOrElse("id"), dateField)
                case FileTypes.NTRIPLES =>
                    loadRdfAsTextual(path, geometryField)
            }
            srdd.rawSpatialRDD = srdd.rawSpatialRDD
                .rdd.filter(g => g.getGeometryType != Geometry.TYPENAME_GEOMETRYCOLLECTION && g.isValid && !g.isEmpty)
            srdd
        }
        else {
            val spatialRDD = new SpatialRDD[Geometry]()
            spatialRDD.rawSpatialRDD = file.listFiles
                .filter(_.isFile)
                .map(f => readFile(f.getPath,  realIdField, geometryField, dateField))
                .foldLeft(sc.emptyRDD[Geometry])((baseRDD, srdd) => baseRDD.union(srdd.rawSpatialRDD.rdd))
            spatialRDD
        }
    }


    /**
     * Loads a delimited file
     * @param filepath path to the delimited text file
     * @param realIdField instances' unique id
     * @param geometryField geometry field
     * @param dateField date field if exists
     * @param delimiter delimiter
     * @return a spatial RDD
     *
     * TODO handle date
     */
    def loadDelimitedFile(filepath: String, realIdField: String, geometryField: String, dateField: Option[String], delimiter: String): SpatialRDD[Geometry] ={
//        WktReader.readToGeometryRDD(sc, filepath, geometryField.toInt, false, true)
        val rawTextRDD: RDD[String] = sc.textFile(filepath)
        val idIndex = realIdField.toInt
        val geometryIndex = geometryField.toInt
        val maxIndex = math.max(idIndex, geometryIndex)

        val geomRDD: RDD[Geometry] = rawTextRDD.mapPartitions{ p =>
            val reader = new WKTReader()
            val geometryFactory = new GeometryFactory()
            p.map{line => line.split(delimiter)}
            .filter(tokens => tokens.nonEmpty && tokens.length > maxIndex)
            .map{ tokens =>
                val geomText = tokens(geometryIndex).replaceAll("\"", "")
                val id = tokens(idIndex)
                val geometry = Try(reader.read(geomText)) match {
                    case Success(g) if g!= null=> g
                    case _ =>
                        geometryFactory.createEmpty(2)
                }
                geometry.setUserData(id)
                geometry
            }
        }
        val spatialRDD = new SpatialRDD[Geometry]()
        spatialRDD.rawSpatialRDD = geomRDD
        spatialRDD
    }

    /**
     * Loads RDF as text and not as a Graph model
     * @param filepath path to the delimited text file
     * @param geometryPredicate predicate pointing to Geometries
     * @return a spatial RDD
     */
    def loadRdfAsTextual(filepath: String, geometryPredicate: String): SpatialRDD[Geometry] = {
        val cleanWKT = (wkt: String) => wkt.replaceAll("<\\S+>\\s?", "").replaceAll("\"", "")
        val rowRDD: RDD[Row] = spark.read.textFile(filepath)
            .rdd.map(s => s.split(" ", 3))
            .filter(s => s(1) == geometryPredicate)
            .map(s => (s(0), cleanWKT(s(2))))
            .filter(s => s._1 != null && s._2 != null && s._2.nonEmpty)
            .filter(s => !s._2.contains("EMPTY"))
            .map(s => Row(s._1, s._2))

        val schema = new StructType()
            .add(StructField("Subject", StringType, nullable = true))
            .add(StructField("WKT", StringType, nullable = true))

        val df = spark.createDataFrame(rowRDD, schema)
        df.createOrReplaceTempView("GEOMETRIES")
        val query = "SELECT ST_GeomFromWKT(GEOMETRIES.WKT) AS WKT,  GEOMETRIES.Subject AS Subject FROM GEOMETRIES".stripMargin

        val spatialDF = spark.sql(query)
        Adapter.toSpatialRdd(spatialDF, "WKT", Seq("WKT", "Subject"))
    }


    /**
     * Loads an ESRI Shapefile
     * @param filepath path to the SHP file
     * @param realIdField instances' unique id
     * @param dateField date field if exists
     * @return a spatial RDD
     */
    def loadSHP(filepath: String, realIdField: String, dateField: Option[String]): SpatialRDD[Geometry] ={
        val parentFolder = filepath.substring(0, filepath.lastIndexOf("/"))
        val srdd = ShapefileReader.readToGeometryRDD(sc, parentFolder)
        adjustUserData(srdd, realIdField, dateField)
        null
    }


    /**
     * Loads a GeoJSON file
     * @param filepath path to the SHP file
     * @param realIdField instances' unique id
     * @param dateField date field if exists
     * @return a spatial RDD
     */
    def loadGeoJSON(filepath: String, realIdField: String, dateField: Option[String]): SpatialRDD[Geometry] ={
        val srdd = GeoJsonReader.readToGeometryRDD(sc, filepath)
        adjustUserData(srdd, realIdField, dateField)
    }

    /**
     *  Adjust users' data.
     *  Discard all properties except the id and the date if it's requested.
     * @param srdd the input rdd
     * @param realIdField the field of id
     * @param dateField the field of data if it's given
     * @return geometries with only the necessary user data
     */
    def adjustUserData(srdd: SpatialRDD[Geometry], realIdField: String, dateField: Option[String]): SpatialRDD[Geometry]={
        val idIndex = srdd.fieldNames.indexOf(realIdField)
        val rddWithUserData: RDD[Geometry] = dateField match {
            case Some(dateField) =>
                val dateIndex = srdd.fieldNames.indexOf(dateField)
                srdd.rawSpatialRDD.rdd.map { g =>
                    val userData = g.getUserData.toString.split("\t")
                    val id = userData(idIndex)
                    val date = userData(dateIndex)
                    g.setUserData(id + '\t' + date)
                    g
                }
            case _ =>
                srdd.rawSpatialRDD.rdd.map{ g =>
                    val userData = g.getUserData.toString.split("\t")
                    val id = userData(idIndex)
                    g.setUserData(id)
                    g
                }
        }
        srdd.setRawSpatialRDD(rddWithUserData)

        // filter records with valid geometries and ids
        srdd.setRawSpatialRDD(srdd.rawSpatialRDD.rdd.filter(g => ! (g.isEmpty || g == null || g.getUserData.toString == "")))
        srdd
    }

}
