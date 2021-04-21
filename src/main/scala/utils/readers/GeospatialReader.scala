package utils.readers

import org.apache.sedona.core.formatMapper.GeoJsonReader
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.locationtech.jts.geom.Geometry
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import utils.Constants.FileTypes
import utils.DatasetConfigurations

object GeospatialReader {

    def extract(dc: DatasetConfigurations): SpatialRDD[Geometry] = {
        val extension = dc.getExtension
        extension match {
            case FileTypes.GEOJSON =>
                loadGeoJSON(dc.path, dc.realIdField.getOrElse("id"), dc.dateField)
            case FileTypes.SHP =>
                loadSHP(dc.path, dc.realIdField.getOrElse("id"), dc.dateField)
        }
    }

    /**
     * Loads an ESRI Shapefile
     * @param filepath path to the SHP file
     * @param realIdField instances' unique id
     * @param dateField date field if exists
     * @return a spatial RDD
     */
    def loadSHP(filepath: String, realIdField: String, dateField: Option[String]): SpatialRDD[Geometry] ={
        val conf = new SparkConf()
        conf.set("spark.serializer", classOf[KryoSerializer].getName)
        conf.set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
        val sc = SparkContext.getOrCreate(conf)

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
        val conf = new SparkConf()
        conf.set("spark.serializer", classOf[KryoSerializer].getName)
        conf.set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
        val sc = SparkContext.getOrCreate(conf)

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
