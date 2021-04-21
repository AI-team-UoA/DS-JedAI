package utils.readers

import org.apache.sedona.core.formatMapper.WktReader
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry
import utils.Constants.FileTypes
import utils.DatasetConfigurations

object CSVReader {

    def extract(dc: DatasetConfigurations): SpatialRDD[Geometry] = {
        val extension = dc.getExtension
        extension match {
            case FileTypes.CSV =>
                loadDelimitedFile(dc.path, dc.realIdField.getOrElse("id"), dc.geometryField, dc.dateField, ",", header = true)
            case FileTypes.TSV =>
                loadDelimitedFile(dc.path, dc.realIdField.getOrElse("id"), dc.geometryField, dc.dateField, "\t", header = true)
        }
    }

    /**
     * Loads a delimited file
     * @param filepath path to the delimited text file
     * @param realIdField instances' unique id
     * @param geometryField geometry field
     * @param dateField date field if exists
     * @param delimiter delimiter
     * @param header if first row contains the headers
     * @return a spatial RDD
     */
    def loadDelimitedFile(filepath: String, realIdField: String, geometryField: String, dateField: Option[String], delimiter: String, header: Boolean): SpatialRDD[Geometry] ={
        val conf = new SparkConf()
        conf.set("spark.serializer", classOf[KryoSerializer].getName)
        conf.set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
        val sc = SparkContext.getOrCreate(conf)
        val spark = SparkSession.getActiveSession.get

        SedonaSQLRegistrator.registerAll(spark)
        WktReader.readToGeometryRDD(sc, filepath, geometryField.toInt, false, true)
    }
}
