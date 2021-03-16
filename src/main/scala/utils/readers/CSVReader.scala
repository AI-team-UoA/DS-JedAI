package utils.readers

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import utils.Constants.FileTypes
import utils.{Constants, DatasetConfigurations}

case class CSVReader(sourceDc: DatasetConfigurations, partitions: Int, gt: Constants.GridType.GridType = Constants.GridType.QUADTREE) extends Reader {

    def load(dc: DatasetConfigurations): SpatialRDD[Geometry] = {
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
        conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        val sc = SparkContext.getOrCreate(conf)
        val spark = SparkSession.getActiveSession.get

        GeoSparkSQLRegistrator.registerAll(spark)

        var inputDF = spark.read.format("csv")
            .option("delimiter", delimiter)
            .option("quote", "\"")
            .option("header", header)
            .load(filepath)
            .filter(col(realIdField).isNotNull)
            .filter(col(geometryField).isNotNull)
            .filter(! col(geometryField).contains("EMPTY"))

        var query = s"SELECT ST_GeomFromWKT(GEOMETRIES.$geometryField) AS WKT,  GEOMETRIES.$realIdField AS REAL_ID FROM GEOMETRIES".stripMargin

        if (dateField.isDefined) {
            inputDF = inputDF.filter(col(dateField.get).isNotNull)
            query = s"SELECT ST_GeomFromWKT(GEOMETRIES.$geometryField) AS WKT,  GEOMETRIES.$realIdField AS REAL_ID, GEOMETRIES.${dateField.get} AS DATE  FROM GEOMETRIES".stripMargin
        }

        inputDF.createOrReplaceTempView("GEOMETRIES")

        val spatialDF = spark.sql(query)
        val srdd = new SpatialRDD[Geometry]
        srdd.rawSpatialRDD = Adapter.toRdd(spatialDF)
        srdd
    }
}
