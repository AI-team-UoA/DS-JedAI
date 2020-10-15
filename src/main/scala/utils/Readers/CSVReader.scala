package utils.Readers

import DataStructures.SpatialEntity
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
object CSVReader extends TReader {

    val spark: SparkSession = SparkSession.builder().getOrCreate()

    def load(filePath: String, realIdField: String, geometryField: String) : RDD[SpatialEntity] =
        loadCSV(filePath, realIdField, geometryField, header = true)

    def loadCSV(filePath: String, realIdField: String, geometryField: String, header: Boolean) : RDD[SpatialEntity] =
        loadProfiles(filePath, realIdField, geometryField, header, ",")

    def loadTSV(filePath: String, realIdField: String, geometryField: String, header: Boolean) : RDD[SpatialEntity] =
        loadProfiles(filePath, realIdField, geometryField, header, "\t")

    def loadProfiles(filePath: String, realIdField: String, geometryField: String, header: Boolean, separator: String): RDD[SpatialEntity] = {

        val dt = spark.read.option("header", header).option("sep", separator).option("delimiter", "\"").csv(filePath)

        val SpatialEntities: RDD[SpatialEntity] = dt.rdd
            .mapPartitions{
                rows =>
                    val wktReader = new WKTReader()
                    rows.map {
                        row =>
                            val wkt: String = row.getAs(geometryField).toString
                            val geometry: Geometry = wktReader.read(wkt)
                            (geometry, row)
                    }
            }
            .filter(!_._1.isEmpty)
            .map {
                case(geometry, row) =>
                    val originalID: String = row.getAs(realIdField).toString
                    SpatialEntity(originalID, geometry)
            }
        SpatialEntities.filter(!_.geometry.isEmpty)
    }

}
