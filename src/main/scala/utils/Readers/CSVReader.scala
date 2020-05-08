package utils.Readers

import DataStructures.{KeyValue, SpatialEntity}
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
object CSVReader extends TReader {

    val spark: SparkSession = SparkSession.builder().getOrCreate()

    def load(filePath: String, realIdField: String, geometryField: String, startIdFrom: Int = 0) : RDD[SpatialEntity] = {
        loadProfiles(filePath, realIdField, geometryField, startIdFrom)
    }

    def loadProfiles(filePath: String, realIdField: String, geometryField: String,  startIdFrom: Int = 0, header: Boolean = true,
                              separator: String = ","): RDD[SpatialEntity] = {

        val dt = spark.read.option("header", header).option("sep", separator).option("delimiter", "\"").csv(filePath)
        val attrColumns: Array[(String, Int)] = dt.columns.zipWithIndex.filter{ case(col, i) => col != realIdField && col != geometryField}

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
            .zipWithIndex()
            .map {
                case((geometry, row), index) =>
                    val id = index + startIdFrom
                    val originalID: String = row.getAs(realIdField).toString
                    SpatialEntity(id.toInt, originalID, geometry)
            }
        SpatialEntities.filter(!_.geometry.isEmpty)
    }

}
