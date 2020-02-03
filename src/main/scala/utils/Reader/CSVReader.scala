package utils.Reader

import DataStructures.{GeoProfile, KeyValue}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession



object CSVReader extends ReaderTrait {

    // TODO: Try to aplly spatial Partitioning
    // WARNING: GeoSpark couldn't read the csv properly

    val spark: SparkSession = SparkSession.builder().getOrCreate()

    override def loadProfiles(filePath: String, realIdField: String, geometryField: String) : RDD[GeoProfile] = {
        loadProfiles2(filePath, realIdField, geometryField)
    }

    def loadProfiles2(filePath: String, realIdField: String, geometryField: String, header: Boolean = true,
                              separator: String = ",", startIdFrom: Int = 0): RDD[GeoProfile] = {

        val dt = spark.read.option("header", header).option("sep", separator).option("delimiter", "\"").csv(filePath)
        val attrColumns: Array[(String, Int)] = dt.columns.zipWithIndex.filter{ case(col, i) => col != realIdField && col != geometryField}

        val geoProfiles: RDD[GeoProfile] = dt.rdd.zipWithIndex()
            .map {
                case(row, index) =>
                    val id = index + startIdFrom
                    val geometry: String = row.getAs(geometryField).toString
                    val originalID: String = row.getAs(realIdField).toString
                    val attr =  attrColumns
                        .filter({ case(_, index) => !row. isNullAt(index)})
                        .map({case(col, index) => KeyValue(col, row.get(index).toString)})
                    GeoProfile(id.toInt, originalID, attr, geometry)
            }

        geoProfiles
    }

}
