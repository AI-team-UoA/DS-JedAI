package utils.Reader

import DataStructures.{SpatialEntity, KeyValue}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
object CSVReader extends ReaderTrait {

    val spark: SparkSession = SparkSession.builder().getOrCreate()

    override def loadProfiles(filePath: String, realIdField: String, geometryField: String) : RDD[SpatialEntity] = {
        loadProfiles2(filePath, realIdField, geometryField)
    }

    def loadProfiles2(filePath: String, realIdField: String, geometryField: String,  startIdFrom: Int = 0, header: Boolean = true,
                              separator: String = ","): RDD[SpatialEntity] = {

        val dt = spark.read.option("header", header).option("sep", separator).option("delimiter", "\"").csv(filePath)
        val attrColumns: Array[(String, Int)] = dt.columns.zipWithIndex.filter{ case(col, i) => col != realIdField && col != geometryField}

        val SpatialEntities: RDD[SpatialEntity] = dt.rdd.zipWithIndex()
            .map {
                case(row, index) =>
                    val id = index + startIdFrom
                    val geometry: String = row.getAs(geometryField).toString
                    val originalID: String = row.getAs(realIdField).toString
                    val attr =  attrColumns
                        .filter({ case(_, index) => !row. isNullAt(index)})
                        .map({case(col, index) => KeyValue(col, row.get(index).toString)})
                    SpatialEntity(id.toInt, originalID, attr, geometry)
            }

        SpatialEntities.filter(!_.geometry.isEmpty)
    }

}
