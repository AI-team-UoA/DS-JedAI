package utils.Readers

import DataStructures.SpatialEntity
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD

object Reader {

    private val log = LogManager.getRootLogger

    def read(filepath: String, realID_field: String, geometryField: String, startIdFrom: Int = 0): RDD[SpatialEntity] ={
        val extension = filepath.toString.split("\\.").last
        extension match {
            case "csv" =>
                CSVReader.loadProfiles(filepath, realID_field, geometryField, startIdFrom)
            case "nt" =>
                RDFReader.loadProfiles(filepath, realID_field, geometryField, startIdFrom)
            case _ =>
                log.error("DS-JEDAI: This filetype is not supported yet")
                System.exit(1)
                null
        }

    }
}
