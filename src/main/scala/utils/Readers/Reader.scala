package utils.Readers

import DataStructures.SpatialEntity
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD

object Reader {

    private val log = LogManager.getRootLogger

    // TODO: Make header configurable -- probably will need changes in the fields' names
    def read(filepath: String, realID_field: String, geometryField: String, partitions: Int = 0, spatialPartition: Boolean = false): RDD[SpatialEntity] ={
        val extension = filepath.toString.split("\\.").last
        if (spatialPartition){
            SpatialReader.setPartitions(partitions)
            extension match{
                case "csv" =>
                    SpatialReader.loadCSV(filepath, realID_field, geometryField, header = true)
                case "tsv" =>
                    SpatialReader.loadTSV(filepath, realID_field, geometryField, header = true)
            }
        }
        else {
            val rdd: RDD[SpatialEntity] = extension match {
                case "csv" =>
                    CSVReader.loadCSV(filepath, realID_field, geometryField, header = true)
                case "tsv" =>
                    CSVReader.loadTSV(filepath, realID_field, geometryField, header = true)
                case "nt" =>
                    RDF_Reader.load(filepath, realID_field, geometryField)
                case _ =>
                    log.error("DS-JEDAI: This filetype is not supported yet")
                    System.exit(1)
                    null
            }
            if (partitions > 0) rdd.repartition(partitions)
            else rdd
        }

    }
}
