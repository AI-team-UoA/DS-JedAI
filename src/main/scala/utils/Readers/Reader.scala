package utils.Readers

import DataStructures.SpatialEntity
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import utils.Configuration

object Reader {

    private val log = LogManager.getRootLogger

    // TODO: Make header configurable -- probably will need changes in the fields' names
    def read(filepath: String, realID_field: String, geometryField: String, conf: Configuration): RDD[SpatialEntity] ={
        val extension = filepath.toString.split("\\.").last
        val partitions = conf.getPartitions
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
