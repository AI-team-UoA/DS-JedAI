package utils.Readers

import DataStructures.SpatialEntity
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession



object RDF_Reader extends TReader {


    def loadProfiles(filePath: String, realID_field: String, geometryField: String, startIdFrom: Int = 0): RDD[SpatialEntity] ={
        val spark: SparkSession = SparkSession.builder().getOrCreate()

        val lang = Lang.NTRIPLES
        val triples = spark.rdf(lang)(filePath)
        triples.take(5).foreach(println(_))

        null
    }

}
