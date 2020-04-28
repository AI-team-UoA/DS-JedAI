package utils.Readers

import DataStructures.SpatialEntity
import org.apache.spark.rdd.RDD

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
trait TReader {

  def loadProfiles( filePath: String,
                    realID_field: String,
                    geometryField: String,
                    startIdFrom: Int
                  ): RDD[SpatialEntity]
}
