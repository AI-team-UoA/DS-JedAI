package utils.Reader

import DataStructures.SpatialEntity
import org.apache.spark.rdd.RDD

/**
 * @author George MAndilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
trait ReaderTrait {

  def loadProfiles( filePath: String,
                    realID_field: String,
                    geometryField: String
                  ): RDD[SpatialEntity]
}
