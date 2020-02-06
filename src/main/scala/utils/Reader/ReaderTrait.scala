package utils.Reader

import DataStructures.SpatialEntity
import org.apache.spark.rdd.RDD

trait ReaderTrait {

  def loadProfiles( filePath: String,
                    realID_field: String,
                    geometryField: String
                  ): RDD[SpatialEntity]
}
