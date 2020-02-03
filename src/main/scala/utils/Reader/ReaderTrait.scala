package utils.Reader

import DataStructures.GeoProfile
import org.apache.spark.rdd.RDD

trait ReaderTrait {

  def loadProfiles( filePath: String,
                    realID_field: String,
                    geometryField: String
                  ): RDD[GeoProfile]
}
