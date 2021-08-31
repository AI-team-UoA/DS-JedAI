package model.entities

import model.TileGranularities
import model.entities.segmented.{DecomposedEntity, FineGrainedEntity, IndexedDecomposedEntity}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.locationtech.jts.geom.{Envelope, Geometry}
import utils.configuration.Constants
import utils.configuration.Constants.EntityTypeENUM
import utils.configuration.Constants.EntityTypeENUM.EntityTypeENUM
import utils.geometryUtils.decompose.{DecomposerT, EnvelopeRefiner, GridDecomposer, RecursiveDecomposer}

/**
 * GeometryToEntity transformer returns transformation functions that to map geometries into Entities
 * based on input entity type
 *
 * We prefer to return functions than use a Factory-like method as it would force the re-initialization of the
 * decomposers for each entity individually.
 */
object GeometryToEntity {

    /**
     * Return a function that maps a geometry to the requested type of entity
     * @param entityType requested type of entity
     * @param decompositionThetaOpt decomposition threshold in order it is a decomposed entity
     * @param datePattern date pattern in case is a temporal entity
     * @return a function that maps a geometry to the requested type of entity
     */
    def getTransformer(entityType: EntityTypeENUM, decompositionThetaOpt: Option[TileGranularities], datePattern: Option[String] = None): Geometry => EntityT ={

        entityType match {

            case EntityTypeENUM.SPATIAL_ENTITY =>
                geometry: Geometry => SpatialEntity(geometry.getUserData.asInstanceOf[String], geometry)

            case EntityTypeENUM.SPATIOTEMPORAL_ENTITY =>
                val pattern = datePattern.getOrElse(Constants.defaultDatePattern)
                val formatter: DateTimeFormatter = DateTimeFormat.forPattern(pattern)

                geometry: Geometry => {
                    val userdata = geometry.getUserData.asInstanceOf[String].split("\t")
                    assert(userdata.length == 2)
                    val realID = userdata(0)
                    val dateStr = userdata(1)
                    val date: DateTime = formatter.parseDateTime(dateStr)
                    val dateInDefaultPattern = date.toString(Constants.defaultDatePattern)
                    SpatioTemporalEntity(realID, geometry, dateInDefaultPattern)
                }

            case EntityTypeENUM.INDEXED_DECOMPOSED_ENTITY =>
                val decomposer = GridDecomposer(decompositionThetaOpt.get)
                val segmentationF: Geometry => Seq[Geometry] = decomposer.decomposeGeometry
                geometry => IndexedDecomposedEntity(geometry.getUserData.asInstanceOf[String], geometry, decompositionThetaOpt.get, segmentationF)

            case EntityTypeENUM.DECOMPOSED_ENTITY =>
                val decomposer: DecomposerT[Geometry] = RecursiveDecomposer(decompositionThetaOpt.get)
                val segmentationF: Geometry => Seq[Geometry] = decomposer.decomposeGeometry
                geometry: Geometry =>  DecomposedEntity(geometry.getUserData.asInstanceOf[String], geometry, segmentationF)

            case EntityTypeENUM.DECOMPOSED_ENTITY_1D =>
                val decomposer: DecomposerT[Geometry] = RecursiveDecomposer(decompositionThetaOpt.get)
                val segmentationF: Geometry => Seq[Geometry] = g =>  decomposer.decomposeGeometry(g)(oneDimension = true)
                geometry: Geometry =>  DecomposedEntity(geometry.getUserData.asInstanceOf[String], geometry, segmentationF)

            case EntityTypeENUM.FINEGRAINED_ENTITY =>
                val decomposer: EnvelopeRefiner = EnvelopeRefiner(decompositionThetaOpt.get)
                val segmentationF: Geometry => Seq[Envelope] = decomposer.decomposeGeometry
                geometry: Geometry => FineGrainedEntity(geometry.getUserData.asInstanceOf[String], geometry, decompositionThetaOpt.get, segmentationF)
        }
    }
}