package model.entities

import model.TileGranularities
import model.entities.segmented.{DecomposedEntity, FineGrainedEntity, IndexedDecomposedEntity}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.locationtech.jts.geom.{Envelope, Geometry}
import utils.configuration.Constants
import utils.configuration.Constants.EntityTypeENUM
import utils.configuration.Constants.EntityTypeENUM.{DECOMPOSED_ENTITY, DECOMPOSED_ENTITY_1D, EntityTypeENUM, INDEXED_DECOMPOSED_ENTITY_1D}
import utils.geometryUtils.decompose.{DecomposerT, EnvelopeRefiner, GridDecomposer, RecursiveDecomposer}

// TODO comment
sealed trait EntityType {
    val entityType: EntityTypeENUM
    val transform: Geometry => Entity
}

case class SpatialEntityType() extends EntityType {
    val entityType: EntityTypeENUM = EntityTypeENUM.SPATIAL_ENTITY
    val transform: Geometry => Entity = (geom: Geometry) => SpatialEntity(geom.getUserData.asInstanceOf[String], geom)
}

case class SpatioTemporalEntityType(pattern: Option[String]) extends EntityType {
    val entityType: EntityTypeENUM = EntityTypeENUM.SPATIOTEMPORAL_ENTITY

    private val pattern_ = pattern.getOrElse(Constants.defaultDatePattern)
    private val formatter: DateTimeFormatter = DateTimeFormat.forPattern(pattern_)

    val transform: Geometry => Entity = { geom: Geometry =>
        val userdata = geom.getUserData.asInstanceOf[String].split("\t")
        assert(userdata.length == 2)
        val realID = userdata(0)
        val dateStr = userdata(1)
        val date: DateTime = formatter.parseDateTime(dateStr)
        val dateInDefaultPattern = date.toString(Constants.defaultDatePattern)
        SpatioTemporalEntity(realID, geom, dateInDefaultPattern)
    }
}

case class DecomposedEntityType(tileGranularities: TileGranularities, entityType: EntityTypeENUM) extends EntityType {
    val decomposer: DecomposerT[Geometry] = entityType match {
        case INDEXED_DECOMPOSED_ENTITY_1D => GridDecomposer(tileGranularities)
        case _ => RecursiveDecomposer(tileGranularities)
    }
    val segmentationF: Geometry => Seq[Geometry] = entityType match {
        case DECOMPOSED_ENTITY_1D | INDEXED_DECOMPOSED_ENTITY_1D => g: Geometry => decomposer.decomposeGeometry(g)(oneDimension = true)
        case DECOMPOSED_ENTITY => decomposer.decomposeGeometry
    }
    val transform: Geometry => Entity = (geom: Geometry) =>  DecomposedEntity(geom.getUserData.asInstanceOf[String], geom, segmentationF)
}

case class IndexedDecomposedEntityType(tileGranularities: TileGranularities) extends EntityType {
    val entityType: EntityTypeENUM = EntityTypeENUM.INDEXED_DECOMPOSED_ENTITY
    val decomposer: GridDecomposer = GridDecomposer(tileGranularities)
    val segmentationF: Geometry => Seq[Geometry] = decomposer.decomposeGeometry
    val transform: Geometry => Entity = (geom: Geometry) => IndexedDecomposedEntity(geom.getUserData.asInstanceOf[String], geom, tileGranularities, segmentationF)
}

case class FineGrainedEntityType(tileGranularities: TileGranularities) extends EntityType {
    val entityType: EntityTypeENUM = EntityTypeENUM.FINEGRAINED_ENTITY
    val decomposer: EnvelopeRefiner = EnvelopeRefiner(tileGranularities)
    val segmentationF: Geometry => Seq[Envelope] = decomposer.decomposeGeometry
    val transform: Geometry => Entity = (geom: Geometry) => FineGrainedEntity(geom.getUserData.asInstanceOf[String], geom, tileGranularities, segmentationF)
}

object EntityTypeFactory {

    def get(entityTypeType: EntityTypeENUM, thetaOpt: Option[TileGranularities], datePattern: Option[String] = None): Option[EntityType] ={

        entityTypeType match {

            case EntityTypeENUM.SPATIAL_ENTITY =>
                Some(SpatialEntityType())

            case EntityTypeENUM.SPATIOTEMPORAL_ENTITY =>
                Some(SpatioTemporalEntityType(datePattern))

            case EntityTypeENUM.DECOMPOSED_ENTITY | EntityTypeENUM.DECOMPOSED_ENTITY_1D | EntityTypeENUM.INDEXED_DECOMPOSED_ENTITY_1D =>
                thetaOpt.map(theta => DecomposedEntityType(theta, entityTypeType))

            case EntityTypeENUM.INDEXED_DECOMPOSED_ENTITY =>
                thetaOpt.map(theta => IndexedDecomposedEntityType(theta))

            case EntityTypeENUM.FINEGRAINED_ENTITY =>
                thetaOpt.map(theta => FineGrainedEntityType(theta))
        }
    }
}