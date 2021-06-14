package model.entities

import model.TileGranularities
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.locationtech.jts.geom.Geometry
import utils.configuration.Constants
import utils.configuration.Constants.EntityTypeENUM
import utils.configuration.Constants.EntityTypeENUM.EntityTypeENUM
import utils.geometryUtils.decompose
import utils.geometryUtils.decompose.RecursiveDecomposer

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

case class FragmentedEntityType(tileGranularities: TileGranularities) extends EntityType {
    val entityType: EntityTypeENUM = EntityTypeENUM.FRAGMENTED_ENTITY
    val decomposer: RecursiveDecomposer = decompose.RecursiveDecomposer(tileGranularities)
    val fragmentationF: Geometry => Seq[Geometry] = decomposer.splitBigGeometries
    val transform: Geometry => Entity = (geom: Geometry) =>  FragmentedEntity(geom.getUserData.asInstanceOf[String], geom)(fragmentationF)
}

case class IndexedFragmentedEntityType(tileGranularities: TileGranularities) extends EntityType {
    val entityType: EntityTypeENUM = EntityTypeENUM.INDEXED_FRAGMENTED_ENTITY
    val transform: Geometry => Entity = (geom: Geometry) => IndexedFragmentedEntity(geom.getUserData.asInstanceOf[String], geom, tileGranularities)
}

case class FineGrainedEntityType(tileGranularities: TileGranularities) extends EntityType {
    val entityType: EntityTypeENUM = EntityTypeENUM.FINEGRAINED_ENTITY
    val transform: Geometry => Entity = (geom: Geometry) => FineGrainedEntity(geom.getUserData.asInstanceOf[String], geom, tileGranularities)
}

object EntityTypeFactory {

    def get(entityTypeType: EntityTypeENUM, theta: TileGranularities, datePattern: Option[String] = None): EntityType ={

        entityTypeType match {

            case EntityTypeENUM.SPATIAL_ENTITY =>
                SpatialEntityType()

            case EntityTypeENUM.SPATIOTEMPORAL_ENTITY =>
                SpatioTemporalEntityType(datePattern)

            case EntityTypeENUM.FRAGMENTED_ENTITY =>
                FragmentedEntityType(theta)

            case EntityTypeENUM.INDEXED_FRAGMENTED_ENTITY =>
                IndexedFragmentedEntityType(theta)

            case EntityTypeENUM.FINEGRAINED_ENTITY =>
                FineGrainedEntityType(theta)
        }
    }
}