package model.entities

import model.TileGranularities
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.locationtech.jts.geom.Geometry
import utils.configuration.Constants
import utils.geometryUtils.RecursiveFragmentation

sealed trait EntityType {
    val entityType: String
    val transform: Geometry => Entity
}

case class SpatialEntityType() extends EntityType {
    val entityType: String = "SpatialEntity"
    val transform: Geometry => Entity = (geom: Geometry) => SpatialEntity(geom.getUserData.asInstanceOf[String], geom)
}

case class SpatioTemporalEntityType(pattern: String) extends EntityType {
    val entityType: String = "SpatioTemporalEntity"

    private val formatter: DateTimeFormatter = DateTimeFormat.forPattern(pattern)

    val transform: Geometry => Entity = { geom: Geometry =>
        val userdata = geom.getUserData.asInstanceOf[String].split("\t")
        assert(userdata.length == 2)
        val realID = userdata(0)
        val dateStr = userdata(1)
        val date: DateTime = formatter.parseDateTime(dateStr)
        val dateStr_ = date.toString(Constants.defaultDatePattern)
        SpatioTemporalEntity(realID, geom, dateStr_)
    }
}


case class FragmentedEntityType(tileGranularities: TileGranularities) extends EntityType {
    val entityType: String = "FragmentedEntity"
    val fragmentationF: Geometry => Seq[Geometry] = RecursiveFragmentation.splitBigGeometries(tileGranularities)
    val transform: Geometry => Entity = (geom: Geometry) =>  FragmentedEntity(geom.getUserData.asInstanceOf[String], geom)(fragmentationF)
}


case class IndexedFragmentedEntityType(tileGranularities: TileGranularities) extends EntityType {
    val entityType: String = "IndexedFragmentedEntity"
    val transform: Geometry => Entity = (geom: Geometry) => IndexedFragmentedEntity(geom.getUserData.asInstanceOf[String], geom, tileGranularities)
}