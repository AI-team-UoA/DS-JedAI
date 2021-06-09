package model.entities

import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import utils.configuration.Constants.Relation.Relation
import utils.configuration.Constants

case class SpatioTemporalEntity(originalID: String, geometry: Geometry, dateStr: String)  extends Entity {

    lazy val dateTime: DateTime = {
        val formatter = DateTimeFormat.forPattern(Constants.defaultDatePattern)
        formatter.parseDateTime(dateStr)
    }


    def temporalFiltering(targetDate: DateTime): Boolean ={
        val days = math.abs(Days.daysBetween(dateTime.toLocalDate, targetDate.toLocalDate).getDays)
        days < 2
    }

    override def intersectingMBR(se: Entity, relation: Relation): Boolean = {
        se match {
            case entity: SpatioTemporalEntity => super.intersectingMBR(se, relation) && temporalFiltering(entity.dateTime)
            case _ =>  super.intersectingMBR(se, relation)
        }
    }

}


/**
 * auxiliary constructors
 */
object SpatioTemporalEntity {

    def apply(originalID: String, wkt: String, dateStr: String): SpatioTemporalEntity ={
        val wktReader = new WKTReader()
        val geometry: Geometry = wktReader.read(wkt)

        SpatioTemporalEntity(originalID, geometry, dateStr)
    }

}