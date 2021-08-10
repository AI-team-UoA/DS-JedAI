package model.entities

import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}
import org.locationtech.jts.geom.{Envelope, Geometry}
import utils.configuration.Constants
import utils.configuration.Constants.Relation.Relation

case class SpatioTemporalEntity(originalID: String, geometry: Geometry, dateStr: String, env: Envelope)  extends Entity {

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

object SpatioTemporalEntity{

    def apply(originalID: String, geometry: Geometry, dateStr: String): SpatioTemporalEntity =
        SpatioTemporalEntity(originalID, geometry, dateStr, geometry.getEnvelopeInternal)
}