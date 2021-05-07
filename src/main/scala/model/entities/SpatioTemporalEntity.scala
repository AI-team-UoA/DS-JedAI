package model.entities

import model.MBR
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import utils.Constants
import utils.Constants.Relation.Relation

case class SpatioTemporalEntity(originalID: String = "", geometry: Geometry, mbr: MBR, dateStr: String)  extends Entity {

    lazy val dateTime: DateTime = {
        val formatter = DateTimeFormat.forPattern(Constants.defaultDatePattern)
        formatter.parseDateTime(dateStr)
    }


    def temporalFiltering(targetDate: DateTime): Boolean ={
        val days = math.abs(Days.daysBetween(dateTime.toLocalDate, targetDate.toLocalDate).getDays)
        days < 2
    }

    override def filter(se: Entity, relation: Relation, block: (Int, Int), thetaXY: (Double, Double), partition: Option[MBR]): Boolean = {
        val spatialFilter = super.filter(se, relation, block, thetaXY, partition)
        se match {
            case entity: SpatioTemporalEntity => spatialFilter && temporalFiltering(entity.dateTime)
            case _ => spatialFilter
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
        val mbb = MBR(geometry)

        SpatioTemporalEntity(originalID, geometry, mbb, dateStr)
    }

    def apply(originalID: String, geom: Geometry, dateStr: String): SpatioTemporalEntity ={
        val geometry: Geometry = geom
        val mbb = MBR(geometry)

        SpatioTemporalEntity(originalID, geometry, mbb, dateStr)
    }

}