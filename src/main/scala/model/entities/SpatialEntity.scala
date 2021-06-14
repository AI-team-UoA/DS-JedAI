package model.entities

import org.locationtech.jts.geom.Geometry

case class SpatialEntity(originalID: String, geometry: Geometry) extends Entity
