/*
 * Copyright (c) 2021 Martin Davis.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * and Eclipse Distribution License v. 1.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package org.locationtech.jts.operation.union;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.geom.util.PolygonExtracter;
import org.locationtech.jts.operation.union.CascadedPolygonUnion;

public class PartitionedUnion {
  
  public static Geometry union(Geometry geoms)
  {
    List polys = PolygonExtracter.getPolygons(geoms);
    PartitionedUnion op = new PartitionedUnion(polys);
    return op.union();
  }

  private Geometry[] inputPolys;
  
  public PartitionedUnion(Collection<Geometry> polys)
  {
    this.inputPolys = toArray(polys);
  }
  
  private static Geometry[] toArray(Collection<Geometry> polys) {
    return polys.toArray(new Geometry[0]);
  }
  
  public Geometry union()
  {
    if (inputPolys.length == 0)
      return null;
    
    SpatialPartition part = new SpatialPartition(inputPolys, new SpatialPartition.Relation() {
      
      @Override
      public boolean isEquivalent(int i, int j) {
         //return inputPolys[i1].intersects(inputPolys[i2]);
         /*
         return inputPolys[i1].getEnvelopeInternal()
             .intersects( inputPolys[i2].getEnvelopeInternal() );
         */
         PreparedGeometry pg = PreparedGeometryFactory.prepare(inputPolys[i]);
         return pg.intersects(inputPolys[j]);
      }
    });
    
    //--- compute union of each set
    GeometryFactory geomFactory = inputPolys[0].getFactory();
    List<Geometry> unionGeoms = new ArrayList<Geometry>();
    int numSets = part.getCount();
    for (int i = 0; i < numSets; i++) {
      Geometry geom = union(part, i);
      unionGeoms.add(geom);
    }
    return geomFactory.buildGeometry(unionGeoms);
  }

  private Geometry union(SpatialPartition part, int s) {
    //--- one geom in partition, so just copy it
    if (part.getSize(s) == 1) {
      return part.getGeometry(s, 0).copy();
    }

    List<Geometry> setGeoms = new ArrayList<Geometry>();
    for (int i = 0; i < part.getSize(s); i++) {
      setGeoms.add( part.getGeometry(s, i) );
    }
    return CascadedPolygonUnion.union(setGeoms);
  }
}
