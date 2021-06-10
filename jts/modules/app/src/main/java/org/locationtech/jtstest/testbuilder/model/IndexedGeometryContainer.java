/*
 * Copyright (c) 2016 Vivid Solutions.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * and Eclipse Distribution License v. 1.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */

package org.locationtech.jtstest.testbuilder.model;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jtstest.testbuilder.geom.*;


public class IndexedGeometryContainer
implements GeometryContainer
{
  private GeometryEditModel geomModel;
  private int index;
  
  public IndexedGeometryContainer(GeometryEditModel geomModel, int index) {
    this.geomModel = geomModel;
    this.index = index;
  }

  public Geometry getGeometry()
  {
    return geomModel.getGeometry(index);
  }

}
