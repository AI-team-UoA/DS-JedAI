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

package org.locationtech.jtstest.testbuilder.ui;

import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.Transferable;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.io.IOException;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.*;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jtstest.testbuilder.io.IOUtil;

public class GeometryTransferable implements Transferable 
{
  public static final DataFlavor GEOMETRY_FLAVOR =
    new DataFlavor(Geometry.class, "Geometry");
  
  private Geometry geom;
  private boolean isFormatted;
  
  private static final DataFlavor[] flavors = { 
  	DataFlavor.stringFlavor,       
  	GEOMETRY_FLAVOR };

  public GeometryTransferable(Geometry geom) {
    this.geom = geom;
  }

  public GeometryTransferable(Geometry geom, boolean isFormatted) {
    this.geom = geom;
    this.isFormatted = isFormatted;
  }

  public DataFlavor[] getTransferDataFlavors() {
      return flavors;
  }

  public boolean isDataFlavorSupported(DataFlavor flavor) {
      for (int i = 0; i < flavors.length; i++) {
          if (flavor.equals(flavors[i])) {
              return true;
          }
      }
      return false;
  }

  public Object getTransferData(DataFlavor flavor)
      throws UnsupportedFlavorException, IOException
  {
    if (flavor.equals(GEOMETRY_FLAVOR)) {
      return geom;
  }
  if (flavor.equals(DataFlavor.stringFlavor)) {
    return IOUtil.toWKT(geom, isFormatted);
  }
  throw new UnsupportedFlavorException(flavor);

  }
}
