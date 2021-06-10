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
package org.locationtech.jtstest.testbuilder.ui.tools;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jtstest.testbuilder.geom.GeometryBoxDeleter;


/**
 * Deletes vertices or components within a selection box from a geometry component
 * @version 1.7
 */
public class DeleteVertexTool extends BoxBandTool {
  private static DeleteVertexTool singleton = null;

  public static DeleteVertexTool getInstance() {
    if (singleton == null)
      singleton = new DeleteVertexTool();
    return singleton;
  }

  private DeleteVertexTool() {
    super();
  }

  protected void gestureFinished() 
  {      
    Envelope env = getBox().getEnvelopeInternal();
    Geometry g = geomModel().getGeometry();
    
    Geometry edit = null;
    
    if (isRightButton()) {
      edit = GeometryBoxDeleter.deleteVertices(g, env);
    }
    else if (isControlKeyDown()) {
      edit = GeometryBoxDeleter.deleteComponents(g, env, true);
    }
    else {
      edit = GeometryBoxDeleter.deleteComponents(g, env, false);
    }
    
    geomModel().setGeometry(edit);
  }


}
