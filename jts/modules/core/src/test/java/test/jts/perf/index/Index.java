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

package test.jts.perf.index;


import java.util.List;

import org.locationtech.jts.geom.Envelope;


/**
 * Adapter for different kinds of indexes
 * @version 1.7
 */
public interface Index
{
  void insert(Envelope itemEnv, Object item);
  List query(Envelope searchEnv);
  void finishInserting();
}
