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
package org.locationtech.jts.io;

import java.io.IOException;

/**
 * A interface for classes providing an input stream of bytes.
 * This interface is similar to the Java <code>InputStream</code>,
 * but with a narrower interface to make it easier to implement.
 *
 */
public interface InStream
{
  /**
   * Reads <code>buf.length</code> bytes from the input stream
   * and stores them in the supplied buffer.
   *
   * @param buf the buffer to receive the bytes
   * @return the number of bytes read, or -1 if at end-of-file
   *
   * @throws IOException if an I/O error occurs
   */
  int read(byte[] buf) throws IOException;
}
