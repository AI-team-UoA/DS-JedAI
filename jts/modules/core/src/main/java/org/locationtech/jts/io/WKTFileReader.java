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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.util.ArrayList;
import java.util.List;

import org.locationtech.jts.geom.Geometry;

/**
 * Reads a sequence of {@link Geometry}s in WKT format 
 * from a text file.
 * The geometries in the file may be separated by any amount
 * of whitespace and newlines.
 * 
 * @author Martin Davis
 *
 */
public class WKTFileReader 
{
	private File file = null;
  private Reader reader;
//  private Reader fileReader = new FileReader(file);
	private WKTReader wktReader;
	private int count = 0;
	private int limit = -1;
	private int offset = 0;
  private boolean isStrictParsing = true;
	
  /**
   * Creates a new <tt>WKTFileReader</tt> given the <tt>File</tt> to read from 
   * and a <tt>WKTReader</tt> to use to parse the geometries.
   * 
   * @param file the <tt>File</tt> to read from
   * @param wktReader the geometry reader to use
   */
	public WKTFileReader(File file, WKTReader wktReader)
	{
		this.file = file;
    this.wktReader = wktReader;
	}
	
  /**
   * Creates a new <tt>WKTFileReader</tt>, given the name of the file to read from.
   * 
   * @param filename the name of the file to read from
   * @param wktReader the geometry reader to use
   */
  public WKTFileReader(String filename, WKTReader wktReader)
  {
    this(new File(filename), wktReader);
  }
  
  /**
   * Creates a new <tt>WKTFileReader</tt>, given a {@link Reader} to read from.
   * 
   * @param reader the reader to read from
   * @param wktReader the geometry reader to use
   */
  public WKTFileReader(Reader reader, WKTReader wktReader)
  {
    this.reader = reader;
    this.wktReader = wktReader;
  }
  
  /**
   * Sets the maximum number of geometries to read.
   * 
   * @param limit the maximum number of geometries to read
   */
  public void setLimit(int limit)
  {
    this.limit = limit;
  }
  
  /**
   * Allows ignoring WKT parse errors 
   * after at least one geometry has been read,
   * to return a partial result.
   * 
   * @param isLenient whether to ignore parse errors
   */
  public void setStrictParsing(boolean isStrict)
  {
    this.isStrictParsing = isStrict;
  }
  
	/**
	 * Sets the number of geometries to skip before storing.
   * 
	 * @param offset the number of geometries to skip
	 */
	public void setOffset(int offset)
	{
		this.offset = offset;
	}
	
	/**
	 * Reads a sequence of geometries.
	 * If an offset is specified, geometries read up to the offset count are skipped.
	 * If a limit is specified, no more than <tt>limit</tt> geometries are read.
	 * 
	 * @return the list of geometries read
	 * @throws IOException if an I/O exception was encountered
	 * @throws ParseException if an error occurred reading a geometry
	 */
	public List read() 
	throws IOException, ParseException 
	{
    // do this here so that constructors don't throw exceptions
    if (file != null)
      reader = new FileReader(file);
    
		count = 0;
		try {
			BufferedReader bufferedReader = new BufferedReader(reader);
			try {
				return read(bufferedReader);
			} finally {
				bufferedReader.close();
			}
		} finally {
			reader.close();
		}
	}
	
  private List read(BufferedReader bufferedReader) 
      throws IOException, ParseException {
    List geoms = new ArrayList();
    try {
      read(bufferedReader, geoms);
    }
    catch (ParseException ex) {
      // throw if strict or error is on first geometry
      if (isStrictParsing || geoms.size() == 0)
        throw ex;
    }
    return geoms;
  }

  private void read(BufferedReader bufferedReader, List geoms) 
      throws IOException, ParseException {
    while (!isAtEndOfFile(bufferedReader) && !isAtLimit(geoms)) {
      Geometry g = wktReader.read(bufferedReader);
      if ( count >= offset )
        geoms.add(g);
      count++;
    }
  }

	private boolean isAtLimit(List geoms)
	{
		if (limit < 0) return false;
		if (geoms.size() < limit) return false;
		return true;
	}
 
  /**
	 * Tests if reader is at EOF, and skips any leading whitespace
	 */
  private boolean isAtEndOfFile(BufferedReader bufferedReader) throws IOException {
    // skip whitespace
    int ch;
    do {
      bufferedReader.mark(1);
      ch = bufferedReader.read();
      // EOF reached
      if (ch < 0) return true;
    } while (Character.isWhitespace(ch));
    bufferedReader.reset();
    
    return false;
  }
	 
}
