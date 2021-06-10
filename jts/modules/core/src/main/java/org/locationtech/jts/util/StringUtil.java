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

package org.locationtech.jts.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;

import org.locationtech.jts.io.OrdinateFormat;
import org.locationtech.jts.io.OrdinateFormat;

/**
 * Utility methods for working with {@link String}s.
 * 
 * @author Martin Davis
 *
 */
public class StringUtil
{
  /**
   * Mimics the the Java SE {@link String#split(String)} method.
   *
   * @param s the string to split.
   * @param separator the separator to use.
   * @return the array of split strings.
   */
 public static String[] split(String s, String separator)
 {
   int separatorlen = separator.length();
   ArrayList tokenList = new ArrayList();
   String tmpString = "" + s;
   int pos = tmpString.indexOf(separator);
   while (pos >= 0) {
     String token = tmpString.substring(0, pos);
     tokenList.add(token);
     tmpString = tmpString.substring(pos + separatorlen);
     pos = tmpString.indexOf(separator);
   }
   if (tmpString.length() > 0)
     tokenList.add(tmpString);
   String[] res = new String[tokenList.size()];
   for (int i = 0; i < res.length; i++) {
     res[i] = (String) tokenList.get(i);
   }
   return res;
 }

 public final static String NEWLINE = System.getProperty("line.separator");

 /**
  *  Returns an throwable's stack trace
  */
 public static String getStackTrace(Throwable t) {
     ByteArrayOutputStream os = new ByteArrayOutputStream();
     PrintStream ps = new PrintStream(os);
     t.printStackTrace(ps);
     return os.toString();
 }

 public static String getStackTrace(Throwable t, int depth) {
     String stackTrace = "";
     StringReader stringReader = new StringReader(getStackTrace(t));
     LineNumberReader lineNumberReader = new LineNumberReader(stringReader);
     for (int i = 0; i < depth; i++) {
         try {
             stackTrace += lineNumberReader.readLine() + NEWLINE;
         } catch (IOException e) {
             Assert.shouldNeverReachHere();
         }
     }
     return stackTrace;
 }

  /**
   * Returns a string representation of the given number,
   * using a format compatible with WKT.
   * 
   * @param d a number
   * @return a string 
   *
   * @deprecated use {@link OrdinateFormat}
   */
  public static String toString(double d) {
    return OrdinateFormat.DEFAULT.format(d);
  }

  public static String spaces(int n)
  {
    return chars(' ', n);
  }
  
  public static String chars(char c, int n)
  {
    char[] ch = new char[n];
    for (int i = 0; i < n; i++) {
      ch[i] = c;
    }
    return new String(ch);
  }
}
