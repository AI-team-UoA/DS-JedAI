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

package org.locationtech.jtstest.testbuilder;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Vector;

import javax.swing.ImageIcon;
import javax.swing.event.TreeModelListener;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreePath;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jtstest.testbuilder.geom.GeometryUtil;


public class GeometryTreeModel implements TreeModel
{
  public static Comparator SORT_AREA_ASC = new AreaComparator(false);
  public static Comparator SORT_AREA_DESC = new AreaComparator(true);
  public static Comparator SORT_LEN_ASC = new LengthComparator(false);
  public static Comparator SORT_LEN_DESC = new LengthComparator(true);
  
  private Vector<TreeModelListener> treeModelListeners = new Vector<TreeModelListener>();

  private GeometricObjectNode rootGeom;

  public GeometryTreeModel(Geometry geom, int source, Comparator comp)
  {
    rootGeom = GeometryNode.create(geom, new GeometryContext(source, comp));
  }

  // ////////////// TreeModel interface implementation ///////////////////////

  /**
   * Adds a listener for the TreeModelEvent posted after the tree changes.
   */
  public void addTreeModelListener(TreeModelListener l)
  {
    treeModelListeners.addElement(l);
  }

  /**
   * Returns the child of parent at index index in the parent's child array.
   */
  public Object getChild(Object parent, int index)
  {
    GeometricObjectNode gn = (GeometricObjectNode) parent;
    return gn.getChildAt(index);
  }

  /**
   * Returns the number of children of parent.
   */
  public int getChildCount(Object parent)
  {
    GeometricObjectNode gn = (GeometricObjectNode) parent;
    return gn.getChildCount();
  }

  /**
   * Returns the index of child in parent.
   */
  public int getIndexOfChild(Object parent, Object child)
  {
    GeometricObjectNode gn = (GeometricObjectNode) parent;
    return gn.getIndexOfChild((GeometricObjectNode) child);
  }

  /**
   * Returns the root of the tree.
   */
  public Object getRoot()
  {
    return rootGeom;
  }

  /**
   * Returns true if node is a leaf.
   */
  public boolean isLeaf(Object node)
  {
    GeometricObjectNode gn = (GeometricObjectNode) node;
    return gn.isLeaf();
  }

  /**
   * Removes a listener previously added with addTreeModelListener().
   */
  public void removeTreeModelListener(TreeModelListener l)
  {
    treeModelListeners.removeElement(l);
  }

  /**
   * Messaged when the user has altered the value for the item identified by
   * path to newValue. Not used by this model.
   */
  public void valueForPathChanged(TreePath path, Object newValue)
  {
    System.out
        .println("*** valueForPathChanged : " + path + " --> " + newValue);
  }
  
  public static class AreaComparator implements Comparator {

    private int dirFactor;

    public AreaComparator(boolean direction) {
      this.dirFactor = direction ? 1 : -1;
    }
    
    @Override
    public int compare(Object o1, Object o2) {
      double area1 = ((GeometricObjectNode) o1).getGeometry().getArea();
      double area2 = ((GeometricObjectNode) o2).getGeometry().getArea();
      return dirFactor * Double.compare(area1, area2);
    }
  }
  public static class LengthComparator implements Comparator {

    private int dirFactor;

    public LengthComparator(boolean direction) {
      this.dirFactor = direction ? 1 : -1;
    }
    
    @Override
    public int compare(Object o1, Object o2) {
      double area1 = ((GeometricObjectNode) o1).getGeometry().getLength();
      double area2 = ((GeometricObjectNode) o2).getGeometry().getLength();
      return dirFactor * Double.compare(area1, area2);
    }
  }
}

abstract class GeometricObjectNode
{
  protected static String indexString(int index)
  {
    return "[" + index + "]";
  }

  protected static String sizeString(int size)
  {
    return "(" + size + ")";
  }

  protected int index = -1;

  protected String text = "";;

  public GeometricObjectNode(String text)
  {
    this.text = text;
  }

  public void setIndex(int index)
  {
    this.index = index;
  }

  public String getText()
  {
    if (index >= 0) {
      return indexString(index) + " " + text;
    }
    return text;
  }
  
  public abstract ImageIcon getIcon();
  
  public abstract Geometry getGeometry();

  public abstract boolean isLeaf();

  public abstract GeometricObjectNode getChildAt(int index);

  public abstract int getChildCount();

  public abstract int getIndexOfChild(GeometricObjectNode child);

}

class GeometryContext {
  int source = 0;
  private Comparator comp;
  
  GeometryContext(int source) {
    this.source = source;
  }

  public GeometryContext(int source, Comparator comp) {
    this.source = source;
    this.comp = comp;
  }
  
  public Comparator getComparator() {
    return comp;
  }

  public boolean isSorted() {
    return comp != null;
  }
}

abstract class GeometryNode extends GeometricObjectNode
{
  public static GeometryNode create(Geometry geom, GeometryContext context)
  {
    if (geom instanceof GeometryCollection)
      return new GeometryCollectionNode((GeometryCollection) geom, context);
    if (geom instanceof Polygon)
      return new PolygonNode((Polygon) geom, context);
    if (geom instanceof LineString)
      return new LineStringNode((LineString) geom, context);
    if (geom instanceof LinearRing)
      return new LinearRingNode((LinearRing) geom, context);
    if (geom instanceof Point)
      return new PointNode((Point) geom, context);
    return null;
  }

  protected GeometryContext context;
  private boolean isLeaf;
  protected List<GeometricObjectNode> children = null;

  public GeometryNode(Geometry geom, GeometryContext context)
  {
    this(geom, 0, null, context);
  }

  public GeometryNode(Geometry geom, int size, String tag, GeometryContext context)
  {
    super(geometryText(geom, size, tag));
    this.context = context;
    if (geom.isEmpty()) {
      isLeaf = true;
    }
  }

  private static String geometryText(Geometry geom, int size, String tag)
  {
    StringBuilder buf = new StringBuilder();
    if (tag != null && tag.length() > 0) {
      buf.append(tag + " : ");
    }
    buf.append(geom.getGeometryType());
    if (geom.isEmpty()) {
      buf.append(" EMPTY");
    }
    else {
      if (size > 0) {
        buf.append(" " + sizeString(size));
      }
    }
    String metrics = GeometryUtil.metricsSummary(geom);
    if (metrics.length() > 0) {
      buf.append("  -  ");
    }
    buf.append( metrics );
    
    return buf.toString();
  }
  
  public boolean isLeaf()
  {
    return isLeaf;
  }
  
  public ImageIcon getIcon()
  {
    return context.source == 0 ? AppIcons.ICON_POLYGON : AppIcons.ICON_POLYGON_B;
  }

  public GeometricObjectNode getChildAt(int index)
  {
    if (isLeaf)
      return null;
    populateChildren();
    return children.get(index);
  }

  public int getChildCount()
  {
    if (isLeaf)
      return 0;
    populateChildren();
    return children.size();
  }

  public int getIndexOfChild(GeometricObjectNode child)
  {
    if (isLeaf)
      return -1;
    populateChildren();
    return children.indexOf(child);
  }

  /**
   * Lazily creates child nodes
   */
  private void populateChildren()
  {
    // already initialized
    if (children != null)
      return;

    children = new ArrayList<GeometricObjectNode>();
    fillChildren();
  }

  protected abstract void fillChildren();
}


class PolygonNode extends GeometryNode
{
  Polygon poly;

  PolygonNode(Polygon poly, GeometryContext context)
  {
    super(poly, poly.getNumPoints(), null, context);
    this.poly = poly;
  }

  public Geometry getGeometry()
  {
    return poly;
  }

  public ImageIcon getIcon()
  {
    return context.source == 0 ? AppIcons.ICON_POLYGON : AppIcons.ICON_POLYGON_B;
  }

  protected void fillChildren()
  {
    for (int i = 0; i < poly.getNumInteriorRing(); i++) {
      children.add(new LinearRingNode((LinearRing) poly.getInteriorRingN(i),
          "Hole " + i, context));
    }
    if (context.isSorted()) {
      children.sort(context.getComparator());
    }
    children.add(0, new LinearRingNode((LinearRing) poly.getExteriorRing(),
        "Shell", context));
  }

}

class LineStringNode extends GeometryNode
{
  private LineString line;

  public LineStringNode(LineString line, GeometryContext context)
  {
    super(line, line.getNumPoints(), null, context);
    this.line = line;
  }

  public LineStringNode(LineString line, String tag, GeometryContext context)
  {
    super(line, line.getNumPoints(), tag, context);
    this.line = line;
  }

  public ImageIcon getIcon()
  {
    return context.source == 0 ? AppIcons.ICON_LINESTRING : AppIcons.ICON_LINESTRING_B;
  }

  public Geometry getGeometry()
  {
    return line;
  }

  protected void fillChildren()
  {
    populateChildren(line.getCoordinates());
  }

  private void populateChildren(Coordinate[] pt)
  {
    Envelope env = line.getEnvelopeInternal();
    
    
    for (int i = 0; i < pt.length; i++) {
      double dist = Double.NaN;
      if (i < pt.length - 1) dist = pt[i].distance(pt[i + 1]);
      GeometricObjectNode node = CoordinateNode.create(pt[i], i, dist);
      children.add(node);
    }
  }
}

class LinearRingNode extends LineStringNode
{
  public LinearRingNode(LinearRing ring, GeometryContext context)
  {
    super(ring, context);
  }
  public LinearRingNode(LinearRing ring, String tag,
      GeometryContext context) {
    super(ring, tag, context);
  }
  public ImageIcon getIcon()
  {
    return context.source == 0 ? AppIcons.ICON_LINEARRING : AppIcons.ICON_LINEARRING_B;
  }
}

class PointNode extends GeometryNode
{
  Point pt;

  public PointNode(Point p, GeometryContext context)
  {
    super(p, context);
    pt = p;
  }

  public ImageIcon getIcon()
  {
    return context.source == 0 ? AppIcons.ICON_POINT : AppIcons.ICON_POINT_B;
  }

  public Geometry getGeometry()
  {
    return pt;
  }

  protected void fillChildren()
  {
    children.add(CoordinateNode.create(pt.getCoordinate()));
  }
}

class GeometryCollectionNode extends GeometryNode
{
  GeometryCollection coll;

  GeometryCollectionNode(GeometryCollection coll, GeometryContext context)
  {
    super(coll, coll.getNumGeometries(), null, context);
    this.coll = coll;
  }

  public Geometry getGeometry()
  {
    return coll;
  }

  protected void fillChildren()
  {
    for (int i = 0; i < coll.getNumGeometries(); i++) {
      GeometryNode node = create(coll.getGeometryN(i), context);
      node.setIndex(i);
      children.add(node);
    }
    if (context.isSorted()) {
      children.sort(context.getComparator());
    }
  }
  
  public ImageIcon getIcon()
  {
    return context.source == 0 ? AppIcons.ICON_COLLECTION : AppIcons.ICON_COLLECTION_B;
  }


}

/**
 * Coordinate is the only leaf node now, but could be 
 * refactored into a LeafNode class.
 * 
 * @author Martin Davis
 *
 */
class CoordinateNode extends GeometricObjectNode
{
  public static CoordinateNode create(Coordinate p)
  {
    return new CoordinateNode(p);
  }

  public static CoordinateNode create(Coordinate p, int i, double distPrev)
  {
    return new CoordinateNode(p, i, distPrev);
  }

  private static DecimalFormat fmt = new DecimalFormat("0.#################", new DecimalFormatSymbols());
  
  private static String label(Coordinate coord, int i, double distPrev)
  {
    String lbl = fmt.format(coord.x) + "   " + fmt.format(coord.y);
    if (! Double.isNaN(distPrev)) {
      lbl += "  --  dist: " + distPrev;
    }
    return lbl;
  }
  

  Coordinate coord;

  public CoordinateNode(Coordinate coord)
  {
    this(coord, 0, Double.NaN);
  }

  public CoordinateNode(Coordinate coord, int i, double distPrev)
  {
    super(label(coord, i, distPrev));
    this.coord = coord;
    this.index = i;
  }
  public ImageIcon getIcon()
  {
    return AppIcons.ICON_POINT;
  }

  public Geometry getGeometry()
  {
    GeometryFactory geomFact = new GeometryFactory();
    return geomFact.createPoint(coord);
  }

  @Override
  public boolean isLeaf()
  {
    return true;
  }

  @Override
  public GeometricObjectNode getChildAt(int index)
  {
    throw new IllegalStateException("should not be here");
  }

  @Override
  public int getChildCount()
  {
    return 0;
  }

  @Override
  public int getIndexOfChild(GeometricObjectNode child)
  {
    throw new IllegalStateException("should not be here");
  }
}

