package ai.di.uoa.gr;

import org.javatuples.Pair;
import org.locationtech.jts.algorithm.LineIntersector;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.IntersectionMatrix;
import org.locationtech.jts.geomgraph.Edge;
import org.locationtech.jts.geomgraph.GeometryGraph;
import org.locationtech.jts.geomgraph.Node;
import org.locationtech.jts.geomgraph.index.MonotoneChain;
import org.locationtech.jts.geomgraph.index.MonotoneChainEdge;
import org.locationtech.jts.geomgraph.index.SweepLineEvent;
import org.locationtech.jts.operation.relate.RelateComputer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class EnhancedGeometry implements Serializable {

    private GeometryGraph geometryGraph;
    private Geometry geom;
    int argIndex;
    private List<SweepLineEvent> events = new ArrayList<>();
    private List <Pair<int[], int[]>> edgesLabels = new ArrayList<>();
    private List <Pair<int[], int[]>> nodesLabels = new ArrayList<>();


    public EnhancedGeometry(Geometry geom, int index, LineIntersector li){
        argIndex = index;
        this.geom = geom;
        geometryGraph = new GeometryGraph(index, geom);
        geometryGraph.computeSelfNodes(li, true);

        for (Object edge : geometryGraph.edges) {
            Edge e = (Edge) edge;
            int[] locations0 = new int[e.label.elt[0].location.length];
            System.arraycopy(e.label.elt[0].location, 0, locations0, 0, e.label.elt[0].location.length);

            int[] locations1 = new int[e.label.elt[1].location.length];
            System.arraycopy(e.label.elt[1].location, 0, locations1, 0, e.label.elt[1].location.length);

            Pair<int[], int[]> pair = new Pair<>(locations0, locations1);
            edgesLabels.add(pair);
        }

        for (Iterator it = geometryGraph.nodes.iterator(); it.hasNext(); ) {
            Object node = it.next();
            Node n = (Node) node;
            int[] locations0 = new int[n.label.elt[0].location.length];
            System.arraycopy(n.label.elt[0].location, 0, locations0, 0, n.label.elt[0].location.length);

            int[] locations1 = new int[n.label.elt[1].location.length];
            System.arraycopy(n.label.elt[1].location, 0, locations1, 0, n.label.elt[1].location.length);
            Pair<int[], int[]> pair = new Pair<>(locations0, locations1);
            nodesLabels.add(pair);
        }
        addEdges(geometryGraph.edges);
    }


    public void clearGraph() {
        geometryGraph.clearGraph();
        int i = 0;
        for (Object edge : geometryGraph.edges) {
            Edge e = (Edge) edge;
            if ( ! Arrays.equals(edgesLabels.get(i).getValue0(), e.label.elt[0].location))
                System.arraycopy(edgesLabels.get(i).getValue0(), 0, e.label.elt[0].location, 0, edgesLabels.get(i).getValue0().length);
            if ( ! Arrays.equals(edgesLabels.get(i).getValue1(), e.label.elt[1].location))
                System.arraycopy(edgesLabels.get(i).getValue1(), 0, e.label.elt[1].location, 0, edgesLabels.get(i).getValue1().length);
            i += 1;
        }

        i = 0;
        for (Iterator it = geometryGraph.nodes.iterator(); it.hasNext(); ) {
            Object node = it.next();
            Node n = (Node) node;
            if ( ! Arrays.equals(nodesLabels.get(i).getValue0(), n.label.elt[0].location))
                System.arraycopy(nodesLabels.get(i).getValue0(), 0, n.label.elt[0].location, 0, nodesLabels.get(i).getValue0().length);
            if ( ! Arrays.equals(nodesLabels.get(i).getValue1(), n.label.elt[1].location))
                System.arraycopy(nodesLabels.get(i).getValue1(), 0, n.label.elt[1].location, 0, nodesLabels.get(i).getValue1().length);
            i += 1;
        }
    }


    private void addEdges(List edges){
        for (Iterator i = edges.iterator(); i.hasNext(); ) {
            Edge edge = (Edge) i.next();
            addEdge(edge, edge);
        }
    }

    private void addEdge(Edge edge, Object edgeSet) {
        MonotoneChainEdge mce = edge.getMonotoneChainEdge();
        int[] startIndex = mce.getStartIndexes();
        for (int i = 0; i < startIndex.length - 1; i++) {
            MonotoneChain mc = new MonotoneChain(mce, i);
            SweepLineEvent insertEvent = new SweepLineEvent(edgeSet, mce.getMinX(i), mc);
            events.add(insertEvent);
            events.add(new SweepLineEvent(mce.getMaxX(i), insertEvent));
        }
    }

    public IntersectionMatrix relate(EnhancedGeometry eg, RelateComputer rc, LineIntersector li){
        clearGraph();
        eg.clearGraph();
        rc.arg = new GeometryGraph[]{geometryGraph, eg.getGeometryGraph()};
        return rc.optimizedComputeIM(events, eg.getEvents(), li);
    }

    public GeometryGraph getGeometryGraph() {
        return geometryGraph;
    }
    public List<SweepLineEvent> getEvents() {
        return events;
    }

    public Geometry getGeometry() {
        return geom;
    }
}
