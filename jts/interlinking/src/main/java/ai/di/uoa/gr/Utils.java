package ai.di.uoa.gr;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.IntersectionMatrix;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.locationtech.jts.geom.Geometry.TYPENAME_GEOMETRYCOLLECTION;

public class Utils {

    public static void printIM(List<IntersectionMatrix> matrices, List<Integer[]> indices, List<Geometry> source, List<Geometry> target){
        int intersection = 0;
        int contains = 0;
        int within = 0;
        int covers = 0;
        int coveredBy = 0;
        int touches = 0;
        int crosses = 0;
        int overlap = 0;
        int equals = 0;
        int i = 0;
        for (IntersectionMatrix im : matrices){
            if (im.isIntersects()) intersection += 1;
            if (im.isContains()) contains += 1;
            if (im.isWithin()) within += 1;
            if (im.isCovers()) covers += 1;
            if (im.isCoveredBy()) coveredBy += 1;
            int sDimension = source.get(indices.get(i)[0]).getDimension();
            int tDimension = target.get(indices.get(i)[1]).getDimension();
            if (im.isTouches(sDimension, tDimension)) touches += 1;
            if (im.isCrosses(sDimension, tDimension)) crosses += 1;
            if (im.isOverlaps(sDimension, tDimension)) overlap += 1;
            if (im.isEquals(sDimension, tDimension)) equals += 1;
            i+=1;
        }

        System.out.println("Total Qualified Pairs: " + matrices.size());
        System.out.println("INTERSECTIONS: " + intersection);
        System.out.println("CONTAINS: " + contains);
        System.out.println("WITHIN: " + within);
        System.out.println("COVERS: " + covers);
        System.out.println("COVERED BY: " + coveredBy);
        System.out.println("TOUCHES: " + touches);
        System.out.println("CROSSES: " + crosses);
        System.out.println("OVERLAPS: " + overlap);
        System.out.println("EQUALS: " + equals);

    }

    public static List<Geometry> readCSV(String path, int max,  boolean header) throws IOException, ParseException {
        List<Geometry> geometries = new ArrayList<>();
        int i = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            int indexWKT = 0;
            WKTReader wktReader= new WKTReader();
            while ((line = br.readLine()) != null && (i < max || max < 0)) {
                String[] values = line.split("\t");
                if (header){
                    header = false;
                    List<String> headers = Arrays.asList(values);
                    indexWKT = headers.indexOf("WKT");
                }
                else{
                    String wkt = values[indexWKT].replace("\"", "");
                    Geometry geometry = wktReader.read(wkt);
                    if (!geometry.isEmpty() && geometry.isValid() && !geometry.getGeometryType().equals(TYPENAME_GEOMETRYCOLLECTION))
                        geometries.add(geometry);
                }
                i+=1;
            }
        }
        return geometries;
    }

}
