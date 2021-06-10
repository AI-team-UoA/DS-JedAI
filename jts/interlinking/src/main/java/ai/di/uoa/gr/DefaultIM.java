package ai.di.uoa.gr;


import org.apache.commons.cli.*;
import org.javatuples.Pair;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.IntersectionMatrix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import static ai.di.uoa.gr.Utils.printIM;
import static ai.di.uoa.gr.Utils.readCSV;

public class DefaultIM {

    public static void main(String[] args) {

        try {
            long startTime = Calendar.getInstance().getTimeInMillis()/1000;
            Options options = new Options();
            options.addOption("s", true, "path to Source dataset");
            options.addOption("t", true, "path to Target dataset");
            options.addOption("n", true, "max number of geometries");

            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            if (!cmd.hasOption("s")) throw new ParseException("Path to Source dataset is required");
            if (!cmd.hasOption("t")) throw new ParseException("Path to Target dataset is required");

            String sourcePath = cmd.getOptionValue("s");
            String targetPath = cmd.getOptionValue("t");
            int maxGeometries = Integer.parseInt(cmd.getOptionValue("n", "-1"));

            List<Geometry> source = readCSV(sourcePath, maxGeometries, false);
            List<Geometry> target = readCSV(targetPath, maxGeometries, false);

            System.out.println("Source: " + source.size());
            System.out.println("Target: " + target.size());

            computeIM(source, target);

            long endTime = Calendar.getInstance().getTimeInMillis()/1000;
            System.out.println("Overall Time: " + (endTime - startTime));
        }
        catch (org.apache.commons.cli.ParseException pe){
            System.err.println("Wrong input options");
            pe.printStackTrace();
        }
        catch (IOException e){
            System.err.println("Output file not found");
            e.printStackTrace();
        }
        catch (org.locationtech.jts.io.ParseException e) {
            System.err.println("Invalid geometries");
            e.printStackTrace();
        }
    }


    public static void computeIM(List<Geometry> source, List<Geometry> target){
        long imStartTime = Calendar.getInstance().getTimeInMillis()/1000;
        List<IntersectionMatrix> matrices = new ArrayList<>();
        List<Integer[]> indices = new ArrayList<>();

        int i = 0;
        for (Geometry s : source){
            int j = 0;
            for(Geometry t: target){
                if (s.getEnvelopeInternal().intersects(t.getEnvelopeInternal())) {
                    IntersectionMatrix im = s.relate(t);
                    if (!im.isDisjoint()) {
                        matrices.add(im);
                        indices.add(new Integer[]{i, j});
                    }
                }
                j += 1;
            }
            i += 1;
        }
        long imEndTime = Calendar.getInstance().getTimeInMillis()/1000;
        printIM(matrices, indices, source, target);
        System.out.println("IM Time: " + (imEndTime - imStartTime));
    }



}
