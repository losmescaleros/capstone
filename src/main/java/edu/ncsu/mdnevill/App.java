package edu.ncsu.mdnevill;

import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.ArrayUtils;
import org.mapdb.*;

import java.io.*;
import org.apache.commons.lang3.RandomStringUtils;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.apache.commons.io.FileUtils;
import java.util.concurrent.ConcurrentNavigableMap;
import gov.lbl.fastbit.*;

import javax.sound.sampled.Line;

/**
 * Created by losmescaleros on 4/21/15.
 */
public class App {
    static DB db;
    static File dbFile;
    static MapDbBookRepository bTreeRepo;
    static final String booksCsv = "data/booksWithPrices.csv";
    static FastBit fb;
    static String fb_dir = "tmp";

    public static void main(String[] args) throws IOException
    {
        dbFile = File.createTempFile("mapdb", "db");
        db = DBMaker.newFileDB(dbFile)
                .closeOnJvmShutdown()
                .make();

        bTreeRepo = new MapDbBookRepository(db);


        initializeDb(booksCsv, 70000);

        booksByYearEqualityQuery();
        booksByYearRangeQuery();
        booksByYearAndPriceRangeQuery();

        /*initializeDb(booksCsv, 5000);
        booksByYearEqualityQuery();
        booksByYearRangeQuery();
        booksByYearAndPriceRangeQuery();*/
    }

    public static void booksByYearEqualityQuery()
    {
        System.out.println();
        System.out.println("Executing equality query on books by year for FastBit and MapDb B-Tree...");
        System.out.println("Query: 'Select * Books WHERE year = 2001'");
        System.out.println("FastBit execution:");
        long start = 0;
        long end = 0;
        start = System.currentTimeMillis();
        // Perform FastBit query
        FastBit.QueryHandle h = fb.build_query(null, fb_dir, "year = 2000");
        int numHits = fb.get_result_size(h);
        int ids[] = fb.get_qualified_ints(h, "id");
        end = System.currentTimeMillis();
        System.out.println("FastBit got " + numHits + " hits in " + (end - start) + " milliseconds");

        // Perform MapDb B-Tree query
        System.out.println("MapDb execution:");
        start = System.currentTimeMillis();
        Collection<Book> books = bTreeRepo.getByYear(2000);
        end = System.currentTimeMillis();
        System.out.println("MapDb got " + books.size() + " hits in " + (end - start) + " milliseconds");
    }

    public static void booksByYearRangeQuery()
    {
        System.out.println();
        System.out.println("Executing range query on books by year for FastBit and MapDb B-Tree...");
        System.out.println("Query: 'Select * Books WHERE year >= 2000'");
        System.out.println("FastBit execution:");
        long start = 0;
        long end = 0;
        start = System.currentTimeMillis();
        // Perform FastBit query
        FastBit.QueryHandle h = fb.build_query(null, fb_dir, "year >= 2000");
        int numHits = fb.get_result_size(h);
        int ids[] = fb.get_qualified_ints(h, "id");
        end = System.currentTimeMillis();
        System.out.println("FastBit got " + numHits + " hits in " + (end - start) + " milliseconds");

        // Perform MapDb B-Tree query
        System.out.println("MapDb execution:");
        start = System.currentTimeMillis();
        Collection<Book> books = bTreeRepo.getByYear(2000, true, 3000, false);
        end = System.currentTimeMillis();
        System.out.println("MapDb got " + books.size() + " hits in " + (end - start) + " milliseconds");
    }

    public static void booksByYearAndPriceRangeQuery()
    {
        System.out.println();
        System.out.println("Executing range query on books by year for FastBit and MapDb B-Tree...");
        System.out.println("Query: 'Select * Books WHERE year = 2000 AND price <= 100.00'");
        System.out.println("FastBit execution:");
        long start = 0;
        long end = 0;
        start = System.currentTimeMillis();
        // Perform FastBit query
        FastBit.QueryHandle h = fb.build_query(null, fb_dir, "year = 2000 and price >= 100.00");
        int numHits = fb.get_result_size(h);
        int ids[] = fb.get_qualified_ints(h, "id");
        end = System.currentTimeMillis();
        System.out.println("FastBit got " + numHits + " hits in " + (end - start) + " milliseconds");

        // Perform MapDb B-Tree query
        System.out.println("MapDb execution:");
        start = System.currentTimeMillis();
        Collection<Book> books = bTreeRepo.getByYearAndPrice(2000, 100.00, true);
        end = System.currentTimeMillis();
        System.out.println("MapDb got " + books.size() + " hits in " + (end - start) + " milliseconds");
    }

    public static void addPrice(String csvFile)
    {
        File inFile = new File(csvFile);
        Random r = new Random(100);
        File outFile = new File("data/booksWithPrices.csv");
        LineIterator it = null;
        Collection<String> lines = new ArrayList<String>();
        try{
            it = FileUtils.lineIterator(inFile, "UTF-8");

            while(it.hasNext()){
                StringBuilder sb = new StringBuilder();

                String line = it.nextLine();

                int dollars = r.nextInt(200);
                int cents = r.nextInt(100);
                double price = dollars + ((double)cents/100);
                sb.append(line);
                sb.append(";\"")
                        .append(price)
                        .append("\"");
                lines.add(sb.toString());
            }
            System.out.println("Initialized db with " + bTreeRepo.size() + " records");
        }
        catch(IOException e){
            System.out.println(e.getMessage());
        }
        finally{
            LineIterator.closeQuietly(it);
        }
        try
        {
            FileUtils.writeLines(outFile, lines);
        }
        catch(IOException e)
        {
            System.out.println(e.getMessage());
        }
    }

    public static void initializeDb(String csvFile, int length)
    {
        File f = new File(fb_dir);
        if(f.exists() && f.isDirectory())
        {
            try{
                FileUtils.deleteDirectory(f);
            }
            catch(IOException e){
                System.out.println(e.getMessage());
            }
        }
        fb = new FastBit("<binning none/><encoding interval-equality/>");
        fb.purge_indexes(fb_dir);
        bTreeRepo.clear();

        boolean getAll = false;
        if(length == -1){
            getAll = true;
        }
        ArrayList<Integer> fbYears = new ArrayList<Integer>();
        ArrayList<Integer> fbIds = new ArrayList<Integer>();
        ArrayList<Double> fbPrices = new ArrayList<Double>();

        File inFile = new File(csvFile);
        LineIterator it = null;
        try{
            it = FileUtils.lineIterator(inFile, "UTF-8");

            int count = 0;
            while(it.hasNext() && (count < length || getAll)){
                count++;
                String line = it.nextLine();
                String[] tokens = line.split("\";\"");

                int year = Integer.parseInt(tokens[3]);
                double price = Double.parseDouble(tokens[8].substring(0, tokens[8].length() - 1));
                String isbn = tokens[0].substring(1);
                fbYears.add(year);
                fbIds.add(count);
                fbPrices.add(price);
                Book b = new Book(tokens[1],
                        tokens[2],
                        tokens[0].substring(1),
                        tokens[4],
                        year,
                        price);

                bTreeRepo.add(b, count);
            }
            System.out.println();
            System.out.println("Initialized db with " + bTreeRepo.size() + " records");
        }
        catch(IOException e){
            System.out.println(e.getMessage());
        }
        finally{
            LineIterator.closeQuietly(it);
        }
        int[] years = ArrayUtils.toPrimitive(fbYears.toArray(new Integer[0]));
        int[] ids = ArrayUtils.toPrimitive(fbIds.toArray(new Integer[0]));
        double[] prices = ArrayUtils.toPrimitive(fbPrices.toArray(new Double[0]));
        fb.add_ints("year", years);
        fb.add_ints("id", ids);
        fb.add_doubles("price", prices);
        fb.write_buffer(fb_dir);
    }
}
