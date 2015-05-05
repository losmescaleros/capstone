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
 * @author Mitchell Neville
 * This is a static class that attempts to benchmark MapDB B-Tree Indices and FastBit
 * Bitmap Indices.
 */
public class App {
    // MapDB DB object
    static DB db;
    // Where to save the DB
    static File dbFile;
    // Book repository used for MapDB interactions
    static MapDbBookRepository bTreeRepo;
    // Book data
    static final String booksCsv = "data/booksWithPrices.csv";
    // FastBit instance
    static FastBit fb;
    // Directory to save FastBit indices
    static String fb_dir = "tmp";

    /**
     * Build the databases and indices, then perform various queries to benchmark
     * each index type. The queries will print output to the console, indicating
     * the time it took to execute.
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException
    {
        dbFile = File.createTempFile("mapdb", "db");
        db = DBMaker.newFileDB(dbFile)
                .closeOnJvmShutdown()
                .make();

        bTreeRepo = new MapDbBookRepository(db);

        // Initialize the databases with a dataset. The data/booksWithPrices.csv dataset
        // contains over 70,000 rows. This method call should be changed to change the
        // cardinality of the benchmark we are performing. For example, we might use
        // booksCsv with 1000 rows as: initializeDb(booksCsv, 1000)
        initializeDb(booksCsv, 1000);

        // Execute the various query types
        booksByYearEqualityQuery();
        booksByYearRangeQuery();
        booksByYearAndPriceRangeQuery();
    }

    /**
     * Perform equality queries on MapDB and FastBit. The equality query is supposed to be similar
     * to the SQL statement 'Select * FROM Books WHERE year = 2001'
     */
    public static void booksByYearEqualityQuery()
    {
        System.out.println();
        System.out.println("Executing equality query on books by year for FastBit and MapDb B-Tree...");
        System.out.println("Query: 'Select * FROM Books WHERE year = 2001'");
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

    /**
     * Perform range queries on MapDB and FastBit. The range query is supposed to be similar
     * to the SQL statement 'Select * FROM Books WHERE year >= 2001'
     */
    public static void booksByYearRangeQuery()
    {
        System.out.println();
        System.out.println("Executing range query on books by year for FastBit and MapDb B-Tree...");
        System.out.println("Query: 'Select FROM * Books WHERE year >= 2000'");
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

    /**
     * Perform mixed queries on MapDB and FastBit. The mixed query is supposed to be similar
     * to the SQL statement 'Select * FROM Books WHERE year = 2000 AND price <= 100.00'
     */
    public static void booksByYearAndPriceRangeQuery()
    {
        System.out.println();
        System.out.println("Executing range query on books by year for FastBit and MapDb B-Tree...");
        System.out.println("Query: 'Select * FROM Books WHERE year = 2000 AND price <= 100.00'");
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

    /**
     * Randomly generate prices between $0.00 and $200.00 for each row of the books.csv file
     * @param csvFile
     */
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

    /**
     * Initialize the databases using the CSV files, indicating how many rows
     * to use
     * @param csvFile
     * @param length
     */
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
        // Change the FastBit options here according to
        // https://sdm.lbl.gov/~kewu/fastbit/doc/indexSpec.html
        // Basic options for no binning and equality encoding: <binning none/><encoding equality/>
        // Binning with a specific number: <binning nbins=2000/>
        // Range encoding: <encoding range/>
        fb = new FastBit("<binning none/><encoding equality/>");
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
        // Read the CSV file and create a new Book for each line. 
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
                // Add the book to the MapDB B-Tree
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
        // Add the FastBit indices and write them
        int[] years = ArrayUtils.toPrimitive(fbYears.toArray(new Integer[0]));
        int[] ids = ArrayUtils.toPrimitive(fbIds.toArray(new Integer[0]));
        double[] prices = ArrayUtils.toPrimitive(fbPrices.toArray(new Double[0]));
        fb.add_ints("year", years);
        fb.add_ints("id", ids);
        fb.add_doubles("price", prices);
        fb.write_buffer(fb_dir);
    }
}
