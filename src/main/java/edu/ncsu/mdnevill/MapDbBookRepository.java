package edu.ncsu.mdnevill;

import org.mapdb.BTreeMap;
import org.mapdb.Bind;
import org.mapdb.DB;
import org.mapdb.Fun;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * @author Mitchell Neville
 * This class is a repository for interacting with a MapDB database of Book
 * objects.
 */
public class MapDbBookRepository {
    // MapDB's B-Tree Index implementation
    BTreeMap<Integer, Book> bTree;
    BTreeMap<Fun.Tuple2<Integer, Double>, Book> subTree;
    // Used for querying Books by author
    NavigableSet<Fun.Tuple2<String, Integer>> bookByAuthor;
    // Used for querying Books by publishing year
    NavigableSet<Fun.Tuple2<Integer, Integer>> bookByYear;
    // Used for querying Books by price
    NavigableSet<Fun.Tuple2<Double, Integer>> bookByPrice;

    /**
     * Create a new repository
     * @param db MapDB DB object
     */
    public MapDbBookRepository(DB db)
    {
        this.bTree = db.getTreeMap("books");
        this.subTree = db.getTreeMap("booksByYearAndPrice");
        setMapDbBindings();
    }

    /**
     * Set the bindings for performing queries on attributes
     */
    public void setMapDbBindings()
    {
        bookByAuthor = new TreeSet<Fun.Tuple2<String, Integer>>();
        Bind.secondaryKey(bTree, bookByAuthor, new Fun.Function2<String, Integer, Book>() {
            @Override
            public String run(Integer key, Book value) {
                return value.author;
            }
        });

        bookByYear = new TreeSet<Fun.Tuple2<Integer, Integer>>();
        Bind.secondaryKey(bTree, bookByYear, new Fun.Function2<Integer, Integer, Book>(){
            @Override
            public Integer run(Integer key, Book value){
                return value.year;
            }
        });
        bookByPrice = new TreeSet<Fun.Tuple2<Double, Integer>>();
        Bind.secondaryKey(bTree, bookByPrice, new Fun.Function2<Double, Integer, Book>(){
            @Override
            public Double run(Integer key, Book value){
                return value.price;
            }
        });
    }

    /**
     * Get the size of the B-Tree
     * @return
     */
    public int size()
    {
        return bTree.size();
    }

    /**
     * Get a book by its ID
     * @param id
     * @return
     */
    public Book get(int id)
    {
        return bTree.get(id);
    }

    /**
     * Add a book to the database, specifying its ID
     * @param b
     * @param id
     */
    public void add(Book b, int id)
    {
        bTree.put(id, b);
        subTree.put(Fun.t2(b.year, b.price), b);
    }

    /**
     * Clear the B-Tree
     */
    public void clear()
    {
        bTree.clear();
    }

    /**
     * Get a book by its author
     * @param author
     * @return
     */
    public Collection<Book> getByAuthor(String author)
    {
        Collection<Book> ret = new ArrayList<Book>();

        Iterable<Integer> ids = Fun.filter(bookByAuthor, author);
        for(Integer id : ids){
            ret.add(bTree.get(id));
        }
        return ret;
    }

    /**
     * Get all books published in a range of years
     * @param start Start year
     * @param startInc Is the start year inclusive?
     * @param end End year
     * @param endInc Is the end year inclusive?
     * @return
     */
    public Collection<Book> getByYear(int start, boolean startInc, int end, boolean endInc)
    {
        Collection<Book> ret = new ArrayList<Book>();
        NavigableSet<Fun.Tuple2<Integer, Integer>> s;

        Iterable<Integer> ids = Fun.filter(bookByYear, start, startInc, end, endInc);

        for(Integer id : ids){
            ret.add(bTree.get(id));
        }
        return ret;
    }

    /**
     * Get all books published after a certain year.
     * @param start Star year
     * @param startInc Is the start year inclusive?
     * @return
     */
    public Collection<Book> getByYear(int start, boolean startInc)
    {
        Collection<Book> ret = new ArrayList<Book>();
        NavigableSet<Fun.Tuple2<Integer, Integer>> s;
        s = bookByYear.tailSet(new Fun.Tuple2<Integer, Integer>(start, null), startInc);


        for(Fun.Tuple2<Integer, Integer> item : s)
        {
            ret.add(bTree.get(item.b));
        }

        return ret;
    }

    /**
     * Get all books published in a specific year
     * @param year
     * @return
     */
    public Collection<Book> getByYear(int year)
    {
        Collection<Book> ret = new ArrayList<Book>();
        NavigableSet<Fun.Tuple2<Integer, Integer>> s;

        Iterable<Integer> ids = Fun.filter(bookByYear, year);

        for(Integer id : ids){
            ret.add(bTree.get(id));
        }
        return ret;
    }

    /**
     * Get all books published in a certain year that are over a certain price
     * @param year Publishing year
     * @param startPrice Start price
     * @param startPriceInc Is the start price inclusive?
     * @return
     */
    public Collection<Book> getByYearAndPrice(int year, double startPrice, boolean startPriceInc)
    {
        Collection<Book> ret = new ArrayList<Book>();
        ConcurrentNavigableMap<Fun.Tuple2<Integer, Double>, Book> sub = subTree.subMap(Fun.t2(year, startPrice), Fun.t2(year, Fun.<Double>HI()));

        for(Book b : sub.values())
        {
            ret.add(b);
        }

        return ret;
    }

    /**
     * Get all books over a certain price
     * @param start Start price
     * @param startInc Is the start price inclusive?
     * @return
     */
    public Collection<Book> getByPrice(double start, boolean startInc)
    {
        Collection<Book> ret = new ArrayList<Book>();
        NavigableSet<Fun.Tuple2<Double, Integer>> s;

        s = bookByPrice.tailSet(new Fun.Tuple2<Double, Integer>(start, null), startInc);

        for(Fun.Tuple2<Double, Integer> item : s)
        {
            ret.add(bTree.get(item.b));
        }

        return ret;
    }



}
