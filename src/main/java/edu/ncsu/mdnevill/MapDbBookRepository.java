package edu.ncsu.mdnevill;

import org.mapdb.BTreeMap;
import org.mapdb.Bind;
import org.mapdb.DB;
import org.mapdb.Fun;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Created by losmescaleros on 4/24/15.
 */
public class MapDbBookRepository {
    BTreeMap<Integer, Book> bTree;
    BTreeMap<Fun.Tuple2<Integer, Double>, Book> subTree;
    NavigableSet<Fun.Tuple2<String, Integer>> bookByAuthor;
    NavigableSet<Fun.Tuple2<Integer, Integer>> bookByYear;
    NavigableSet<Fun.Tuple2<Double, Integer>> bookByPrice;

    public MapDbBookRepository(DB db)
    {
        this.bTree = db.getTreeMap("books");
        this.subTree = db.getTreeMap("booksByYearAndPrice");
        setMapDbBindings();
    }

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

    public int size()
    {
        return bTree.size();
    }

    public Book get(int id)
    {
        return bTree.get(id);
    }

    public void add(Book b, int id)
    {
        bTree.put(id, b);
        subTree.put(Fun.t2(b.year, b.price), b);
    }

    public void clear()
    {
        bTree.clear();
    }

    public Collection<Book> getByAuthor(String author)
    {
        Collection<Book> ret = new ArrayList<Book>();

        Iterable<Integer> ids = Fun.filter(bookByAuthor, author);
        for(Integer id : ids){
            ret.add(bTree.get(id));
        }
        return ret;
    }

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
