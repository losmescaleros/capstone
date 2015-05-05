package edu.ncsu.mdnevill;

import java.io.Serializable;
import java.text.DecimalFormat;

/**
 * Created by losmescaleros on 4/24/15.
 */
public class Book implements Serializable {
    final String title;
    final String author;
    final String isbn;
    final String publisher;
    final int year;
    final double price;

    public Book(String t, String a, String isbn, String p, int y, double pr)
    {
        this.title = t;
        this.author = a;
        this.isbn = isbn;
        this.publisher = p;
        this.year = y;
        this.price = pr;
    }

    @Override
    public boolean equals(Object o){
        if(this == o){
            return true;
        }
        if(o == null || getClass() != o.getClass()){
            return false;
        }

        Book b = (Book) o;
        if(isbn != null ? !isbn.equals(b.isbn) : b.isbn != null){
            return false;
        }
        if(title != null ? !title.equals(b.title) : b.title != null){
            return false;
        }
        if(author != null ? !author.equals(b.author) : b.author != null){
            return false;
        }
        if(year != b.year){
            return false;
        }

        if(publisher != null ? !publisher.equals(b.publisher) : b.publisher != null){
            return false;
        }
        if(price != b.price){
            return false;
        }

        return true;
    }

    @Override
    public String toString()
    {
        String strPrice = DecimalFormat.getCurrencyInstance().format(price);
        return isbn + ": " + title + " by " + author + " published in " + year + " is " + strPrice;
    }
}
