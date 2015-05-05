package edu.ncsu.mdnevill;

import org.mapdb.Serializer;
import java.io.*;

/**
 * Created by losmescaleros on 4/22/15.
 */
public class Person implements Serializable{
    final String firstName;
    final String lastName;
    final Integer age;
    final boolean isMale;
    final String city;

    public Person(String fn, String ln, Integer a, boolean m, String c) {
        super();
        this.firstName = fn;
        this.lastName = ln;
        this.age = a;
        this.isMale = m;
        this.city = c;
    }

    public String getFirstName(){
        return this.firstName;
    }

    public String getLastName(){
        return this.lastName;
    }

    public Integer getAge(){
        return this.age;
    }

    public boolean isMale(){
        return this.isMale;
    }

    public String getCity(){
        return this.city;
    }

    @Override
    public boolean equals(Object o){
        if(this == o){
            return true;
        }
        if(o == null || getClass() != o.getClass()){
            return false;
        }

        Person person = (Person) o;

        if(firstName != null ? !firstName.equals(person.firstName) : person.firstName != null){
            return false;
        }
        if(lastName != null ? !lastName.equals(person.lastName) : person.lastName != null){
            return false;
        }
        if(age != person.age){
            return false;
        }
        if(isMale != person.isMale){
            return false;
        }
        if(city != null ? !city.equals(person.city) : person.city != null){
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        String gender = isMale ? "male" : "female";
        StringBuilder sb = new StringBuilder();
        sb.append(this.firstName)
                .append(" ")
                .append(this.lastName)
                .append(" is a ")
                .append(this.age)
                .append(" year old ")
                .append(gender)
                .append(" from ")
                .append(this.city);

        return sb.toString();
    }
}
