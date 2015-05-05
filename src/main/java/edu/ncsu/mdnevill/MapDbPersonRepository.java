package edu.ncsu.mdnevill;

import org.mapdb.BTreeMap;
import org.mapdb.Bind;
import org.mapdb.DB;
import org.mapdb.Fun;

import java.util.ArrayList;
import java.util.Collection;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Created by losmescaleros on 4/23/15.
 */
public class MapDbPersonRepository {
    BTreeMap<String, Person> bTree;
    NavigableSet<Fun.Tuple2<String, String>> personByFirstName;
    NavigableSet<Fun.Tuple2<Boolean, String>> genderIndex;

    public MapDbPersonRepository(DB db)
    {
        this.bTree = db.getTreeMap("people");
        setMapDbBindings();
    }

    public void setMapDbBindings()
    {
        personByFirstName = new TreeSet<Fun.Tuple2<String, String>>();
        Bind.secondaryKey(bTree, personByFirstName, new Fun.Function2<String, String, Person>() {
            @Override
            public String run(String key, Person value) {
                return value.getFirstName();
            }
        });
        // Create a secondary key to get all people by gender (male = true, female = false)
        genderIndex = new TreeSet<Fun.Tuple2<Boolean, String>>();
        Bind.secondaryKey(bTree, genderIndex, new Fun.Function2<Boolean, String, Person>(){
            @Override
            public Boolean run(String key, Person value){
                return Boolean.valueOf(value.isMale());
            }
        });
    }

    public int size()
    {
        return bTree.size();
    }

    public void add(Person p)
    {
        bTree.put(UUID.randomUUID().toString(), p);
    }

    public Collection<Person> getByFirstName(String firstName)
    {
        Collection<Person> ret = new ArrayList<Person>();

        Iterable<String> ids = Fun.filter(personByFirstName, firstName);
        for(String id : ids){
            ret.add(bTree.get(id));
        }
        return ret;
    }

    public Collection<Person> getByFirstNameRange(String lower, String upper)
    {
        Collection<Person> ret = new ArrayList<Person>();
        ConcurrentNavigableMap<String, Person> sub = bTree.subMap(lower, upper);
        Iterable<String> ids = sub.keySet();
        for(String id : ids){
            ret.add(bTree.get(id));
        }
        return ret;
    }

    public Collection<Person> getByGender(boolean isMale)
    {
        Collection<Person> ret = new ArrayList<Person>();

        Iterable<String> ids = Fun.filter(genderIndex, isMale);
        for(String id : ids){
            ret.add(bTree.get(id));
        }
        return ret;
    }
}
