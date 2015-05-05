Mitchell Neville

CSC 591 - Capstone Project

# Bitmap vs. B-Tree Indices

# Introduction

Despite the fact that there is much evidence to the contrary, it is still a popular belief that Bitmap Indices should be used primarily on columns with few distinct values. This is evidenced by the fact that Wikipedia’s first few sentences support this idea1, as well as popular Oracle consultants. Recent research, however, indicates that this idea is a myth and that Bitmap Indices are well suited to columns with many distinct values. This project seeks to provide some clarity on the issue by benchmarking Bitmap and B-Tree Indices and comparing and contrasting their performance in a variety of situations. In particular, this project will explore Bitmap and B-Tree Indices by analyzing their performance with a variety of dataset cardinalities and column value ranges. 

To begin the investigation, both Bitmap and B-Tree Indices will be benchmarked with a variety of queries. Both equality and range queries will be performed, as well as a combination of the two. The dataset for this be purely numerical, and queries on string values will not be attempted. In addition, benchmarks will be based on the cardinality of the dataset, so that the number of rows will vary from test to test in order to establish a clear correlation between query types, index types, and dataset cardinality. Rather than implement Bitmap or B-Trees independently, two existing technologies will be adopted instead: [FastBit](https://sdm.lbl.gov/fastbit/) and [MapDB](https://github.com/jankotek/MapDB).

# Setting Up the Environment

This project assumes a working Linux environment with Maven installed. Creating a Maven environment is beyond the scope of this README. In addition, FastBit should be installed on the target machine. Documentation for installing FastBit with its Java JNI can be found [here] (https://github.com/gingi/fastbit/tree/master/java). While installing FastBit, be sure to follow the instructions closely, ensuring that the Java JNI library files are installed properly. 

After installing FastBit, you must link to its JNI. In order to do this, ensure that the environment variable ```LD_LIBRARY_PATH``` is set to the location containing ```libfastbitjni.so```. 

# Running the Application

This is a Maven application, and it requires the dependencies to be built into the Java jar file during compilation to run properly. After extracting the solution, you should navigate to the folder containing the ```pom.xml``` file, then run:

```
$ mvn clean compile assembly:single
```

This will generate the jar file in the ```target``` subfolder, so that you can run the following to perform benchmarks. 

```
$ java -jar target/CSC_591_Capstone-1.0-SNAPSHOT-jar-with-dependencies.jar
```

The benchmark parameters must be changed within the ```App.java``` file. Specifically, if you wish to change the data set cardinality, you should change the following code area in the main class of ```App.java```:

```
initializeDb(booksCsv, 1000);
```

This statement initializes the databases with some number of rows from the specified CSV file, in this case the books CSV file. If you wish to change the FastBit parameters, including binning and encoding parameters, you can do so in the ```initializeDb``` method of ```App.java```. For example, you should change the statement that initializes the ```fb``` variable:

```
fb = new FastBit("<binning none/><encoding equality/>");
```

Acceptable parameters include:

```
<binning nbins=2000/> // Specify the number of bins
<encoding range/> // Specify the encoding type
```

# Interpreting the Results

Upon execution, the application will run the benchmarks with the parameters in ```App.java```. The application will print its various queries along with the time it took to execute, so that output may look similar to the following:

```
Initialized db with 1000 records

Executing equality query on books by year for FastBit and MapDb B-Tree...
Query: 'Select * FROM Books WHERE year = 2001'
FastBit execution:
FastBit got 104 hits in 3 milliseconds
MapDb execution:
MapDb got 104 hits in 5 milliseconds

Executing range query on books by year for FastBit and MapDb B-Tree...
Query: 'Select FROM * Books WHERE year >= 2000'
FastBit execution:
FastBit got 389 hits in 1 milliseconds
MapDb execution:
MapDb got 389 hits in 22 milliseconds

Executing range query on books by year for FastBit and MapDb B-Tree...
Query: 'Select * FROM Books WHERE year = 2000 AND price <= 100.00'
FastBit execution:
FastBit got 55 hits in 7 milliseconds
MapDb execution:
MapDb got 55 hits in 2 milliseconds

```
