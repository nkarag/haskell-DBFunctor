# DBFunctor   
# Functional Data Management
## Type Safe ETL/ELT in Haskell
**DBFunctor** is a library for *Functional Data Management*, or putting it in other words  for implementing *ETL/ELT operations in Haskell* over tabular data. 

*(***ETL*** stands for **Extract Transform and Load** and is the standard technology for accomplishing data management tasks in Data Warehouses / Data Marts and in general for preparing data for any analytic purposes (Ad hoc queries, data exploration/data analysis, Reporting and Business Intelligence, feeding Machine Learning algorithms, etc.). ELT is a newer variation of ETL and means that the data are first Loaded into their final destination and then the data Transformation runs in-place (as opposed to running at a separate staging area on possibly a different server)).*
### When to Use it?
DBFunctor should be used whenever data manipulation/transformation tasks over *tabular data* must be performed and we wish to perform them with Haskell code, yielding all the well-known (to Haskellers) benefits from doing that. 
DBFunctor provides a data structure called *RTable,* which implements the concept of a *Relational Table* (which -simply put- is a set of tuples) and all relevant relational algebra operations (Selection, Projection, Inner Join, Outer Joins, aggregations, Group By, Set Operations etc.). 
Moreover, it implements the concept of the *ETL Mapping*, which is the equivalent of a "mapping" in an ETL tool (like Informatica, Talend, Oracle Data Integrator, SSIS, Pentaho, etc.). With this powerful construct, one can build arbitrary complex data pipelines, which can enable any type of data transformations and all these by writing Haskell code.
### What Kinds of Data?
With the term "tabular data" we mean any type of data that can be mapped to an RTable (e.g., CSV, TSV, DB Table/Query, JSON etc). Essentially, for a Haskell data type `a`to be "tabular", one must implement the following functions:

    toRTable :: RTableMData -> a -> RTable
    fromRTable :: RTableMData -> RTable -> a
These two functions implement the "logic" of transforming data type `a` to/from an RTable based on specific RTable Metadata, which specify the column names and data types of the RTable, as well as (optionally) the primary key constraint, or alternative unique constraints (i.e., similar information provided with a CREATE TABLE statement in SQL) . 
By implementing these two functions, data type `a` essentially becomes an instance of the type class `RTabular` and thus can be transformed with the  DBFunctor package. Currently, we have implemented a CSV data type (based one the [Cassava](https://github.com/haskell-hvr/cassava) library), in order to enable data transformations over CSV files.

### Main Features
 - Enables **Functional Data Management** by implementing the Relational Table concept and exposing all relational algebra operations (select, project, (inner/outer)join, set operations, aggregations, group by etc.) , as well as the "**ETL Mapping**" concept (common to all ETL tools) as Haskell functions and data types.
 - It is applicable to any kind of data that can be presented in a tabular form. This is achieved by a simple Type Class interface.
 - Provides an **Embedded Domain Specific Language (EDSL) for ETL**, called **Julius** , which enables to express complex data transformation flows (i.e., an arbitrary combination of ETL operations) in a more friendly manner (a *Julius Expression*), with  Haskell code (no special language for ETL scripting required - just Haskell).
 - Easy manipulation of your "tabular data" (e.g.,CSV files). Create arbitrary complex  data transformation flows for your tabular data with ease, in the functional way haskellers love.
 - printf -like formatting function, for printing RTables into screen. Ideal for building command-line applications (e.g., a REPL) for manipulating tabular data (for example see [hCSVDB](https://github.com/nkarag/haskell-CSVDB)).
### Current Status
Currently the DBFunctor package is stable for data manipulation of CSV files (any delimiter allowed), with the Julius EDSL , or directly via RTable functions and the ETLMapping data type. The use of the Julius language is strongly recommended because it simplifies greatly and standardizes the creation of complex ETL flows. 
 ### Future Vision
Apart from supporting other "Tabular" data types (e.g., database tables/queries) our ultimate goal is, eventually to make DBFunctor the **first *Declarative library for ETL/ELT***, by exploiting the virtues of functional programming and Haskell strong type system in particular. 
Here we use "declarative"  in the same sense that SQL is a declarative language for querying data. (You only have to state what data you want to be returned and you don't care about how this will be accomplished - the DBMS engine does this for you behind the scenes). 
In the same manner,  ideally, one should only need to code the desired data transformation from a *source schema* to a *target schema*, as well as all the *data integrity constraints* and *business rules* that should hold after the transformation and not having to define all the individual steps for implementing the transformation, as it is the common practice today. This will yield tremendous benefits compared to common ETL challenges faced today and change the way we build data transformation flows. Just to name a few:
 - ETL code correctness out-of-the-box 
 - No data quality errors due to ETL developer mistakes
 - Self-documented ETL code (Your documentation i.e., the Source-to-Target mapping and the business rules, is also the only code you need to write!)
 - Drastically minimize time-to-market for delivering Data Marts and Data Warehouses, or simply implementing Data Analysis tasks.

The above is inspired by the theoretical work on Categorical Databases by David Spivak,
### Available Modules
*DBFunctor* consists of the following set of Haskell libraries:
* **RTable.Core**: A Relational Table Abstraction and relational algebra operations library. Any data type that can be converted to a tabular form can be "viewed" as an RTable. So RTables can be used as an abstraction over many different types of data (CSV, RDBMS tables, JSON, XML, etc) 
* **Etl.Core**: Enhances the RTable library with ETL/ELT operations introducing the *ETL Mapping concept*.
* **Etl.Julius**: An Embedded Domain Specific Language for ETL/ELT
* **Data.CSV**:  It implements a CSV data type that can be converted to an RTable. It is based on the [Cassava](https://github.com/haskell-hvr/cassava) library.
### An ETL Example: Create a simple Star-Schema over CSV files
In this example we will show how we can create a simple Star-Schema from a CSV file, which plays the role of our data source. I.e., we will implement a very simple ETL flow with the Julius EDSL. [read more
](https://github.com/nkarag/DBFunctor/doc/ETL-EXAMPLE.md).
### Concepts
https://github.com/nkarag/DBFunctor/doc/CONCEPTS.md
### Julius Tutorial
https://github.com/nkarag/DBFunctor/doc/JULIUS-TUTORIAL.md
