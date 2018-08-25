![dbfunctor logo](./dbfunctor.png)
# DBFunctor:  Functional Data Management
## ETL/ELT* Data Processing in Haskell
**DBFunctor** is a [Haskell](https://haskell-lang.org/) library for *ETL/ELT[^1]* data processing of tabular data. What does this mean?
It simply means that whenever you have a ***data analysis*, *data preparation*, or *data transformation* task** and you want to do it with Haskell type-safe code, that you enjoy, love and trust so much, now you can! 
### Main Features
 1. **Julius: An Embedded Domain Specific (EDSL) Language for ETL**
Provides an intuitive type-level Embedded Domain Specific (EDSL) Language called *Julius* for expressing complex data flows (i.e., ETL flows)  but also for performing SQL-like data analysis. For more info check this [Julius tutorial](https://github.com/nkarag/haskell-DBFunctor/blob/master/doc/JULIUS-TUTORIAL.md).
 2. **Supports all known relational operations**
Julius supports all known relational operations (selection, projection, inner/outer join, grouping, ordering, aggregation, set operations etc.)
 3. **Provides the ETL Mapping and other typical ETL constructs**
Julius implements typical ETL constructs such the *Column Mapping* and the *ETL Mapping*.
 4. **Applicable to all kinds of tabular data**
It is applicable to all kinds of "tabular data" (see explanation below)
 5. **In-memory, database-less data processing**
Data transformations or queries can run *in-memory*, within your Haskell code, without the need for a database to process your data.  
6. **Offloading to a database for heavy queries/data transformations**
In addition, a query or data transformation  can be *offloaded to a Database*, when data don't fit in memory, or heavy data processing over large volumes of data is required. The result can be fetched into the client's memory  (i.e., where your haskell code runs) in the `RTable` data structure  (see below), or stored in a database staging table.
 7. **Workflow Operations**
Julius provides common workflow operations. Workflows provide the ability to combine the evaluation of several different Julius Expressions (i.e., data pipelines) in an arbitrary logic. Examples of such operations include:
 - Ability to handle a failure of some operation in a Julius expression:
	 - retry the failed operation (after corrective actions have taken place) and continue the evaluation of the Julius expression from this point onward.
	 - skip the failed operation and move on with the rest operations in the pipeline.
	 - restart the Julius expression from the beginning
	 - terminate the Julius expression and skip all pending operations
 - Ability to start a Julius expression based on the success or failure result of another one
 - Ability to fork several different Julius expressions that will run concurrently
 - Conditional execution of Julius expressions and iteration functionality
 - Workflow hierarchy (i.e., flows, subflows etc.)
 8. **"Declarative ETL"**
Enables *declarative ETL* implementation  in the same sense that SQL is declarative for querying data (see more below).
### Typical examples of DBFunctor use-cases
 - **Build database-less Haskell apps.** Build your data processing haskell apps without the need to import your data in a database for querying functionality or any for executing any data transformations. Analyze your CSV files in-place with plain haskell code (*for Haskellers!*).
 - **Data Preparation.** I.e., clean-up data, calculate derived fields and variables, group by and aggregate etc., in order to feed some machine learning algorithm (*for Data Scientists*).
 - **Data Transformation.** in order to transform data from Data Model A to Data Model B (typical use-case *for Data Engineers* who perform ETL/ELT[^1] tasks for feeding Data Warehouses or Data Marts)
 - **Data Exploration.** Ad hoc data analysis tasks, in order to explore a data set for several purposes such as to find business insights and solve a specific business problem, or maybe to do data profiling in order to evaluate the quality of the data coming from a data source, etc (*for Data Analysts*).
 - **Business Intelligence.** Build reports, or dashboards in order to share business insights with others and drive decision making process (*for BI power-users*)

[^1]:  **ETL** stands for **Extract Transform and Load** and is the standard technology for accomplishing data management tasks in Data Warehouses / Data Marts and in general for preparing data for any analytic purposes (Ad hoc queries, data exploration/data analysis, Reporting and Business Intelligence, feeding Machine Learning algorithms, etc.). **ELT** is a newer variation of ETL and means that the data are first Loaded into their final destination and then the data transformation runs in-place (as opposed to running at a separate staging area on possibly a different server)).

### When to Use it?
DBFunctor should be used whenever a data analysis, or data manipulation, or data transformation task, over *tabular data*, must be performed and we wish to perform it with Haskell code -yielding all the well-known (to Haskellers) benefits from doing that- without the need to use a database query engine for this task.
DBFunctor provides an in-memory data structure called `RTable`, which implements the concept of a *Relational Table* (which -simply put- is a set of tuples) and all relevant relational algebra operations (Selection, Projection, Inner Join, Outer Joins, aggregations, Group By, Set Operations etc.).
Moreover, it implements the concept of *Column Mapping* (for deriving new columns based on existing ones - by splitting , merging , or with any other possible combination using a lambda expression or a function to define the new value) and that of the *ETL Mapping*, which is the equivalent of a "mapping" in an ETL tool (like Informatica, Talend, Oracle Data Integrator, SSIS, Pentaho, etc.). With this powerful construct, one can **build arbitrary complex data pipelines**, which can enable any type of data transformations and all these **by writing Haskell code.**
### What Kinds of Data?
With the term "tabular data" we mean any type of data that can be mapped to an RTable (e.g., CSV (or any other delimiter), DB Table/Query, JSON etc). Essentially, for a Haskell data type `a`to be "tabular", one must implement the following functions:
```haskell
   toRTable :: RTableMData -> a -> RTable
   fromRTable :: RTableMData -> RTable -> a
```    
These two functions implement the "logic" of transforming data type `a` to/from an RTable based on specific RTable Metadata, which specify the column names and data types of the RTable, as well as (optionally) the primary key constraint, and/or alternative unique constraints (i.e., similar information provided with a CREATE TABLE statement in SQL) .
By implementing these two functions, data type `a` essentially becomes an instance of the type `class RTabular` and thus can be transformed with the  DBFunctor package. Currently, we have implemented a CSV data type (any delimeter allowed), based one the [Cassava](https://github.com/haskell-hvr/cassava) library, in order to enable data transformations over CSV files.
### Current Status and Roadmap
Currently (version DBFunctor-0.1.0.0), the DBFunctor package **is stable for in-memory data transformation and queries of CSV files**  (any delimiter allowed), with the **Julius EDSL** (module Etl.Julius) , or directly via RTable functions (module RTable.Core). The use of the Julius language is strongly recommended because it simplifies greatly and standardizes the creation of complex ETL flows.
All in all, currently main features from #1 to #5 (from the list above) have been implemented and main features > #5  are future work that will be released in later versions. 
### Future Vision -> Declarative ETL
Our ultimate goal is, eventually to make DBFunctor the **first *Declarative library for ETL/ELT, or data processing in general***, by exploiting the virtues of functional programming and Haskell strong type system in particular.
Here we use "declarative"  in the same sense that SQL is a declarative language for querying data. (You only have to state what data you want to be returned and you don't care about how this will be accomplished - the DBMS engine does this for you behind the scenes).
In the same manner,  ideally, one should only need to code the desired data transformation from a *source schema* to a *target schema*, as well as all the *data integrity constraints* and *business rules* that should hold after the transformation and not having to define all the individual steps for implementing the transformation, as it is the common practice today. This will yield tremendous benefits compared to common ETL challenges faced today and change the way we build data transformation flows. Just to name a few:
 - Automated ETL coding driven by Source-to-Target mapping and business rules
 - ETL code correctness out-of-the-box
 - Data Integrity / Business Rules controls automatically embedded in your ETL code
 - Self-documented ETL code (Your documentation i.e., the Source-to-Target mapping and the business rules, is also the only code you need to write!)
 - Drastically minimize time-to-market for delivering Data Marts and Data Warehouses, or simply implementing Data Analysis tasks.

The above is inspired by the theoretical work on Categorical Databases by David Spivak,
### Available Modules
*DBFunctor* consists of the following set of Haskell modules:
* **RTable.Core**: Implements the relational Table concept. Defines all necessary data types like `RTable` and `RTuple` as well as basic relational algebra operations on RTables.
* **Etl.Julius**: A simple Embedded DSL for ETL/ELT data processing in Haskell
* **RTable.Data.CSV**:  Implements `RTable` over CSV (TSV, or any other delimiter) files logic. It is based on the [Cassava](https://github.com/haskell-hvr/cassava) library.
### A Very Simple Example
 In this example, we will load a CSV file, turn it into an RTable and then issue a very simple query on it and print the result, just to show the whole concept.
So  lets say we have a CSV file called test-data.csv. The file stores table metadata from an Oracle database. Each row represents a table stored in the database. Here is a small extract from the csv file:

    $ head test-data.csv
        OWNER,TABLE_NAME,TABLESPACE_NAME,STATUS,NUM_ROWS,BLOCKS,LAST_ANALYZED
        APEX_030200,SYS_IOT_OVER_71833,SYSAUX,VALID,0,0,06/08/2012 16:22:36
        APEX_030200,WWV_COLUMN_EXCEPTIONS,SYSAUX,VALID,3,3,06/08/2012 16:22:33
        APEX_030200,WWV_FLOWS,SYSAUX,VALID,10,3,06/08/2012 22:01:21
        APEX_030200,WWV_FLOWS_RESERVED,SYSAUX,VALID,0,0,06/08/2012 16:22:33
        APEX_030200,WWV_FLOW_ACTIVITY_LOG1$,SYSAUX,VALID,1,29,07/20/2012 19:07:57
        APEX_030200,WWV_FLOW_ACTIVITY_LOG2$,SYSAUX,VALID,14,29,07/20/2012 19:07:57
        APEX_030200,WWV_FLOW_ACTIVITY_LOG_NUMBER$,SYSAUX,VALID,1,3,07/20/2012 19:08:00
        APEX_030200,WWV_FLOW_ALTERNATE_CONFIG,SYSAUX,VALID,0,0,06/08/2012 16:22:33
        APEX_030200,WWV_FLOW_ALT_CONFIG_DETAIL,SYSAUX,VALID,0,0,06/08/2012 16:22:33
**1. Turn the CSV file into an RTable**
The first thing we want to do is to read the file and turn it into an RTable. In order to do this we need to define the RTable Metadata, which is the same information one can provide in an SQL CREATE TABLE statement, i,e, column names, column data types and integrity constraints (Primary Key, Unique Key only - no Foreign Keys). So lets see how this is done:     
```haskell
    -- Define table metadata
    src_DBTab_MData :: RTableMData
    src_DBTab_MData =
	    createRTableMData   (   "sourceTab"  -- table name
                                 ,[  ("OWNER", Varchar)                                      -- Owner of the table
                                    ,("TABLE_NAME", Varchar)                                -- Name of the table
                                    ,("TABLESPACE_NAME", Varchar)                           -- Tablespace name
                                    ,("STATUS",Varchar)                                     -- Status of the table object (VALID/IVALID)
                                    ,("NUM_ROWS", Integer)                                  -- Number of rows in the table
                                    ,("BLOCKS", Integer)                                    -- Number of Blocks allocated for this table
                                    ,("LAST_ANALYZED", Timestamp "MM/DD/YYYY HH24:MI:SS")   -- Timestamp of the last time the table was analyzed (i.e., gathered statistics)
                                  ]
                             )
                                ["OWNER", "TABLE_NAME"] -- primary key
                                [] -- (alternative) unique keys
    main :: IO ()
    main = do
       -- read source csv file
       srcCSV <- readCSV "./app/test-data.csv"    
       let
         -- turn source csv to an RTable
         src_DBTab = toRTable src_DBTab_MData srcCSV
    ...
```
We have used the following functions:
```haskell
-- | createRTableMData : creates RTableMData from input given in the form of a list
--   We assume that the column order of the input list defines the fixed column order of the RTuple.
createRTableMData ::
        (RTableName, [(ColumnName, ColumnDType)])
        -> [ColumnName]     -- ^ Primary Key. [] if no PK exists
        -> [[ColumnName]]   -- ^ list of unique keys. [] if no unique keys exists
        -> RTableMData
```
in order to define the RTable metadata.
For reading the CSV file we have used:
```haskell
-- | readCSV: reads a CSV file and returns a CSV data type (Treating CSV data as opaque byte strings)
readCSV ::
    FilePath  -- ^ the CSV file
    -> IO CSV  -- ^ the output CSV type
```
Finally, in order to turn the CSV data type into an RTable, we have used function:
```haskell
toRTable :: RTableMData -> CSV -> RTable
```
which comes from the `RTabular` type class instance of the `CSV` data type.
 **2. Query the RTable**
Once we have created an RTable, we can issue queries on it, or apply any type of data transformations. Note that due to immutability, each query or data transformation creates a new RTable.
 We will now issue the following query:
We return all the rows, which correspond to some filter predicate - in particular all rows where the table_name starts with a 'B'. For this we use the Julius EDSL, in order to express the query and then with the function `juliusToRTable :: ETLMappingExpr -> RTable `, we evaluate the expression into an RTable.
```haskell
tabs_with_B = juliusToRTable $
   EtlMapStart
   :-> (EtlR $
           ROpStart
              -- apply a filter on RTable "src_DBTab" based on a predicate, expressed with a lambda expression
           :. (Filter (From $ Tab src_DBTab) $                 
                   FilterBy (\t ->	let fstChar = Data.Text.take 1 $ fromJust $ toText (t <!> "TABLE_NAME")
						            in fstChar == (pack "B"))
           )
              -- A simple column projection applied on the Previous result
           :. (Select ["OWNER", "TABLE_NAME"] $  From Previous)
   )
```
A Julius expression is a *data processing chain* consisting of various Relational Algebra operations `(EtlR $ ...)`  and/or column mappings `(EtlC $ ...)` connected together via the `:->` data constructor,  of the form (Julius expressions are read *from top-to-bottom  or from left-to-right*):
```haskell
myJulExpression =
	EtlMapStart
	:-> (EtlC $ ...)  -- this is a Column Mapping
	:-> (EtlR $   -- this is a series of Relational Algebra Operations
	     ROpStart
	  :. (Rel Operation 1) -- a relational algebra operation
	  :. (Rel Operation 2))
	:-> (EtlC $ ...)  -- This is another Column Mapping
	:-> (EtlR $ -- more relational algebra operations
	     ROpStart
	  :. (Rel Operation 3)
	  :. (Rel Operation 4)
	  :. (Rel Operation 5))
	:-> (EtlC $ ...) -- This is Column Mapping 3
	:-> (EtlC $ ...) -- This is Column Mapping 4
	...
```
In our example, the Julius expression consists only of two relational algebra operations: a `Filter` operation, which uses an RTuple  predicate of the form 	`RTuple -> Bool` to filter out RTuples (i.e., rows) that dont satisfy this predicate. The predicate is expressed as the lambda expression:
```haskell
FilterBy (\t ->	let fstChar = Data.Text.take 1 $ fromJust $ toText (t <!> "TABLE_NAME")
				in fstChar == (pack "B"))
```
The second relational operation is a simple Projection expressed with the node `(Select ["OWNER", "TABLE_NAME"] $ From Previous)`
Finally, in order to print the result of the query on the screen, we use the `printfRTable :: RTupleFormat -> RTable -> IO()` function, which brings printf-like functionality into the printing of RTables
And here is the output:
```
$ stack exec -- dbfunctor-example
These are the tables that start with a "B":

-------------------------------------
OWNER      TABLE_NAME                
~~~~~      ~~~~~~~~~~                
DBSNMP     BSLN_BASELINES            
DBSNMP     BSLN_METRIC_DEFAULTS      
DBSNMP     BSLN_STATISTICS           
DBSNMP     BSLN_THRESHOLD_PARAMS     
SYS        BOOTSTRAP$                

5 rows returned
-------------------------------------
```
Here is the complete example.
```haskell
module Main where

import  RTable.Core         (RTableMData ,ColumnDType (..) ,createRTableMData, printfRTable, genRTupleFormat, genDefaultColFormatMap, toText, (<!>))
import  RTable.Data.CSV     (CSV, readCSV, toRTable)
import  Etl.Julius          

import Data.Text            (take, pack)
import Data.Maybe           (fromJust)


--  Define Source Schema (i.e., a set of tables)

-- | This is the basic source table
-- It includes the tables of an imaginary database
src_DBTab_MData :: RTableMData
src_DBTab_MData =
    createRTableMData   (   "sourceTab"  -- table name
                            ,[  ("OWNER", Varchar)                                      -- Owner of the table
                                ,("TABLE_NAME", Varchar)                                -- Name of the table
                                ,("TABLESPACE_NAME", Varchar)                           -- Tablespace name
                                ,("STATUS",Varchar)                                     -- Status of the table object (VALID/IVALID)
                                ,("NUM_ROWS", Integer)                                  -- Number of rows in the table
                                ,("BLOCKS", Integer)                                    -- Number of Blocks allocated for this table
                                ,("LAST_ANALYZED", Timestamp "MM/DD/YYYY HH24:MI:SS")   -- Timestamp of the last time the table was analyzed (i.e., gathered statistics)
                            ]
                        )
                        ["OWNER", "TABLE_NAME"] -- primary key
                        [] -- (alternative) unique keys

-- | Define Target Schema (i.e., a set of tables)


main :: IO ()
main = do
    -- read source csv file
    srcCSV <- readCSV "./app/test-data.csv"

    let
        -- create source RTable from source csv
        src_DBTab = toRTable src_DBTab_MData srcCSV      

        -- select all tables starting with a B
        tabs_with_B = juliusToRTable $
                EtlMapStart
                :-> (EtlR $
                        ROpStart                            
                        :. (Filter (From $ Tab src_DBTab) $                 
                                FilterBy (\t ->  let fstChar = Data.Text.take 1 $ fromJust $ toText (t <!> "TABLE_NAME") in fstChar == (pack "B"))
                        )
                        :. (Select ["OWNER", "TABLE_NAME"] $
                            From Previous
                        )
                )

    putStrLn "\nThese are the tables that start with a \"B\":\n"

    -- print source RTable first 100 rows
    printfRTable (  
                    -- this is the equivalent when pinting on the screen to a list of columns in a SELECT clause in SQL
                    genRTupleFormat ["OWNER", "TABLE_NAME"] genDefaultColFormatMap
                 ) $ tabs_with_B
```
### An ETL Example: Create a simple Star-Schema over CSV files (in progress)
In this more comprehensive example, we will show how we can create a simple Star-Schema from a CSV file, which plays the role of our data source. I.e., we will implement a very simple ETL flow with the Julius EDSL. [read more
](https://github.com/nkarag/haskell-DBFunctor/blob/master/doc/ETL-EXAMPLE.md).
### Julius Tutorial (in progress)
https://github.com/nkarag/haskell-DBFunctor/blob/master/doc/JULIUS-TUTORIAL.md
### How to run
Download or clone DBFunctor in a new directory, then run `stack build`
with the [stack](https://docs.haskellstack.org/en/stable/README/) tool.
In order, to use it in you own haskell app, include it in your stack.yaml file, as a local package that you want to link your code to. (Soon DBFunctor will be added to Hackage and you will not need to use it as a local package.)
