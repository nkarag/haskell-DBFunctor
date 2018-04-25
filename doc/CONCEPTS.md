**DBFunctor - Concepts**
=============

Main Features*
---------
 - Enables **Functional Data Management** by exposing all relational algebra operations, as well as the "**ETL Mapping**" concept (common to all ETL tools) as Haskell functions and data types.
 - Easy manipulation of your CSV files. Create any type of data transformation flows for your CSV files with ease, in the functional way haskellers love.
 - No DB-specific storage (that is why we call it a "thin" database layer). CSV files stay as-is on your disk.
 - Relational Table abstraction over CSV files.
 - Relational Algebra operations over CSV files (select, project, (inner/outer)join, set operations, aggregations, group by etc.)  -  exposed as haskell functions.
 - Perform ETL (Extract Transform Load) over your CSV files via the ETL Mapping concept, which is exposed as a Haskell data type. 
 - **Any data transformation**, no matter how complex it is, can be easily implemented through this ETL Mapping data type.
 - **Embedded Domain Specific Language (EDSL) Julius** for ETL, expressing complex data transformation flows (i.e., an arbitrary combination of relational operations and Column Mappings) in a more friendly manner.
 - SQL interface over CSV files. SQL dialect enhanced with a "functional flavor".
 - REPL with available ETL operations and Relational Algebra Operators and support for SQL queries.
 - Lazy evaluation of data pipelines and data transformations.
 - Composition of ETL operations and Relational Algebra operations becomes trivial via function composition, or through the Julius E-DSL.
 - Parallel processing of large CSV files.
 - Notebook style GUI (e.g., like Apache Zeppelin) over CSV files, implemented in Purescript.

The Main Idea
---------

## The Relational Table Concept (RTable data type)
hCSVDB implements the **Relational Table** concept. Defines all necessary data types like `RTable` and `RTuple` and all the basic relational algebra operations on RTables (filter, projection, join, set operations, group by, etc.). 

An RTable can be created from a CSV file with this simple function:
 `csvToRTable :: RTableMData -> CSV -> RTable`
We provide some necessary RTable metadata, such as the name of the columns and the corresponding  data types and a CSV file and we get in return an RTable data structure. Note that we use the [Cassava library](https://github.com/hvr/cassava) for parsing (decoding/encoding) CSV files.

Once we have defined the RTable data type, it is fairly straightforward to define all the basic relational algebra operations. Some examples are the following:

A. The  filter operation
`runRfilter :: RPredicate -> RTable -> RTable`
where
`type RPredicate = RTuple -> Bool`

Note that *any  function* with a relational tuple (RTuple data type) as input, which returns a Bool can be a predicate. This means that we can have a predicate for our WHERE clause (in SQL parlance) that can be as generic as a function of this type signature can be. This is much more general and powerful than SQL, where the WHERE-clause predicate is restricted to specific expressions.

B. The projection operation
```
runProjection :: 
  [ColumnName] -- ^ list of columns to be included in the final result RTable
  -> RTable
  -> RTable
```

C. The Inner Join operation:
```
    runInnerJoin ::
        RJoinPredicate
        -> RTable
        -> RTable
        -> RTable
```
where
`type  RJoinPredicate = RTuple -> RTuple -> Bool`

Again note, how generic a join predicate can be:  *Any function* that receives to relational tuples as input and returns a Bool (indicating when these two tuples match) can play the role of our join predicate. This is much more general and powerful than SQL, where the join predicate is restricted to specific expressions.
## ETL over RTables
### The Column Mapping
Apart from the typical Relational Algebra operations, we also want to "do ETL" over our CSV files, which means over our RTable abstraction. To this end, we have defined the **Column Mapping** concept. This is an arbitrary (immutable) transformation of an RTable that produces a new RTable (Target) based on a single RTable (Source). We can have:

* 1x1 Column Mappings (i.e., 1 source column is mapped to 1 target column)
* 1xN Column Mappings (i.e., 1 source column is mapped (expanded) to N target columns)
* Nx1 Column Mappings (i.e., N source columns are mapped (merged) into 1 target column)
* NxM Column Mapping (i.e., N source columns are mapped to M target columns)

The target column's values are the result of a Column Transformation function of the form: 
 `type ColXForm = [RDataType] -> [RDataType]`
So the source column's values (stored in a list) are given as inputs to a *column transformation function* and the values of the Target columns are the result of this transformation function (these values are also returned in a list). The column transformation can be as generic as this function definition can be.
The RDataType is the standard data type of an RTable column value. Finally, in a Column Mapping we can optionally remove the source columns and also provide some row filtering predicate.

Next, is an example of a simple Nx1 Column Mapping expressed in the **Julius Embedded DSL** for ETL. In this example, we define a Column Mapping, which when executed (in the context of an ETL Mapping, as explained next) a new RTable will be produced that will be the same as the source RTable "myTable", except that the source columns "Amount" and "DebitCredit" will be removed and in their place, a new column named "AmountSigned" will be produced, based on the "myTransformation" column transformation function. Also no rows filtering will take place. The column transformation puts a sign on the amount value based on the text value of the "DebitCredit" column.  
```
EtlC $ 
  Source ["Amount","DebitCredit"] $ 
  Target ["AmountSigned"] $ 
  By myTransformation (On $ Tab myTable) 
  RemoveSrc $    -- remove source columns
  FilterBy (\rtuple -> True)  -- no rows filtering

myTransformation :: [RDataType] -> [RDataType]
myTransformation [RT.RDouble amount, RT.RText debCred] = 
   case debCred of
      "D" -> [RT.RDouble (-amount)]
      "C" -> [RT.RDouble amount] 
```
### The ETL Mapping
The ETLMapping data type is the equivalent of a *mapping* in an ETL tool. It consists of an arbitrary series of *ETL Operations*  (i.e., a Column Mapping or a Relational Algebra operation) that are applied in a specified order,  to one (Unary) or two (Binary) input RTable(s), in order to produce a final new RTable. This RTable will be the result of this data transformation. Each ETL Operation produces a new RTable, which becomes the input to the next ETL Operation.

In terms of database operations an ETL Mapping is the equivalent of a CREATE TABLE AS SELECT (CTAS) operation in an RDBMS. This means that  anything that can be done in the SELECT part (i.e., column projection, arbitrary column expressions, row filtering, grouping and multiple join operations, etc.),  in order to produce a new table, can be included in an ETL Mapping.  

In the Julius EDSL for ETL, we can express an ETL Mapping consisting of various Column Mappings and Relational Operators like this
(Note: Julius expressions are read *from top-to-bottom  or from left-to-right*):
```
myEtlMapping =
	  EtlMapStart
  :-> (EtlC $ ...)  -- this is Column Mapping 1
  :-> (EtlR $   -- this is a series of Relational Algebra Operations
           ROpStart
        :. (Rel Operation 1)
        :. (Rel Operation 2))
  :-> (EtlC $ ...)  -- This is Column Mapping 2
  :-> (EtlR $
           ROpStart
        :. (Rel Operation 3) 
        :. (Rel Operation 4) 
        :. (Rel Operation 5))
  :-> (EtlC $ ...) -- This is Column Mapping 3
  :-> (EtlC $ ...) -- This is Column Mapping 4
```
##### ETL Mapping Implementation Note
The ETLMapping data type is implemented as a binary tree where the node represents the ETL Operation to be executed and the left branch is another  ETL Mapping, while the right branch is an RTable (that might be empty in the case of a Unary ETLOperation). Execution proceeds from bottom-left to top-right. This is similar in concept to a left-deep join tree. In a Left-Deep ETLOperation tree the "pipe" of ETLOperations comes from the left branches always.
```
 A Left-Deep ETLOperation Tree
 
                              final RTable result
                                    / 
                                 etlOp3 
                              /       \ 
                           etlOp2     rtab2
                          /      \ 
 A leaf-node -->    etlOp1    emptyRTab
                    /       \
              ETLMapEmpty   rtab1
```


## Julius: An Embedded Domain Specific Language for ETL
In order to easily define complex ETL mappings, that implement arbitrary data transformations on RTables, we have implemented an **Embedded Domain Specific Language (EDSL)** for this purpose, called **Julius**.
With Julius we can express any ETL Mapping consisting of an arbitrary number of Column Mappings and Relational Algebra operations. Julius expressions are read *from top-to-bottom  or from left-to-right*. 
Julius expressions when evaluated produce an ETLMapping, which can then be executed and produce the resulting RTable.

Some examples follow next:
### Examples of the Julius EDSL for expressing ETL mappings    
**Simple Relational Operations examples:**
**Filter** => `SELECT * FROM myTable WHERE myFpred`
```
mySimpleFilter = 
          EtlMapStart
          :-> (EtlR $
                 ROpStart  
                 :. (Filter (From $ Tab myTable) $ FilterBy myFpred))
                 
myFpred :: RPredicate
myFpred = undefined
```
 **Projection**  => `SELECT col1, col2, col3 FROM myTable`
```
    mySimpleProjection = 
       EtlMapStart
       :-> (EtlR $ 
              ROpStart
              :. (Select ["col1", "col2", "col3"] $ From $ Tab myTable))
```
**Filter then Projection** => `SELECT col1, col2, col3 FROM myTable WHERE myFpred`
```
    myFilterThenProj = 
       EtlMapStart
       :-> (EtlR $
              ROpStart
              :. (Filter (From $ Tab myTable) $ FilterBy myFpred)
              :. (Select ["col1", "col2", "col3"] $ From $ Tab myTable))
```
**Aggregation**  => `SELECT sum(trgCol) as trgColSum FROM myTable`
```
    myAggregation = 
       EtlMapStart
       :-> (EtlR $ 
              ROpStart
              :. (Agg $ AggOn [Sum "trgCol" $ As "trgColSum"] $ From $ Tab myTable))
```
**Group By**  => `
SELECT col1, col2, col3, sum(col4) as col4Sum, max(col4) as col4Max 
FROM myTable 
GROUP BY col1, col2, col3`
```
myGroupBy = 
   EtlMapStart
   :-> (EtlR $ 
           ROpStart
           :. (GroupBy ["col1", "col2", "col3"] 
                 (AggOn [Sum "col4" $ As "col4Sum", Max "col4" $ As "col4Max"] $
                 From $ Tab myTable) $ 
                 GroupOn myGpred))
 
myGpred :: RGroupPredicate
myGpred = undefined
```
**Inner Join** => `SELECT * FROM myTable1 JOIN myTable2 ON (myJoinPred)`
```
myJoin =
   EtlMapStart
   :-> (EtlR $ 
          ROpStart
          :. (Join (TabL myTable1) (Tab myTable2) $ JoinOn myJoinPred))

myJoinPred :: RJoinPredicate
myJoinPred = undefined
```
**Left Outer Join** => `SELECT * FROM myTable1 LEFT JOIN myTable2 ON (myJoinPred)`
```
myJoin =
   EtlMapStart
   :-> (EtlR $ 
          ROpStart
          :. (LJoin (TabL myTable1) (Tab myTable2) $ JoinOn myJoinPred))

myJoinPred :: RJoinPredicate
myJoinPred = undefined
```
**A 3-way Inner Join** => 
```
    SELECT * 
    FROM myTable 
    	JOIN myTable2 ON (myJoinPred) 
          JOIN myTable3 ON (myJoinPred2) 
             JOIN ON myTable4 ON (myJoinPred4)
```
```
    my3Join =
       EtlMapStart
       :-> (EtlR $
              ROpStart
              :. (Join (TabL myTable) (Tab myTable2) $ JoinOn myJoinPred)
              :. (Join (TabL myTable3) Previous $ JoinOn myJoinPred2)
              :. (Join (TabL myTable4) Previous $ JoinOn myJoinPred3))
```
**Union** => `SELECT * FROM myTable1 UNION SELECT * FROM myTable2`
```
    myUnion = 
       EtlMapStart
       :-> (EtlR $ 
              ROpStart
              :. (Union (TabL myTable1) (Tab myTable2)))              
```
A more general **example of an ETL mapping** including *column mappings* and *relational algebra operations*:
You should read the following Julius expression *from bottom to top*, which results to the following discrete processing steps:
      
 1. A 1x1 column mapping on myTable based on myTransformation, produces
   an output table that is input to the next step 
 2. A filter operation  based on myFpred predicate : `SELECT * FROM <previous result> WHERE
   myFpred`
 3. A group by operation: 
    `SELECT col1, col2, col3, SUM(col4) as col2sum,  MAX(col4) as col4Max FROM previous-result`
 4. A projection operation: `SELECT col1, col2, col3, col4sum FROM previous-result`
 5. A 1x1 column mapping on myTable based on myTransformation
 6. An aggregation: `SELECT SUM(trgCol2) as trgCol2Sum FROM previous-result`
 7. A 1xN column mapping based on myTransformation2
```
myEtlExpr :: ETLMappingExpr
myEtlExpr =
   EtlMapStart
   :-> (EtlC $ 
          Source ["srcCol"] $ Target ["trgCol"] $ 
          By myTransformation (On $ Tab myTable) DontRemoveSrc $ 
          FilterBy myFpred2)         
  :-> (EtlR $
          ROpStart
          :. (Filter (From Previous) $ FilterBy myFpred)
          :. (GroupBy  ["col1", "col2", "col3"] 
               (AggOn [Sum "col4" $ As "col4Sum", Max "col4" $ As "col4Max"] $ 
               From Previous) $ 
               GroupOn myGpred) 
          :. (Select ["col1", "col2", "col3", "col4Sum"] $ From Previous) )
  :-> (EtlC $ 
          Source ["tgrCol"] $ Target ["trgCol2"] $ 
          By myTransformation (On Previous) RemoveSrc $ FilterBy myFpred2)
  :-> (EtlR $ 
          ROpStart
          :. (Agg $ AggOn [Sum "trgCol2" $ As "trgCol2Sum"] $ From Previous))
  :-> (EtlC $ 
          Source ["trgCol2Sum"] $ Target ["newCol1", "newCol2"] $ 
          By myTransformation2 (On Previous) DontRemoveSrc $ FilterBy myFpred2) 
```
The following function evaluates the Julius EDSL expression and produces an ETLMapping

    evalJulius :: ETLMappingExpr -> ETLMapping

 and then we can  run the produced ETL Mapping with the `etl` function:
```
etl :: ETLMapping -> RTable
``` 
 and get the final RTable, which is the result of all the processing steps :

    finalRTable = etl $ evalJulius myEtlExpr
### Naming Intermediate Results in a Julius Expression   
If we want in a long Julius expression to name some intermediate steps (i.e., results) in order to refer to them at a later stage, within the same expression, then we can use the syntax:
```
EtlMapStart
:-> (...)
...
:=> NamedResult "resultA" (EtlC $ ...)      -- or (EtlR $ ...)
:-> (...)
...
:=> NamedResult "resultB" (...)
...
```
The named result corresponds to a prefix Julius expression up to the point of the specific name, including the operation at the name. For example:
```
etlXpression = 
   EtlMapStart
   :-> (EtlC $ ...)                
   :=> NamedResult "myResult" (EtlR $ ...)   -- name intermediate result
   :-> (EtlR $ ... )
   :-> (EtlR $
        ROpStart
        :.  (Minus 
              (TabL $    -- use named result
              juliusToRTable $ takeNamedResult "myResult" etlXpression)                    
              (Previous)))
```
In order to get the prefix expression that matches a specific name (e.g., "myResult" in the above example), we use function 
```
takeNamedResult :: 
        NamedResultName  -- ^ the name of the intermediate result
    ->  ETLMappingExpr   -- ^ input ETLMapping Expression
    ->  ETLMappingExpr   -- ^ output ETLMapping Expression
```
In the example above, we use the returned (prefix) Julius expression from takeNamedResults, which is the following:
```
takeNamedResult "myResult" etlXpression == EtlMapStart
                                           :-> (EtlC $ ...)                
                                           :=> NamedResult "myResult" (EtlR $ ...) 
```
In order, to use it at a later stage in the original Julius expression (etlXpression), so we dont have to repeat redundantly the whole prefix expression.

-----------
(*) hCSVDB is still work in progress.

> Written with [StackEdit](https://stackedit.io/).
