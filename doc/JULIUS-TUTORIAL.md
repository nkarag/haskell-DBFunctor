
# Julius EDSL Tutorial
## Table Of Contents

1. [Introduction](#introduction)
2.  [Julius Basic Syntax: The Julius Expression. ](#syntax)
3. [Julius DDL: Creating an RTable from a CSV file](#ddl)
4. [Working with Relational Algebra Operations](#ralgebra)
5. [Column Transformations: The Column Mapping Clause](#colmap)
6. [Creating a Data Flow](#dataflow)
7. [Naming Intermediate Results](#intresults)
8. [Evaluating a Julius Expression](#evaljul)
9. [Printing Results](#print)
10. [Output Result to CSV file](#output) 

<a name="introduction"></a>
## Introduction  
__Julius__ is an *Embedded Domain Specific Language (EDSL)* for ETL/ELT data processing in Haskell.  Julius enables us to express complex data transformation flows (i.e., an arbitrary combination of ETL operations) in a more friendly manner (a __Julius Expression__), with plain Haskell code (no special language for ETL scripting required).
In this tutorial, we will show the basics of Julius in order to help someone to get started.
<a  name="syntax"></a> 
## Julius Basic Syntax: The Julius Expression  
A *Julius Expression* is a sequence of individual *ETL Operations* connected with the `:->` constructor. Each such operation acts on one (Unary), or two (Binary) input `RTable`(s) and produces a new (immutable) `RTable`. 
So, *a Julius Expression is a means to define an arbitrary complex data transformation over RTables, consisting of an arbitrary number of processing steps (aka ETL Operations) . *
In its most basic form a Julius expression looks like this:

    EtlMapStart :-> <etl operation> :-> ... <etl operation>

The `:->` connector is left associative because a Julius expression is evaluated from left to right (or from top to bottom). Essentially `:->` is a data constructor of the `ETLMappingExpr` data type , which receives two input parameters (check out the documentation of the Etl.Julius module in the DBFunctor package in Hackage - just google "Hackage Etl.Julius").
An "etl operation" can be either a *Relational Algebra Expression*, or a *Column Mapping* (i.e,. a column-level transformation). A Relational Algebra is a sequence of relational algebra operations acting on some input RTable(s). A Column Mapping is a transformation a the column-level where one or more new columns are created based on ore or more existing ones.We explain both concepts in the following paragraphs. 
A Relational Algebra expression in Julius EDSL begins with an `EtlR $` notation (after "$" follow the relational algebra operations), while a column mapping begins with `EtlC $`. So, a Julius expression essentially looks like this:
```
EtlMapStart :-> (EtlX $ ...) :-> (EtlX $ ...) :-> ... (EtlX $ ...)
```
where 'X' in "EtlX" is  'C' ,or 'R'.
We will comeback to Julius expressions in more detail, but first lets see how can we create an RTable.
<a  name="ddl"></a> 
## Julius DDL: Creating an RTable from a CSV file
In order to create an RTable from your data (i.e., load your data into memory into the RTable data structure), you only need to call function `toRTable`:

    toRTable :: (RTabular a) => RTableMData -> a -> RTable

`toRTable` is a method of the `RTabluar` type class, which resides in the RTable.Core module of the DBFunctor package. So, if your data are loaded in a data type `a` , which is an instance of the `RTabular` type class, then the only thing you have to do, is to call `toRTable`, in order to get a new RTable loaded with your data. Of course `toRTable` also requires as input the necessary RTable metadata (`RTableMData`), which define the RTable's columns and corresponding data types (similar to an SQL `CREATE TABLE`DDL statement).

For example, assume we have a csv file "mydata.csv" with a data set that we want to convert into an RTable, in order to be able to process it with some Julius expression.
The first step is to read the csv file into some data type `a` that is an instance of the `RTabular` type class. In module, RTable.Data.CSV of the DBfunctor package, we have defined data type `CSV` to represent a CSV file and have made it an instance of `RTabular`. So the only thing you have to do is:
1. Read the csv file from disk into the CSV data type
2. Define the RTable metadata (i.e., its columns and corresponding data types) using function `createRTableMData` exported from the Etl.Julius module of the DBFunctor package.
```
createRTableMData :: 
	(RTableName, [(ColumnName, ColumnDType)])	 
	-> [ColumnName]		--	Primary Key. [] if no PK exists
	-> [[ColumnName]]	--  list of unique keys. [] if no unique keys exists
	-> RTableMData
```
3. Use `toRTable` to convert the CSV into an RTable
These steps are depicted in the following code snippet.
```
import     Etl.Julius
import     RTable.Data.CSV     (CSV, readCSV, writeCSV, toRTable)

main:: IO()
main = do
	-- 1. Read the csv file
	mycsv <- readCSV "mydata.csv"
	-- 3. Transform CSV data type into an RTable data type
	let myRTable = toRTable rtabMdata mycsv
	-- 4. Do stuff with your new RTable
	...
	where
		-- 2. Define the structure of the RTable
		rtabMData :: RTableMData
		rtabMData = createRTableMData (
			"myTab"  -- table name
            ,[	("OWNER", Varchar)  			-- Owner of the table
				,("TABLE_NAME", Varchar)    	-- Name of the table
                ,("TABLESPACE_NAME", Varchar)	-- Tablespace name
                ,("STATUS",Varchar)    			-- Status of the table object (VALID/IVALID)
	            ,("NUM_ROWS", Integer)          -- Number of rows in the table
                ,("BLOCKS", Integer)            -- Number of Blocks allocated for this table
                ,("LAST_ANALYZED", Timestamp "MM_DD_YYYY HH24:MI:SS")   -- Timestamp of the last time the table was analyzed (i.e., gathered statistics)
	        ]
		)
            ["OWNER", "TABLE_NAME"] -- primary key
            [] -- (alternative) unique keys
```
<a name="ralgebra"></a>
## Working with Relational Algebra Operations
Relational Algebra is the algebra behind SQL. Any SQL statement is transformed behind the scenes into an algebraic expression consisting of one or more relational algebra operations that define how we want to process our data. Typical examples of such operations are: Selection (i.e., row filter), Projection, Inner-Join, Outer-Join, Aggregation, Grouping, Ordering, Union, Intersection, Difference etc. Relational algebra operations are applied to sets of tuples (aka relations). 
Julius EDSL supports all common relational algebra operations and applies them onto RTables, which are sets of RTuples. With Julius one can express an arbitrary complex data-processing-operation /query, combining an arbitrary number of relational operations. The means to achieve this is the *Relational Algebra Expression Clause* in a Julius expression, that as we discussed before, it always begins with an:`EtlR $`clause.
Lets see the syntax of a Relational Algebra Expression.
### Relational Algebra Expressions
A relational algebra expression in Julius EDSL is a sequence of relational operation clauses connected with the `:.` connector.  The first operation is always `ROpStart`, which represents an "empty" operation and is there to signify the beginning of the expression. Of course as we have said before the expression must follow an `EtlR $` clause. Here is the typical structure of a relational algebra expression in Julius:
```
EtlMapStart
        :-> (EtlR $
                ROpStart  
                :. (<relational operation 1>)
				:. (<relational operation 2>)
		
		...
				:. (<relational operation N>)
			)

```
Similar to the `:->` connector , `:.`  is left-associative (i.e., evaluated from left to right, or from top to bottom) with a higher precedence (infixl 6) than `:->` which is infixl 5.
### Relational Algebra Operations
In this section we discuss the syntax of each relational algebra operation.
#### SELECTION
**Meaning**: Filter RTuples of an RTable based on some RTuple-level predicate
**Input**: 
- RTable to apply the filter, 
- Predicate on RTuple level

**Output**: New (filtered) RTable

Syntax:
```
<Selection Operation> ::=
					Filter <FromRTable> <ByPred>

<FromRTable>	::=	From <TabExpr>
<TabExpr>		::= Tab <RTable> | Previous 	(* Specify the input RTable, or "Previous" if the filter is to be applied to a previous result, generated by the previous operation within the same expression *)
<RTable>		::= RTable
<ByPred>		::= FilterBy <RPredicate>
<RPredicate>	::= RTuple -> Bool		(* An RPredicate is simply a function of type RTuple -> Bool *)
```
Example
```
juliusToRTable $           
    EtlMapStart
    :-> (EtlR $
            ROpStart  
            :. (Filter (From $ Tab expenses) $ FilterBy myFpred))

myFpred :: RPredicate
myFpred = \t -> t <!> "category" == "FOOD:SUPER_MARKET" 
                && 
                t <!> "amount" > 50.00
```
SQL equivalent
```
SELECT * 
FROM expenses exp 
WHERE   exp.category = 'FOOD:SUPER_MARKET' 
        AND exp.amount > 50.00
```
Notes
1. In order to access the value stored in a specific column in an RTuple, we can use the `<!>` function or the `<!!>` function. The former returns the value, while the latter returns the value wrapped within a Maybe, in the case that the column is not found. (Check out the documentation of the RTable.Core module of the DBFunctor package). Here are the corresponding signatures:
```
(<!>)	:: RTuple -> ColumnName -> RDataType	
(<!!>)	:: RTuple -> ColumnName -> Maybe RDataType	

```
2. Function `juliusToRTable` evaluates a Julius expression and returns an RTable. We will discuss Julius expression evaluation later on.
#### PROJECTION (SELECT)
**Meaning**: ddd
**Input**: 
- ddd
- 
**Output**: ddd
Syntax:
```
<Projection Operation> ::=
```
Example
```
juliusToRTable $           
    EtlMapStart
    :-> (EtlR $
            ROpStart  
            :. (
```
SQL equivalent
```
```
Notes
ddd
#### INNER JOIN 
#### OUTER JOIN
#### AGGREGATION
#### GROUPING (GROUP BY)
#### Grouping on a Derived Column
#### ORDER BY
Ordering
#### Set Operations
### Arbitrary RTable Operation
<a name="colmap"></a>
## Column Transformations: The Column Mapping Clause
<a name="dataflow"></a>
## Creating a Data Flow
<a name="intresults"></a>
## Naming Intermediate Results
<a name="evaljul"></a>
## Evaluating a Julius Expression
<a name="print"></a>
## Printing Results
### Default Formatted Printing
### Formatted Printing
<a name="output"></a>
## Output Result to CSV file


> Written with [StackEdit](https://stackedit.io/).
