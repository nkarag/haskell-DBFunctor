{-|
Module      : Julius
Description : A simple Embedded DSL for ETL/ELT data processing in Haskell
Copyright   : (c) Nikos Karagiannidis, 2018
                  
License     : BSD3
Maintainer  : nkarag@gmail.com
Stability   : stable
Portability : POSIX

__Julius__ is a type-level /Embedded Domain Specific Language (EDSL)/ for ETL/ELT data processing in Haskell.  
Julius enables us to express complex data transformation flows (i.e., an arbitrary combination of ETL operations) in a more friendly manner (a __Julius Expression__), 
with plain Haskell code (no special language for ETL scripting required). For more information read this [Julius Tutorial] (https://github.com/nkarag/haskell-DBFunctor/blob/master/doc/JULIUS-TUTORIAL.md).

= When to use this module
This module should be used whenever one has "tabular data" (e.g., some CSV files, or any type of data that can be an instance of the 'RTabular'
type class and thus define the 'toRTable' and 'fromRTable' functions) and wants to analyze them in-memory with the well-known relational algebra operations 
(selection, projection, join, groupby, aggregations etc) that lie behind SQL. 
This data analysis takes place within your haskell code, without the need to import the data into a database (database-less 
data processing) and the result can be turned into the original format (e.g., CSV) with a simple call to the 'fromRTable' function.

"Etl.Julius" provides a simple language for expressing all relational algebra operations and arbitrary combinations of them, and thus is a powerful
tool for expressing complex data transfromations in Haskell. Moreover, the Julius language includes a clause for the __Column Mapping__ ('RColMapping') concept, which
is a construct used in ETL tools and enables arbitrary transformations at the column level and the creation of derived columns based on arbitrary expressions on the existing ones. 
Finally, the ad hoc combination of relational operations and Column Mappings, chained in an data transformation flow, implements the concept of the __ETL Mapping__ ('ETLMapping'), 
which is the core data mapping unit in all ETL tools and embeds all the \"ETL-logic\" for loading/creating a single __target__ 'RTable' from a set of __source__ 'RTable's.
It is implemented in the "ETL.Internal.Core" module. For the relational algebra operations, Julius exploits the functions in the "RTable.Core" 
module, which also exports it.

The [Julius EDSL] (https://github.com/nkarag/haskell-DBFunctor/blob/master/doc/JULIUS-TUTORIAL.md) is the recommended method for expressing ETL flows in Haskell, as well as doing any data analysis task within the "DBFunctor" package. "Etl.Julius" is a self-sufficient 
module and imports all neccesary functionality from "RTable.Core" and "Etl.Internal.Core" modules, so a programmer should only import "Etl.Julius" and nothing else, in order to have 
complete functionality.

= Overview
The core data type in the Julius EDSL is the 'ETLMappingExpr'. This data type creates a so-called __Julius Expression__. This Julius expression is 
the \"Haskell equivalent\" to the ETL Mapping concept discussed above. It is evaluated to an 'ETLMapping' (see 'ETLMapping'), which is our data structure 
for the internal representation of the ETL Mapping, with the 'evalJulius' function and from then, evaluated into an 'RTable' (see 'juliusToRTable'), which is the final result of our transformation. 

A /Julius Expression/ is a chain of ETL Operation Expressions ('EtlOpExpr') connected with the ':->' constructor (or with the ':=>' constructor for named result operations - see below for an explanation)
This chain of ETL Operations always starts with the 'EtlMapStart' constructor and is executed from left-to-right, or from top-to-bottom:

@
EtlMapStart :-> \<ETLOpExpr\> :-> \<ETLOpExpr\> :-> ... :-> \<ETLOpExpr\>
-- equivalently
EtlMapStart :-> \<ETLOpExpr\> 
            :-> \<ETLOpExpr\> 
            :-> ... 
            :-> \<ETLOpExpr\>
@

A Named ETL Operation Expression ('NamedMap') is just an ETL Operation with a name, so as to be able to reference this specific step in the chain of ETL Operations. It is
actually __a named intermediate result__, which can reference and use in other parts of our Julius expression. It is similar in notion to a subquery, known as an \INLINE VIEW\,
or better, it is equivalent to the @WITH@ clause in SQL (i.e., also called subquery factoring in SQL parlance)
For example:

@
EtlMapStart :-> \<ETLOpExpr\> 
            :=> NamedResult \"my_intermdt_result\" \<ETLOpExpr\> 
            :-> ... 
            :-> \<ETLOpExpr\>
@

An ETL Operation Expression ('ETLOpExpr') - a.k.a. a __Julius Expression__ - is either a Column Mapping Expression ('ColMappingExpr'), or a Relational Operation Expression ('ROpExpr'). The former is used
in order to express a __Column Mapping__ (i.e., an arbitrary transformation at the column level, with which we can create any derived column based on existing columns, see 'RColMapping') and
the latter is __Relational Operation__ (Selection, Projection, Join, Outer Join, Group By, Order By, Aggregate, or a generic Unary or Binary RTable operation, see 'ROperation')

== Typical Structure of an ETL Program using "Etl.Julius"
@
{-# LANGUAGE OverloadedStrings #-}

import     Etl.Julius
import     RTable.Data.CSV     (CSV, readCSV, writeCSV, toRTable)

-- 1. Define table metadata
-- E.g.,
src_DBTab_MData :: RTableMData
src_DBTab_MData = 
    createRTableMData   (   \"sourceTab\"  -- table name
                            ,[  (\"OWNER\", Varchar)                                      -- Owner of the table
                                ,(\"TABLE_NAME\", Varchar)                                -- Name of the table
                                ,(\"TABLESPACE_NAME\", Varchar)                           -- Tablespace name
                                ,(\"STATUS\",Varchar)                                     -- Status of the table object (VALID/IVALID)
                                ,(\"NUM_ROWS\", Integer)                                  -- Number of rows in the table
                                ,(\"BLOCKS\", Integer)                                    -- Number of Blocks allocated for this table
                                ,(\"LAST_ANALYZED\", Timestamp "MM/DD/YYYY HH24:MI:SS")   -- Timestamp of the last time the table was analyzed (i.e., gathered statistics) 
                            ]
                        )
                        [\"OWNER\", \"TABLE_NAME\"] -- primary key
                        [] -- (alternative) unique keys    

-- Result RTable metadata
result_tab_MData :: RTableMData
result_tab_MData = ...

-- 2. Define your ETL code
-- E.g.,
myEtl :: [RTable] -> [RTable]
myEtl [rtab] = 
    -- 3. Define your Julius Expression(s)
    let jul = 
            EtlMapStart
            :-> (EtlR $
                    ROpStart  
                    :. (...)
            ...
    -- 4. Evaluate Julius to the Result RTable
    in [juliusToRTable jul]

main :: IO ()
main = do
    
    -- 5. read source csv files
    -- E.g.,
    srcCSV <- readCSV \".\/app\/test-data.csv\"

    -- 6. Convert CSV to an RTable and do your ETL
    [resultRTab] <- runETL myETL $ [toRTable src_DBTab_MData srcCSV]

    -- 7. Print your results on screen
    -- E.g.,
    printfRTable (genRTupleFormat [\"OWNER\", \"TABLE_NAME\",\"LAST_ANALYZED\"] genDefaultColFormatMap) $ resultRTab

    -- 8. Save your result to a CSV file
    -- E.g.,
    writeCSV \".\/app\/result-data.csv\" $ 
                    fromRTable result_tab_MData resultRTab
@

[-- 1.] We define the necessary 'RTable' metadata, for each 'RTable' in our program. This is equivalent to a @CREATE TABLE@ ddl clause in SQL.
[-- 2.] Here is where we define our ETL code. We dont want to do our ETL in the main function, so we separate the ETL code into a separate function (@myETL@). In general,
   in our main, we want to follow the pattern:

        * Read your Input (Extract phase)
        * Do your ETL (Transform phase)
        * Write your Output (Load phase)

   This function receives as input a list with all the necessary __Source__ 'RTable's (in our case we have a single item list) and outputs 
   a list with all the resulting (__Target__) 'RTable', after the all the necessary transformation steps have been executed. 
   Of course an ETL code might produce more than one target 'RTable's, e.g., 
   a target schema (in DB parlance) and not just one as in our example. Moreover, the myETL function can be arbitrary complex, depending on the ETL logic that we want to 
   implement in each case. It is essentially the entry point to our ETL implementation

[-- 3.] Our ETL code in general will consist of an arbitrary number of Julius expressions. One can define multiple separate Julius expressions, 
   some of which might depend on others, in order to implement the corresponding ETL logic. 
   Keep in mind that each Julius expression encapsulates the \"transformtioin logic\" for producing a __single target RTable__. This holds, 
   even if the target RTable is an intermediate result in the overall ETL process and not a final result RTable.

   The evaluation of each individual Julius expression must be in conformance with the input-RTable prerequisites of each 
   Julius expression. So, first we must evaluate all the Julius expressions that dont depend on other Julius expressions but only on
   source RTables. Then, we evaluate the Julius expressions that depend on the previous ones and so on.

[-- 4.] In our case our ETL code consists of a single source RTable that produces a single target RTable. The Julius expression is evaluated 
        into an 'RTable' and returned to the caller of the ETL code (in our case this is @main@)
[-- 5.] Here is where we read our input for the ETL. In our case, this is a simple CSV file that we read with the help of the 'readCSV' function.
[-- 6.] We convert our input CSV to an 'RTable', with the 'toRTable' and pass it as input to our ETL code. We execute our ETL code with the 'runETL' function.
[-- 7.] We print our target 'RTable' on screen using the 'printfRTable' function for formatted printed ('Text.Printf.printf' like) of 'RTable's.
[-- 8.] We save our target 'RTable' to a CSV file with the 'fromRTable' function.


== Simple Julius Expression Examples
Note: Julius Expression are read from top to bottom and from left to right.

=== Selection (i.e., Filter)
==== SQL
@
SELECT * 
FROM expenses exp 
WHERE   exp.category = \'FOOD:SUPER_MARKET\' 
        AND exp.amount > 50.00
@
==== Julius
@
juliusToRTable $           
    EtlMapStart
    :-> (EtlR $
            ROpStart  
            :. (Filter (From $ Tab expenses) $ FilterBy myFpred))

myFpred :: RPredicate
myFpred = \\t ->    t \<!\> \"category\" == \"FOOD:SUPER_MARKET\" 
                    && 
                    t \<!\> \"amount\" > 50.00
@

=== Projection 
==== SQL
@
SELECT \"TxTimeStamp\", \"Category\", \"Description\", \"Amount\" 
FROM expenses exp 
WHERE   exp.category = \'FOOD:SUPER_MARKET\' 
        AND exp.amount > 50.00
@
==== Julius
@
juliusToRTable $           
    EtlMapStart
    :-> (EtlR $
            ROpStart  
            :. (Filter (From $ Tab expenses) $ FilterBy myFpred)
            :. (Select [\"TxTimeStamp\", \"Category\",\"Description\", \"Amount\"] $ From Previous))

myFpred :: RPredicate
myFpred = \\t -> t \<!\> \"category\" == \"FOOD:SUPER_MARKET\" 
                && 
                t \<!\> \"amount\" > 50.00
@

=== Sorting (Order By)
==== SQL
@
SELECT \"TxTimeStamp\", \"Category\", \"Description\", \"Amount\" 
FROM expenses exp 
WHERE   exp.category = \'FOOD:SUPER_MARKET\' 
        AND exp.amount > 50.00
ORDER BY \"TxTimeStamp\" DESC        
@
==== Julius
@
juliusToRTable $           
    EtlMapStart
    :-> (EtlR $
            ROpStart  
            :. (Filter (From $ Tab expenses) $ FilterBy myFpred)
            :. (Select [\"TxTimeStamp\", \"Category\",\"Description\", \"Amount\"] $ From Previous)
            :. (OrderBy [(\"TxTimeStamp\", Desc)] $ From Previous))

myFpred :: RPredicate
myFpred = \\t -> t \<!\> \"category\" == \"FOOD:SUPER_MARKET\" 
                && 
                t \<!\> \"amount\" > 50.00
@

=== Grouping and Aggregation
==== SQL
@
SELECT \"Category\",  sum(\"Amount\") AS \"TotalAmount\"
FROM expenses exp 
GROUP BY  \"Category\"
ORDER BY \"TotalAmount\" DESC        
@
==== Julius
@
juliusToRTable $           
    EtlMapStart
    :-> (EtlR $
            ROpStart  
            :. (GroupBy [\"Category\"]  
                        (AggOn [Sum \"Amount\" $ As \"TotalAmount\"] (From $ Tab expenses)) $ 
                        GroupOn  (\t1 t2 -> t1 \<!\> \"Category\" == t2 \<!\> \"Category\")                                                                         
                )
            :. (OrderBy [ (\"TotalAmount\", Desc)] $ From Previous))
@

=== Group By and then Right Outer Join 
First group the expenses table by category of expenses and then do a right outer join with the budget table, in order to pair
the expenses at the category level with the correpsonding budget amount (if there is one). Preserve the rows from the expenses table.

==== SQL
@
WITH exp
as (
    SELECT \"Category\",  sum(\"Amount\") AS \"TotalAmount\"
    FROM expenses
    GROUP BY  \"Category\"
)
SELECT  exp.\"Category\", exp.\"TotalAmount\", bdg.\"YearlyBudget\"
FROM budget bdg RIGHT JOIN exp ON (bdg.\"Category\" = exp.\"Category\")
ORDER BY exp.\"TotalAmount\" DESC        
@
==== Julius
@
juliusToRTable $           
    EtlMapStart
    :-> (EtlR $
            ROpStart  
            :. (GroupBy [\"Category\"]  
                        (AggOn [Sum \"Amount\" $ As \"TotalAmount\"] (From $ Tab expenses)) $ 
                        GroupOn  (\t1 t2 -> t1 \<!\> \"Category\" == t2 \<!\> \"Category\")                                                                         
                )
                -- >>> A Right Outer Join that preserves the Previous result RTuples and joins with the budget table
            :. (RJoin (TabL budget) Previous $ 
                        JoinOn (\tl tr ->                                             
                                    tl \<!\> \"Category\" == tr \<!\> \"Category\")
                                )
                )
            :. (OrderBy [ (\"TotalAmount\", Desc)] $ From Previous))
@

=== A Column Mapping 
We will use the previous result (i.e., a table with expenses and budget amounts per category of expenses), in order to create a derived column
to calculate the residual amount, with the help of a Column Mapping Expression ('ColMappingExpr').

==== SQL
@
WITH exp
as (
    SELECT \"Category\",  sum(\"Amount\") AS \"TotalAmount\"
    FROM expenses
    GROUP BY  \"Category\"
)
SELECT  exp.\"Category\", exp.\"TotalAmount\", bdg.\"YearlyBudget\", bdg.\"YearlyBudget\" - exp.\"TotalAmount\" AS \"ResidualAmount\"
FROM budget bdg RIGHT JOIN exp ON (bdg.\"Category\" = exp.\"Category\")
ORDER BY exp.\"TotalAmount\" DESC        
@
==== Julius
@
juliusToRTable $           
    EtlMapStart
    :-> (EtlR $
            ROpStart  
            :. (GroupBy [\"Category\"]  
                        (AggOn [Sum \"Amount\" $ As \"TotalAmount\"] (From $ Tab expenses)) $ 
                        GroupOn  (\t1 t2 -> t1 \<!\> \"Category\" == t2 \<!\> \"Category\")                                                                         
                )
            :. (RJoin (TabL budget) Previous $ 
                        JoinOn (\tl tr ->                                             
                                    tl \<!\> \"Category\" == tr \<!\> \"Category\")
                                )
                )
            :. (Select [\"Category\",  \"TotalAmount\", \"YearlyBudget\"] $ From Previous)
            :. (OrderBy [ (\"TotalAmount\", Desc)] $ From Previous)
        )
        -- >>> A Column Mapping to create a derived column (\"ResidualAmount\"")
    :-> (EtlC $
            Source [\"TotalAmount\", \"YearlyBudget\"] $
            Target [\"ResidualAmount\"] $
            By (\[totAmount, yearlyBudget] -> [yearlyBudget - totAmount]) 
                (On Previous) DontRemoveSrc $ FilterBy (\t -> True)
        )
@

=== Naming Intermediate Results in a Julius Expression
In the following example, each named result is an autonomous (intermediate) result, that can be accessed directly and we can handle it as a distinct result (i.e.,'RTable').
So we can refer to such a result in a subsequent expression, or we can print this result separately, with the 'printRTable' function etc. Each such named result resembles
a separate subquery in an SQL @WITH@ clause.

==== SQL
@
WITH detailYearTXTab
as (
    SELECT \"TxTimeStamp\", \"Category\", \"Description\", \"Amount\",\"DebitCredit\"
    FROM txTab
    WHERE   to_number(to_char(\"TxTimeStamp\", 'YYYY')) >= yearInput 
            AND 
            to_number(to_char(\"TxTimeStamp\", 'YYYY')) < yearInput + 1
),
expGroupbyCategory
as (
    SELECT \"Category\", sum (\"Amount\") AS \"AmountSpent\"
    FROM detailYearTXTab
    WHERE
        \"DebitCredit\" = \"D\"
    GROUP BY \"Category\"
    ORDER BY 2 DESC
),
revGroupbyCategory
as (
    SELECT \"Category\", sum(\"Amount\") AS \"AmountReceived\"
    FROM detailYearTXTab
    WHERE
        \"DebitCredit\" = \"C\"
    GROUP BY \"Category\"
    ORDER BY 2 DESC
),
ojoinedWithBudget
as(
    SELECT \"Category\", \"AmountSpent\", YearlyBudget\"
    FROM budget bdg RIGHT JOIN expGroupbyCategory exp ON (bdg.\"Category\" = exp.\"Category\")
),
calculatedFields
as(
    SELECT \"Category\", \"AmountSpent\", \"YearlyBudget\", \"YearlyBudget\" - \"AmountSpent\" AS \"ResidualAmount\" 
    FROM ojoinedWithBudget
)
SELECT  *
FROM calculatedFields
@
==== Julius
@
let
    julExpr = 
        -- 1. get detailed transactions of the year
        :=> NamedResult \"detailYearTXtab\" (EtlR $
                ROpStart
                -- keep RTuples only of the specified year
                :. (Filter (From $ Tab txTab) $    
                        FilterBy (\t ->       rtime (t \<!\> \"TxTimeStamp\") >= 
                                                                        RTimestampVal {year = yearInput, month = 1, day = 1, hours24 = 0, minutes = 0, seconds = 0}
                                                                    &&
                                                                        rtime (t \<!\> \"TxTimeStamp\") < 
                                                                        RTimestampVal { year = yearInput + 1, 
                                                                                        month = 1,
                                                                                        day = 1, hours24 = 0, minutes = 0, seconds = 0})
                    )
                -- keep only columns of interest
                :. (Select [\"TxTimeStamp\", \"Category\", \"Description\", \"Amount\",\"DebitCredit\"] $ From Previous)
                :. (OrderBy [(\"TxTimeStamp\", Asc)] $ From Previous)
            )
        -- 2. expenses group by category
        :=> NamedResult \"expGroupbyCategory\" (EtlR $
                ROpStart
                    -- keep only the \"debit\" transactions
                :. (FilterBy    (From Previous) $
                                FilterBy (\t -> t \<!\> \"DebitCredit\" == \"D\")
                    )                
                :. (GroupBy [\"Category\"] 
                            (AggOn [Sum \"Amount\" $ As \"AmountSpent\" ]  $ From Previous) $ 
                            GroupOn (\t1 t2 ->  t1 \<!\> \"Category\" == t2 \<!\> \"Category\")
                    )
                :. (OrderBy [(\"AmountSpent\", Desc)] $ From Previous)
            )
        -- 3. revenues group by category
        :=> NamedResult \"revGroupbyCategory\" (EtlR $
                ROpStart
                    -- keep only the \"credit\" transactions
                :. (FilterBy    (From $ juliusToRTable $ takeNamedResult \"detailYearTXtab\" julExpr) $
                                FilterBy (\t -> t \<!\> \"DebitCredit\" == \"C\")
                    )                
                :. (GroupBy [\"Category\"] 
                            (AggOn [Sum \"Amount\" $ As \"AmountReceived\" ]  $ From Previous) $ 
                            GroupOn (\t1 t2 ->  t1 \<!\> \"Category\" == t2 \<!\> \"Category\")
                    )
                :. (OrderBy [(\"AmountReceived\", Desc)] $ From Previous)
            )
        -- 3. Expenses Group By Category Outer joined with budget info
        :=> NamedResult \"ojoinedWithBudget\" (EtlR $
                ROpStart
                :. (RJoin (TabL budget) (Tab $ juliusToRTable $ takeNamedResult \"expGroupbyCategory\" julExpr) $ 
                            JoinOn (\tl tr ->                                             
                                        tl \<!\> \"Category\" == tr \<!\> \"Category\")
                                    )
                    )
                :. (Select [\"Category\",  \"AmountSpent\", \"YearlyBudget\"] $ From Previous)
                :. (OrderBy [ (\"TotalAmount\", Desc)] $ From Previous)
            )
        -- 4. A Column Mapping to create a derived column (\"ResidualAmount\")
        :=> NamedResult \"calculatedFields\"  (EtlC $
                Source [\"AmountSpent\", \"YearlyBudget\"] $
                Target [\"ResidualAmount\"] $
                By (\[amountSpent, yearlyBudget] -> [yearlyBudget - amountSpent]) 
                    (On Previous) DontRemoveSrc $ FilterBy (\t -> True)
            )

-- 5. Print detail transactions
printRTable $ juliusToRTable $ takeNamedResult \"detailYearTXtab\" julExpr

-- 6. Print Expenses by Category
printRTable $ juliusToRTable $ takeNamedResult \"expGroupbyCategory\" julExpr

-- 7. Print Expenses with Budgeting Info and Residual Amount
printRTable $ juliusToRTable $ takeNamedResult \"calculatedFields\" julExpr

-- equivalently
printRTable $ juliusToRTable julExpr

-- 8. Print Revenues by Category
printRTable $ juliusToRTable $ takeNamedResult \"revGroupbyCategory\" julExpr
@

Explanation of each named result in the above example:

["detailYearTXtab"] We retrieve the detail transactions of the current year only and project the columns of interest. We order result by transaction timestamp.
["expGroupbyCategory"] We filter the previous result ("detailYearTXtab"), in order to get only the transactions corresponding to expenses (@t \<!\> \"DebitCredit\" == \"D\"@)
and then we group by category, in order to get a total amount spent by category of expenses.
["revGroupbyCategory"] We filter the  "detailYearTXtab" result (see the use of the 'takeNamedResult' function in order to access the "detailYearTXtab" intermediate result), 
in order to get only the transactions corresponding to revenues (@t \<!\> \"DebitCredit\" == \"C\"@) and then we group by category, in order to get a total amount received by 
category of revenues.
["ojoinedWithBudget"] We do a right join of the grouped expenses with the budget table. Again note the use of the 'takeNamedResult' function. The preserved 'RTable' is the
grouped expenses ("expGroupbyCategory").
["calculatedFields"] Finally, we use a Column Mapping in order to create a derived column, which holds the residual amount for each category of expenses.

-}

{-# LANGUAGE OverloadedStrings      #-}
-- :set -XOverloadedStrings

--{-# LANGUAGE OverloadedRecordFields #-}
--{-# LANGUAGE  DuplicateRecordFields #-}

module Etl.Julius ( 
    module RTable.Core

     -- * The Julius EDSL Syntax
     -- ** The Julius Expression
    ,ETLMappingExpr (..)
    ,ETLOpExpr (..)
    ,NamedMap (..)
    ,NamedResultName
    -- ** The Column Mapping Clause
    ,ColMappingExpr (..)
    ,ToColumn (..)  
    ,ByFunction (..)
    ,OnRTable (..)  
    ,TabExpr (..)          
    ,RemoveSrcCol (..)    
    ,ByPred (..)    
    ,SetColumns (..)
    -- ** The Relational Operation Clause
    ,ROpExpr(..)    
    ,RelationalOp (..)    
    ,FromRTable (..)
    ,Aggregate (..)    
    ,AggOp (..)    
    ,AsColumn (..)
    ,AggBy (..)    
    ,GroupOnPred (..)    
    ,TabLiteral (..)
    ,TabExprJoin (..)    
    ,ByGenUnaryOperation (..)
    ,ByGenBinaryOperation (..)
    ,IntoClause (..)
    ,InsertSource (..)
    ,ValuesClause (..)
    ,TabSource (..)
    ,MergeInto (..)
    ,MergeSource (..)
    ,MergeMatchCondition (..)
    ,WhenMatched (..)
    ,UpdateColumns (..)
    -- * Julius Expression Evaluation
    ,evalJulius
    ,juliusToRTable
    ,runJulius
    ,eitherRunJulius 
    ,juliusToResult 
    ,runJuliusToResult
    ,eitherRunJuliusToResult
    ,runETL
    ,eitherRunETL   
    ,takeNamedResult
    -- * Various ETL Operations    
    ,addSurrogateKeyJ    
    ,appendRTableJ    
    ,addSurrogateKey    
    ,appendRTable   
    ) where

-- Data.RTable
import RTable.Core
-- Data.RTable.Etl
import Etl.Internal.Core

-- Data.Vector
import Data.Vector as V

import Control.Exception
import Control.DeepSeq as CDS          

-----------------------------------------------------------------
-- Define an Embedded DSL in Haskell' s type system
-----------------------------------------------------------------

-- | An ETL Mapping Expression is a __\"Julius Expression\"__. It is a sequence of individual ETL Operation Expressions. Each
-- such ETL Operation "acts" on some input 'RTable' and produces a new "transformed" output 'RTable'.
-- The ETL Mapping connector ':->' (as well as the ':=>' connector) is left associative because in a 'ETLMappingExpr' operations are evaluated from left to right (or top to bottom)
-- A Named ETL Operation Expression ('NamedMap') is just an ETL Operation with a name, so as to be able to reference this specific step in the chain of ETL Operations. 
-- It is actually a /named intermediate result/, which we can reference and use in other parts of our Julius expression
infixl 5 :->
infixl 5 :=>
data ETLMappingExpr = 
            EtlMapStart
          | ETLMappingExpr :-> ETLOpExpr
          | ETLMappingExpr :=> NamedMap 
       

-- | An ETL Operation Expression is either a Column Mapping Expression ('ColMappingExpr'), or a Relational Operation Expression ('ROpExpr')
data ETLOpExpr = EtlC ColMappingExpr | EtlR ROpExpr

-- | The name of an intermediate result, used as a key for accessing this result via the 'takeNamedResult' function.
type NamedResultName = String

-- | A named intermediate result in a Julius expression ('ETLMappingExpr'), which we can access via the 'takeNamedResult' function.
data NamedMap = NamedResult NamedResultName ETLOpExpr 

-- | A Column Mapping ('RColMapping') is the main ETL/ELT construct for defining a column-level transformation.
-- Essentially with a Column Mapping we can create one or more new (derived) column(s) (/Target Columns/), based on an arbitrary transformation function ('ColXForm')
-- with input parameters any of the existing columns (/Source Columns/).
-- So a 'ColMappingExpr' is either empty, or it defines the source columns, the target columns and the transformation from source to target.
-- Notes: 
-- * If a target-column has the same name with a source-column and a 'DontRemoveSrc', or a 'RemoveSrc' has been specified, then the (target-column, target-value) key-value pair,
-- overwrites the corresponding (source-column, source-value) key-value pair
-- * The returned 'RTable' will include only the 'RTuple's that satisfy the filter predicate specified in the 'FilterBy' clause.
data ColMappingExpr =  Source [ColumnName] ToColumn | ColMappingEmpty

-- | Defines the Target Columns of a Column Mapping Expression ('ColMappingExpr') and the column transformation function ('ColXForm').
data ToColumn =  Target [ColumnName] ByFunction

-- | Defines the column transformation function of a Column Mapping Expression ('ColMappingExpr'), the input 'RTable' that this transformation will take place, an
-- indicator ('RemoveSrcCol') of whether the Source Columns will be removed or not in the new 'RTable' that will be created after the Column Mapping is executed and
-- finally, an 'RTuple' filter predicate ('ByPred') that defines the subset of 'RTuple's that this Column Mapping will be applied to. If it must be applied to all 'RTuple's,
-- then for the last parameter ('ByPred'), we can just provide the following 'RPredicate':
--
-- @ 
-- FilterBy (\\_ -> True) 
-- @
--
data ByFunction = By ColXForm OnRTable RemoveSrcCol ByPred

-- | Defines the 'RTable' that the current operation will be applied to.
data OnRTable = On TabExpr

-- | Indicator of whether the source column(s) in a Column Mapping will be removed or not (used in 'ColMappingExpr')
-- If a target-column has the same name with a source-column and a 'DontRemoveSrc' has been specified, then the (target-column, target-value) key-value pair,
-- overwrites the corresponding (source-column, source-value) key-value pair.
data RemoveSrcCol = RemoveSrc | DontRemoveSrc

-- | An 'RTuple' predicate clause.
data ByPred = FilterBy RPredicate

-- | A Relational Operation Expression ('ROpExpr') is a sequence of one or more Relational Algebra Operations applied on a input 'RTable'.
-- It is a sub-expression within a Julius Expression ('ETLMappingExpr') and we use it whenever we want to apply relational algebra operations on an RTable
-- (which might be the result of previous operations in a Julius Expression). A Julius Expression ('ETLMappingExpr') can contain an arbitrary number of
-- 'ROpExpr's.
-- The relational operation connector ':.' is left associative because in a 'ROpExpr' operations are evaluated from left to right (or top to bottom).
infixl 6 :.
data ROpExpr =             
            ROpStart
        |   ROpExpr :. RelationalOp        


-- @
-- \<RelationalOp\> = 
--                  \"Filter\" \"From\" \<TabExpr\> \<ByPred\>
--             \|    \"Select\" \"\[\" \<ColName\> \{,\<ColName\>\} \"]  \"From\" \<TabExpr\>
--             |    "Agg" <Aggregate>
--             |    "GroupBy" "[" <ColName> {,<ColName>} "]" <Aggregate> "GroupOn" <RGroupPredicate>
--             |    "Join" <TabLiteral> <TabExpr> "JoinOn" <RJoinPredicate>
--             |    "LJoin" <TabLiteral> <TabExpr> "JoinOn" <RJoinPredicate>
--             |    "RJoin" <TabLiteral> <TabExpr> "JoinOn" <RJoinPredicate>
--             |    "FOJoin" <TabLiteral> <TabExpr> "JoinOn" <RJoinPredicate>
--             |    <TabLiteral> "`Intersect`" <TabExpr> 
--             |    <TabLiteral> "`Union`" <TabExpr> 
--             |    <TabLiteral> "`Minus`" <TabExpr> 
--             |    "GenUnaryOp" "On" <TabExpr> "ByUnaryOp" <UnaryRTableOperation> 
--             |    "GenBinaryOp" <TabLiteral>  <TabExpr>  "ByBinaryOp" <BinaryRTableOperation>
--             |    "OrderBy"  "[" "("<ColName> "," <OrderingSpec> ")" {,"("<ColName> "," <OrderingSpec> ")"} "]"
-- @


-- | The Relational Operation ('RelationalOp') is a Julius clause that represents a __Relational Algebra Operation__. 
data RelationalOp =  
            Filter FromRTable ByPred                        -- ^ 'RTuple' filtering clause (selection operation), based on an arbitrary predicate function ('RPredicate')
        |   Select [ColumnName] FromRTable                  -- ^ Column projection clause
        |   Agg Aggregate                                   -- ^ Aggregate Operation clause
        |   GroupBy [ColumnName] Aggregate GroupOnPred      -- ^ Group By clause, based on an arbitrary Grouping predicate function ('RGroupPredicate')
        |   Join TabLiteral TabExpr TabExprJoin             -- ^ Inner Join clause, based on an arbitrary join predicate function - not just equi-join - ('RJoinPredicate')
        |   LJoin TabLiteral TabExpr TabExprJoin            -- ^ Left Join clause, based on an arbitrary join predicate function - not just equi-join - ('RJoinPredicate')
        |   RJoin TabLiteral TabExpr TabExprJoin            -- ^ Right Join clause, based on an arbitrary join predicate function - not just equi-join - ('RJoinPredicate')
        |   FOJoin TabLiteral TabExpr TabExprJoin           -- ^ Full Outer Join clause, based on an arbitrary join predicate function - not just equi-join - ('RJoinPredicate')
        |   SemiJoin TabLiteral TabExpr TabExprJoin         
            -- ^ Implements the semi-Join operation between two RTables (any type of join predicate is allowed)
            -- It returns the 'RTuple's from the left 'RTable' that match with the right 'RTable'.
            -- Note that if an 'RTuple' from the left 'RTable' matches more than one 'RTuple's from the right 'RTable'
            -- the semi join operation will return only a single 'RTuple'.    
        |   SemiJoinP TabExpr TabLiteral TabExprJoin        --  ^ This is a semi-Join operation to be used when the left table must be the 'Previous' value. 
        |   AntiJoin TabLiteral TabExpr TabExprJoin         
            -- ^ Implements the anti-Join operation between two RTables (any type of join predicate is allowed)
            -- It returns the 'RTuple's from the left 'RTable' that DONT match with the right 'RTable'.
        |   AntiJoinP TabExpr TabLiteral TabExprJoin        --  ^ This is a anti-Join operation to be used when the left table must be the 'Previous' value. 
 
        |   TabLiteral `Intersect` TabExpr                  -- ^ Intersection clause
        |   TabLiteral `Union` TabExpr                      -- ^ Union clause. Note this operation eliminates dublicate 'RTuples'
        |   TabLiteral `UnionAll` TabExpr                   -- ^ Union All clause. It is a Union operation without dublicate 'RTuple' elimination.
        |   TabLiteral `Minus` TabExpr                      -- ^ Minus clause (set Difference operation)
        |   TabExpr `MinusP` TabLiteral                     --  ^ This is a Minus operation to be used when the left table must be the 'Previous' value.
        |   GenUnaryOp OnRTable ByGenUnaryOperation         -- ^ This is a generic unary operation on a RTable ('UnaryRTableOperation'). It is used to define an arbitrary unary operation on an 'RTable'
        |   GenBinaryOp TabLiteral TabExpr ByGenBinaryOperation -- ^ This is a generic binary operation on a RTable ('BinaryRTableOperation'). It is used to define an arbitrary binary operation on an 'RTable'
        |   OrderBy [(ColumnName, OrderingSpec)] FromRTable     -- ^ Order By clause.
        
        {--  DML Section  -}
        |   Update TabExpr SetColumns ByPred
            -- ^ Update an 'RTable'. 
            -- Please note that this is an __immutable__ implementation of an 'RTable' update. This simply means that
            -- the update operation returns a new 'RTable' that includes all the 'RTuple's of the original 'RTable', both the ones
            -- that have been updated and the others that have not. So, the original 'RTable' remains unchanged and no update in-place
            -- takes place whatsoever.
            -- Moreover, if we have multiple threads updating an 'RTable', due to immutability, each thread \"sees\" its own copy of
            -- the 'RTable' and thus there is no need for locking the updated 'RTuple's, as happens in a common RDBMS.

        |   Insert IntoClause
           -- ^ Insert Operation. It can insert into an 'RTable' a single 'RTuple' or a whole 'RTable'. The latter is the equivalent
            -- of an @INSERT INTO SELECT@ clause in SQL. Since, an 'RTable' can be the result of a Julius expression (playing the
            -- role of a subquery within the Insert clause, in this case).
            -- Please note that this is an __immutable__ implementation of an 'RTable' insert.
            -- This simply means that the insert operation returns a new 'RTable' and does not
            -- affect the original 'RTable'. 
            -- Also note that the source and target 'RTable's should have the same structure.
            -- By \"structure\", we mean that the 'ColumnName's and the corresponding data types must match. Essentially what we record in the 'ColumnInfo'
            -- must be the same for the two 'RTable's. Otherwise a 'ConflictingRTableStructures' exception will be thrown.

        |   Upsert MergeInto                                
            -- ^ Upsert (Update+Insert, aka Merge) Operation. We provide a source 'RTable' and a matching condition ('RUpsertPredicate') to the 'RTuple's 
            -- of the target 'RTable'. An 'RTuple' from the target 'RTable' might match to a single only 'RTuple' in the source 'RTable', or not match at all. 
            -- If it is matched to more than one 'RTuple's then an exception ('UniquenessViolationInUpsert')is thrown. 
            -- When an 'RTuple' from the target 'RTable' is matched to a source 'RTuple', then the corresponding columns of the target 'RTuple' are updated
            -- with the new values provided in the source 'RTuple'. This takes place for the target 'RTuple's that match but also that satisfy the input 
            -- 'RPredicate'. Thus we can restrict further with a filter the 'RTuple's of the target 'RTable' where the update will take place.
            -- Finally, the source 'RTuple's that did not match to the target 'RTable', are inserted (appended) to the target 'RTable'
            --
            -- Please note that this is an __immutable__ implementation of an 'RTable' upsert.
            -- This simply means that the upsert operation returns a new 'RTable' and does not
            -- affect the original 'RTable'.
            -- Also note that the source and target 'RTable's should have the same structure.
            -- By \"structure\", we mean that the 'ColumnName's and the corresponding data types must match. Essentially what we record in the 'ColumnInfo'
            -- must be the same for the two 'RTable's. Otherwise a 'ConflictingRTableStructures' exception will be thrown.
            --
            -- @
            --  An Example:
            --  Source RTable: srcTab = 
            --      Id  |   Msg         | Other
            --      ----|---------------|-------
            --      1   |   "updated"   |"a"    
            --      2   |   "world2"    |"a"    
            --      3   |   "inserted"  |"a"    
            --
            --  Target RTable: trgTab = 
            --      Id  |   Msg         | Other
            --      ----|---------------|-------
            --      1   |   "hello1"    |"b"    
            --      2   |   "world1"    |"b"    
            --      4   |   "old"       |"b"    
            --      5   |   "hello"     |"b"    
            --            
            --  juliusToRTable $
            --      EtlMapStart
            --          :-> (EtlR $
            --                  ROpStart
            --                  :.(Upsert $ 
            --                      MergeInto (Tab trgTab) $
            --                          Using (TabSrc srcTab) $
            --                              MergeOn (RUpsertPredicate [\"Id\"] (\\t1 t2 -> t1 \<!\> \"Id\" == t2 \<!\> \"Id\")) $  -- merge condition: srcTab.Id == trgTab.Id
            --                                  WhenMatchedThen $
            --                                      UpdateCols [\"Msg\"] $
            --                                          FilterBy (\\t ->    let
            --                                                                msg = case toText (t \<!\> \"Msg\") of
            --                                                                  Just t -> t
            --                                                                  Nothing -> pack ""
            --                                                              in (take 5 msg) == (pack "hello")
            --                                                   )  -- Msg like "hello%"
            --                  )
            --              )
            --
            --  Result RTable: 
            --      Id  |   Msg         | Other
            --      ----|---------------|-------
            --      1   |   "updated"   |"b"   -- Updated RTuple. Note that only column \"Msg\" has been overwritten, as per the UpdateCols subclause
            --      2   |   "world1"    |"b"   -- Not affected due to FilterBy predicate
            --      3   |   "inserted"  |"a"   -- Inserted RTuple
            --      4   |   "old"       |"b"   -- Not affected due to MergeOn condition
            --      5   |   "hello"     |"b"   -- Not affected due to MergeOn condition  
            -- @
            --


-- | Insert Into subclause
data IntoClause = Into TabExpr InsertSource

-- | Subclause on 'Insert' clause. Defines the source of the insert operation.
-- The @Values@ branch is used for inserting a singl 'RTuple', while the @RTuples@ branch
-- is used for inserting a whole 'RTable', typically derived as the result of a Julius expression.
-- The former is similar in concept with an @INSERT INTO VALUES@ SQL clause, and the latter is similar 
-- in concept with an @INSERT INTO SELECT@ SQL clause.
data InsertSource = Values ValuesClause | RTuples TabSource

-- | Merge Into subclause
data MergeInto = MergeInto TabExpr MergeSource

-- | Upsert source subclause (Using clause in @SQL@)
data MergeSource = Using TabSource MergeMatchCondition

-- | Upsert matching condition subclause
data MergeMatchCondition =  MergeOn RUpsertPredicate WhenMatched

-- | When Matched subclause of Upsert
data WhenMatched = WhenMatchedThen UpdateColumns

-- | Update columns subclause of Upsert
data UpdateColumns = UpdateCols [ColumnName] ByPred 

-- | This subclause refers to the source 'RTable' that will feed an 'Insert' operation
data TabSource = TabSrc RTable 

-- | Subclause on 'Insert' clause. Defines the source 'RTuple' of the insert operation.
type ValuesClause = [(ColumnName, RDataType)]

-- | It is used to define an arbitrary unary operation on an 'RTable'
data ByGenUnaryOperation = ByUnaryOp UnaryRTableOperation         

-- | It is used to define an arbitrary binary operation on an 'RTable'
data ByGenBinaryOperation = ByBinaryOp BinaryRTableOperation         

-- | Resembles the \"FROM" clause in SQL. It defines the 'RTable' on which the Relational Operation will be applied
data FromRTable = From TabExpr

-- | A Table Expression defines the 'RTable' on which the current ETL Operation will be applied. If the 'Previous' constructor is used, then
-- this 'RTable' is the result of the previous ETL Operations in the current Julius Expression ('ETLMappingExpr')
data TabExpr = Tab RTable | Previous

-- | This clause is used for expressions where we do not allow the use of the Previous value
data TabLiteral = TabL RTable 

-- <TabExprJoin> = <TabExpr> "`On`" <RJoinPredicate
--data TabExprJoin = TabExpr `JoinOn` RJoinPredicate

-- | Join Predicate Clause. It defines when two 'RTuple's should be paired.
data TabExprJoin = JoinOn RJoinPredicate

-- | An Aggregate Operation Clause
data Aggregate = AggOn [AggOp] FromRTable

-- | These are the available aggregate operation clauses
data AggOp = 
                Sum ColumnName AsColumn      
            |   Count ColumnName AsColumn     -- ^ Count aggregation (no distinct)
            |   CountDist ColumnName AsColumn -- ^ Count distinct aggregation (i.e., @count(distinct col)@ in SQL). Returns the distinct number of values for this column.
            |   CountStar AsColumn            -- ^ Returns the number of 'RTuple's in the 'RTable' (i.e., @count(*)@ in SQL)           
            |   Min ColumnName AsColumn
            |   Max ColumnName AsColumn
            |   Avg ColumnName AsColumn      -- ^ Average aggregation
            |   StrAgg ColumnName AsColumn Delimiter  -- ^ String aggregation
            |   GenAgg ColumnName AsColumn AggBy    -- ^ A custom aggregate operation


-- | Julius Clause to provide a custom aggregation function
data AggBy = AggBy AggFunction

-- | Defines the name of the column that will hold the aggregate operation result.
-- It resembles the \"AS\" clause in SQL.
data AsColumn = As ColumnName            

-- | A grouping predicate clause. It defines an arbitrary function ('RGroupPRedicate'), which drives when two 'RTuple's should belong in the same group.
data GroupOnPred = GroupOn RGroupPredicate

-- | The Set sub-clause of an 'Update' 'RTable' clause. It specifies each column to be updated along with the new value.
data SetColumns = Set [(ColumnName, RDataType)]

-----------------------
-- Example:
-----------------------
myTransformation :: ColXForm
myTransformation = undefined

myTransformation2 :: ColXForm
myTransformation2 = undefined

myTable :: RTable
myTable = undefined

myTable2 :: RTable
myTable2 = undefined

myTable3 :: RTable
myTable3 = undefined

myTable4 :: RTable
myTable4 = undefined

myFpred :: RPredicate
myFpred = undefined

myFpred2 :: RPredicate
myFpred2 = undefined

myGpred :: RGroupPredicate
myGpred = undefined

myJoinPred :: RJoinPredicate
myJoinPred = undefined

myJoinPred2 :: RJoinPredicate
myJoinPred2 = undefined

myJoinPred3 :: RJoinPredicate
myJoinPred3 = undefined

-- Empty ETL Mappings
myEtlExpr1 =  EtlMapStart
myEtlExpr2 =  EtlMapStart :-> EtlC ColMappingEmpty
myEtlExpr3 =  EtlMapStart :-> EtlR ROpStart 

-- Simple Relational Operations examples:
-- Filter => SELECT * FROM myTable WHERE myFpred
myEtlExpr4 =    EtlMapStart 
            :-> (EtlR $ 
                        ROpStart
                    :.  (Filter (From $ Tab myTable) $ FilterBy myFpred))
--(EtlR $ (Filter (From $ Tab myTable) $ FilterBy myFpred) :. ROpStart)

-- Projection  => SELECT col1, col2, col3 FROM myTable
myEtlExpr5 =        EtlMapStart
                :-> (EtlR $
                            ROpStart
                        :.  (Select ["col1", "col2", "col3"] $ From $ Tab myTable))  

    --(EtlR $ (Select ["col1", "col2", "col3"] $ From $ Tab myTable) :. ROpStart)

-- Filter then Projection => SELECT col1, col2, col3 FROM myTable WHERE myFpred
myEtlExpr51 =       EtlMapStart
                :-> (EtlR $
                            ROpStart
                        :.  (Filter (From $ Tab myTable) $ FilterBy myFpred)
                        :.  (Select ["col1", "col2", "col3"] $ From $ Tab myTable))

            -- (EtlR $       (Select ["col1", "col2", "col3"] $ From $ Tab myTable)
            --             :.  (Filter (From $ Tab myTable) $ FilterBy myFpred) 
            --             :.  ROpStart
            --   )

-- Aggregation  => SELECT sum(trgCol2) as trgCol2Sum FROM myTable

myEtlExpr6 =        EtlMapStart
                :-> (EtlR $
                            ROpStart
                        :.  (Agg $ AggOn [Sum "trgCol2" $ As "trgCol2Sum"] $ From $ Tab myTable))

    --(EtlR $ (Agg $ AggOn [Sum "trgCol2" $ As "trgCol2Sum"] $ From $ Tab myTable) :. ROpStart)

-- Group By  => SELECT col1, col2, col3, sum(col4) as col4Sum, max(col4) as col4Max FROM myTable GROUP BY col1, col2, col3
myEtlExpr7 =    EtlMapStart
            :-> (EtlR $
                    ROpStart
                    :. (GroupBy  ["col1", "col2", "col3"] (AggOn [Sum "col4" $ As "col4Sum", Max "col4" $ As "col4Max"] $ From $ Tab myTable) $ GroupOn myGpred))


    --(EtlR $ (GroupBy  ["col1", "col2", "col3"] (AggOn [Sum "col4" $ As "col4Sum", Max "col4" $ As "col4Max"] $ From $ Tab myTable) $ GroupOn myGpred) :. ROpStart)

-- Inner Join => SELECT * FROM myTable JOIN myTable2 ON (myJoinPred)
myEtlExpr8 =    EtlMapStart
                :-> (EtlR $
                        ROpStart
                        :. (Join (TabL myTable) (Tab myTable2) $ JoinOn myJoinPred) )

--            (EtlR $ (Join (TabL myTable) (Tab myTable2) $ JoinOn myJoinPred) :. ROpStart)

-- A 3-way Inner Join => SELECT * FROM myTable JOIN myTable2 ON (myJoinPred) JOIN myTable3 ON (myJoinPred2) JOIN ON myTable4 ON (myJoinPred4)
myEtlExpr9 =    EtlMapStart
                :-> (EtlR $
                        ROpStart
                        :.  (Join (TabL myTable) (Tab myTable2) $ JoinOn myJoinPred)
                        :.  (Join (TabL myTable3) Previous $ JoinOn myJoinPred2)
                        :.  (Join (TabL myTable4) Previous $ JoinOn myJoinPred3))

            -- (EtlR $    (Join (TabL myTable4) Previous $ JoinOn myJoinPred3)
            --         :. (Join (TabL myTable3) Previous $ JoinOn myJoinPred2)
            --         :. (Join (TabL myTable) (Tab myTable2) $ JoinOn myJoinPred) 
            --         :. ROpStart
            -- )      


-- Union => SELECT * FROM myTable UNION SELECT * FROM myTable2
myEtlExpr10 =   EtlMapStart
                :-> (EtlR $
                        ROpStart
                        :.  (Union (TabL myTable) (Tab myTable2)))

--    (EtlR $ (Union (TabL myTable) (Tab myTable2)) :. ROpStart)

-- A more general example of an ETL mapping including column mappings and relational operations:
-- You read the Julius expression from bottom to top, and results to the follwoing discrete processing steps:      
--      A 1x1 column mapping on myTable based on myTransformation, produces and output table that is input to the next step
--  ->  A filter operation based on myFpred predicate : SELECT * FROM <previous result> WHERE myFpred
--  ->  A group by operation: SELECT col1, col2, col3, SUM(col4) as col2sum,  MAX(col4) as col4Max FROM <previous result>
--  ->  A projection operation: SELECT col1, col2, col3, col4sum FROM <previous result>
--  ->  A 1x1 column mapping on myTable based on myTransformation
--  ->  An aggregation: SELECT SUM(trgCol2) as trgCol2Sum FROM <previous result>
--  ->  A 1xN column mapping based on myTransformation2 
myEtlExpr :: ETLMappingExpr
myEtlExpr = EtlMapStart
            :-> (EtlC $ Source ["srcCol"] $ Target ["trgCol"] $ By myTransformation (On $ Tab myTable) DontRemoveSrc $ FilterBy myFpred2)         
            :-> (EtlR $
                    ROpStart
                    :.  (Filter (From Previous) $ FilterBy myFpred)                     
                    :.  (GroupBy  ["col1", "col2", "col3"] (AggOn [Sum "col4" $ As "col4Sum", Max "col4" $ As "col4Max"] $ From Previous) $ GroupOn myGpred) 
                    :.  (Select ["col1", "col2", "col3", "col4Sum"] $ From Previous)                    
                 )
            :-> (EtlC $ Source ["tgrCol"] $ Target ["trgCol2"] $ By myTransformation (On Previous) RemoveSrc $ FilterBy myFpred2)
            :-> (EtlR $ ROpStart :. (Agg $ AggOn [Sum "trgCol2" $ As "trgCol2Sum"] $ From Previous))
            :-> (EtlC $ Source ["trgCol2Sum"] $ Target ["newCol1", "newCol2"] $ By myTransformation2 (On Previous) DontRemoveSrc $ FilterBy myFpred2) 

     --     (EtlC $ Source ["trgCol2Sum"] $ Target ["newCol1", "newCol2"] $ By myTransformation2 (On Previous) DontRemoveSrc $ FilterBy myFpred2) 
     -- :-> (EtlR $ (Agg $ AggOn [Sum "trgCol2" $ As "trgCol2Sum"] $ From Previous) :. ROpStart )
     -- :-> (EtlC $ Source ["tgrCol"] $ Target ["trgCol2"] $ By myTransformation (On Previous) RemoveSrc $ FilterBy myFpred2)
     -- :-> (EtlR $
     --            -- You read it from bottom to top
     --         (Select ["col1", "col2", "col3", "col4Sum"] $ From Previous) 
     --         :. (GroupBy  ["col1", "col2", "col3"] (AggOn [Sum "col4" $ As "col4Sum", Max "col4" $ As "col4Max"] $ From Previous) $ GroupOn myGpred) 
     --         :. (Filter (From Previous) $ FilterBy myFpred)
     --         :. ROpStart
     --     )
     -- :-> (EtlC $ Source ["srcCol"] $ Target ["trgCol"] $ By myTransformation (On $ Tab myTable) DontRemoveSrc $ FilterBy myFpred2)         
     -- :-> EtlMapStart


-- | Returns a prefix of an ETLMappingExpr that matches a named intermediate result.
-- For example, below we show a Julius expression where we define an intermediate named result called "myResult".
-- This result, is used at a later stage in this Julius expression, with the use of the function takeNamedResult.
--
-- @
--        etlXpression = 
--                    EtlMapStart
--                    :-> (EtlC $ ...)                
--                    :=> NamedResult "myResult" (EtlR $ ...) 
--                    :-> (EtlR $ ... )
--                    :-> (EtlR $
--                            ROpStart
--                            :.  (Minus 
--                                    (TabL $ 
--                                        juliusToRTable $ takeNamedResult "myResult" etlXpression    --  THIS IS THE POINT WHERE WE USE THE NAMED RESULT!
--                                    ) 
--                                    (Previous))
--                        )
-- @
--
-- In the above Julius expression (etlXpresion) the \"myResult\" named result equals to the prefix of the etlXpresion, up to the operation (included) with the
-- named result \"myResult\".
--
-- @
--      
--      takeNamedResult "myResult" etlXpression ==  EtlMapStart
--                                                  :-> (EtlC $ ...)                
--                                                  :=> NamedResult "myResult" (EtlR $ ...) 
--
-- @
--
-- Note that the julius expression is scanned from right to left and thus it will return the longest prefix expression that matches the input name
-- 
takeNamedResult :: 
        NamedResultName -- ^ the name of the intermediate result
    ->  ETLMappingExpr   -- ^ input ETLMapping Expression
    ->  ETLMappingExpr   -- ^ output ETLMapping Expression
takeNamedResult _ EtlMapStart = EtlMapStart
takeNamedResult rname (restExpression :-> etlOpXpr) = takeNamedResult rname restExpression
takeNamedResult rname (restExpression :=> NamedResult n etlOpXpr) = 
    if rname == n 
        then (restExpression :=> NamedResult n etlOpXpr)
        else takeNamedResult rname restExpression

-- | Evaluates (parses) the Julius exrpession and produces an 'ETLMapping'. The 'ETLMapping' is an internal representation of the Julius expression and one needs
-- to combine it with the 'etl' function, in order to evaluate the Julius expression into an 'RTable'. This can be achieved directly with function 'juliusToRTable'
evalJulius :: ETLMappingExpr -> ETLMapping
evalJulius EtlMapStart = ETLMapEmpty
evalJulius (restExpression :-> (EtlC colMapExpression)) = 
    case colMapExpression of
        ColMappingEmpty 
            -> connectETLMapLD ETLcOp {cmap = ColMapEmpty} emptyRTable (evalJulius restExpression)  -- (evalJulius restExpression) returns the previous ETLMapping 
        Source srcColumns (Target trgColumns (By colXformation (On tabExpr) removeSource (FilterBy filterPred))) 
            -> let  prevMapping = case tabExpr of
                        Tab tab -> rtabToETLMapping tab -- make the RTable an ETLMapping (this is a leaf node)
                        Previous -> evalJulius restExpression  
                    removeOption = case removeSource of 
                        RemoveSrc -> Yes 
                        DontRemoveSrc -> No
           in connectETLMapLD ETLcOp {cmap = createColMapping srcColumns trgColumns colXformation removeOption filterPred}
                              emptyRTable -- right branch
                              prevMapping -- left branch
evalJulius (restExpression :=> NamedResult rname (EtlC colMapExpression)) = 
    case colMapExpression of
        ColMappingEmpty 
            -> connectETLMapLD ETLcOp {cmap = ColMapEmpty} emptyRTable (evalJulius restExpression)  -- (evalJulius restExpression) returns the previous ETLMapping 
        Source srcColumns (Target trgColumns (By colXformation (On tabExpr) removeSource (FilterBy filterPred))) 
            -> let  prevMapping = case tabExpr of
                        Tab tab -> rtabToETLMapping tab -- make the RTable an ETLMapping (this is a leaf node)
                        Previous -> evalJulius restExpression  
                    removeOption = case removeSource of 
                        RemoveSrc -> Yes 
                        DontRemoveSrc -> No
           in connectETLMapLD ETLcOp {cmap = createColMapping srcColumns trgColumns colXformation removeOption filterPred}
                              emptyRTable -- right branch
                              prevMapping -- left branch
evalJulius (restExpression :-> (EtlR relOperExpression)) = 
    let (roperation, leftTabExpr, rightTabExpr) = evalROpExpr relOperExpression
        (prevMapping, rightBranch)  = case (leftTabExpr, rightTabExpr) of
            -- binary operation (leaf)
            (TXE (Tab tab1), TXE (Tab tab2)) -> (rtabToETLMapping tab1, tab2)
            -- binary operation (no leaf)
            (TXE Previous, TXE (Tab tab)) -> (evalJulius restExpression, tab)
            -- unary operation (leaf) - option 1
            (TXE (Tab tab), EmptyTab) -> (rtabToETLMapping tab, emptyRTable)
            -- unary operation (leaf) - option 2
            (EmptyTab, TXE (Tab tab)) -> (ETLMapEmpty, tab)
            -- unary operation (no leaf) 
            (TXE Previous, EmptyTab) -> (evalJulius restExpression, emptyRTable)
            -- everything else is just wrong!! Do nothing
            (_, _) -> (ETLMapEmpty, emptyRTable)
    in connectETLMapLD ETLrOp {rop = roperation} rightBranch prevMapping  -- (evalJulius restExpression) returns the previous ETLMapping 
evalJulius (restExpression :=> NamedResult rname (EtlR relOperExpression)) = 
    let (roperation, leftTabExpr, rightTabExpr) = evalROpExpr relOperExpression
        (prevMapping, rightBranch)  = case (leftTabExpr, rightTabExpr) of
            -- binary operation (leaf)
            (TXE (Tab tab1), TXE (Tab tab2)) -> (rtabToETLMapping tab1, tab2)
            -- binary operation (no leaf)
            (TXE Previous, TXE (Tab tab)) -> (evalJulius restExpression, tab)
            -- unary operation (leaf) - option 1
            (TXE (Tab tab), EmptyTab) -> (rtabToETLMapping tab, emptyRTable)
            -- unary operation (leaf) - option 2
            (EmptyTab, TXE (Tab tab)) -> (ETLMapEmpty, tab)
            -- unary operation (no leaf) 
            (TXE Previous, EmptyTab) -> (evalJulius restExpression, emptyRTable)
            -- everything else is just wrong!! Do nothing
            (_, _) -> (ETLMapEmpty, emptyRTable)
    in connectETLMapLD ETLrOp {rop = roperation} rightBranch prevMapping  -- (evalJulius restExpression) returns the previous ETLMapping 


-- | Pure code to evaluate the \"ETL-logic\" of a Julius expression and generate the corresponding target RTable.
--
-- The evaluation of a Julius expression (i.e., a 'ETLMappingExpr') to an RTable is strict. It evaluates fully to Normal Form (NF)
-- as opposed to a lazy evaluation (i.e., only during IO), or evaluation to a WHNF. 
-- This is for efficiency reasons (e.g., avoid space leaks and excessive memory usage). It also has the impact that exceptions will be thrown
-- at the same line of code that 'juliusToRTable' is called. Thus one should wrap this call with a 'catch' handler, or use 'eitherPrintRTable',
-- or 'eitherPrintfRTable', if one wants to handle the exception gracefully.
--
-- Example:
--
-- @
-- do 
--  catch (printRTable $ juliusToRTable $ \<a Julius expression\> )
--        (\\e -> putStrLn $ "There was an error in the Julius evaluation: " ++ (show (e::SomeException)) )
-- @
-- 
-- Or, similarly
-- 
-- @
-- do 
--  p <- (eitherPrintRTable  printRTable $
--                           juliusToRTable $ \<a Julius expression\>                                 
--       ) :: IO (Either SomeException ())
--  case p of
--            Left exc -> putStrLn $ "There was an error in the Julius evaluation: " ++ (show exc)
--            Right _  -> return ()
-- @
-- 
juliusToRTable :: ETLMappingExpr -> RTable
juliusToRTable = CDS.force $ (etl . evalJulius)



-- | Evaluate a Julius expression within the IO Monad. 
-- I.e., Effectful code to evaluate the \"ETL-logic\" of a Julius expression and generate the corresponding target RTable.
--
-- The evaluation of a Julius expression (i.e., a 'ETLMappingExpr') to an RTable is strict. It evaluates fully to Normal Form (NF)
-- as opposed to a lazy evaluation (i.e., only during IO), or evaluation to a WHNF. 
-- This is for efficiency reasons (e.g., avoid space leaks and excessive memory usage). It also has the impact that exceptions will be thrown
-- at the same line of code that 'runJulius' is called. Thus one should wrap this call with a 'catch' handler, or use 'eitherRunJulius',
-- if he wants to handle the exception gracefully.
--
-- Example:
--
-- @
-- do 
--     result <- catch (runJulius $  \<a Julius expression\>)
--                     (\e -> do 
--                              putStrLn $ "there was an error in Julius evaluation: " ++ (show (e::SomeException))
--                              return emptyRTable
--                     )
-- @
-- 
runJulius :: ETLMappingExpr -> IO RTable
runJulius jul = return $!! juliusToRTable jul 


-- | Evaluate a Julius expression and return the corresponding target 'RTable' or an exception.
-- One can define custom exceptions to be thrown within a Julius expression. This function will catch any
-- exceptions that are instances of the 'Exception' type class.
--
-- The evaluation of a Julius expression (i.e., a 'ETLMappingExpr') to an 'RTable' is strict. It evaluates fully to Normal Form (NF)
-- as opposed to a lazy evaluation (i.e., only during IO), or evaluation to a WHNF. 
-- This is for efficiency reasons (e.g., avoid space leaks and excessive memory usage). 
--
-- Example:
--
-- @
-- do 
--     res <- (eitherRunJulius $ \<a Julius expression\>) :: IO (Either SomeException RTable) 
--     resultRTab  <- case res of
--                     Right t  -> return t
--                     Left exc ->  do 
--                                     putStrLn $ "there was an error in Julius evaluation: " ++ (show exc)
--                                     return emptyRTable
--
-- @
--
eitherRunJulius :: Exception e => ETLMappingExpr ->  IO (Either e RTable)    
eitherRunJulius jul = try $ runJulius jul

-- | Receives an input Julius expression, evaluates it to an ETL Mapping ('ETLMapping') and executes it, 
-- in order to return an 'RTabResult' containing an 'RTable' storing the result of the ETL Mapping, as well as the number of 'RTuple's returned 
juliusToResult :: ETLMappingExpr -> RTabResult
juliusToResult = CDS.force $ (etlRes . evalJulius)

-- | Evaluate a Julius expression within the IO Monad and return an 'RTabResult'.
runJuliusToResult :: ETLMappingExpr -> IO RTabResult
runJuliusToResult jul = return $ juliusToResult jul

-- | Evaluate a Julius expression within the IO Monad and return either an 'RTabResult', or an exception, in case of an error during evaluation.
eitherRunJuliusToResult :: Exception e => ETLMappingExpr ->  IO (Either e RTabResult)
eitherRunJuliusToResult jul = try $ runJuliusToResult jul

-- | Generic ETL execution function. It receives a list of input (aka \"source\") 'RTable's and an ETL function that
-- produces a list of output (aka \"target\") 'RTable's. The ETL function should embed all the \"transformation-logic\"
-- from the source 'RTable's to the target 'RTable's.
runETL :: ([RTable] -> [RTable]) -> [RTable] -> IO [RTable]
runETL etlf inRTabs = return $!! etlf inRTabs 

-- | Generic ETL execution function that returns either the target list of 'RTable's, or an exception in case of a problem
-- during the ETL code execution. 
-- It receives a list of input (aka \"source\") 'RTable's and an ETL function that
-- produces a list of output (aka \"target\") 'RTable's. The ETL function should embed all the \"transformation-logic\"
-- from the source 'RTable's to the target 'RTable's.
eitherRunETL ::  Exception e => ([RTable] -> [RTable]) -> [RTable] -> IO (Either e [RTable])
eitherRunETL etlf inRTabs = try $ (runETL etlf inRTabs)


-- | Internal type: We use this data type in order to identify unary vs binary operations and if the table is coming from the left or right branch
data TabExprEnhanced = TXE TabExpr | EmptyTab

-- | Evaluates (parses) a Relational Operation Expression of the form 
--
-- @
--  ROpStart :. ROp  ... :. ROp 
-- @
--
-- and produces the corresponding ROperation data type.
-- This will be an RCombinedOp relational operation that will be the composition of all relational operators
-- in the ROpExpr. In the returned result the TabExpr corresponding to the left and right RTable inputs to the ROperation
-- respectively, are also returned.
evalROpExpr :: ROpExpr -> (ROperation, TabExprEnhanced, TabExprEnhanced)
evalROpExpr ROpStart = (ROperationEmpty, EmptyTab, EmptyTab)
evalROpExpr (restExpression :. rop) = 
    case rop of
        Filter (From tabExpr) (FilterBy filterPred) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = f filterPred -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression

            -- the current RCombinedOp is produced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)                    

        Select colNameList (From tabExpr) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = p colNameList -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression

            -- the current RCombinedOp is produced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)                    

        Agg (AggOn aggOpList (From tabExpr)) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = rAgg $ aggOpExprToAggOp aggOpList -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression

            -- the current RCombinedOp is produced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)                    
        
        GroupBy colNameList (AggOn aggOpList (From tabExpr)) (GroupOn grpPred) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = rG grpPred (aggOpExprToAggOp aggOpList) colNameList -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression

            -- the current RCombinedOp is produced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)  

        OrderBy colOrderingSpecList (From tabExpr) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = rO colOrderingSpecList -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression

            -- the current RCombinedOp is produced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)  
        
        Join (TabL tabl) tabExpr (JoinOn joinPred) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = iJ joinPred tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        LJoin (TabL tabl) tabExpr (JoinOn joinPred) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = lJ joinPred tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        RJoin (TabL tabl) tabExpr (JoinOn joinPred) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = rJ joinPred tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        FOJoin (TabL tabl) tabExpr (JoinOn joinPred) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = foJ joinPred tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        SemiJoin (TabL tabl) tabExpr (JoinOn joinPred) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = sJ joinPred tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        SemiJoinP tabExpr (TabL tabl) (JoinOn joinPred) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = (flip (sJ joinPred)) tabl -- -- this returns a RTable -> RTable function, note that flip ensures that left table will be the second argument in the (sJ joinPred) function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        AntiJoin (TabL tabl) tabExpr (JoinOn joinPred) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = aJ joinPred tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        AntiJoinP tabExpr (TabL tabl) (JoinOn joinPred) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = (flip (aJ joinPred)) tabl -- -- this returns a RTable -> RTable function, note that flip ensures that left table will be the second argument in the (aJ joinPred) function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)
        
        Intersect (TabL tabl) tabExpr ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = i tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        Union (TabL tabl) tabExpr ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = u tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        UnionAll (TabL tabl) tabExpr ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = runUnionAll tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        Minus (TabL tabl) tabExpr ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = d tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        -- TabExpr `MinusP` TabLiteral
        MinusP tabExpr (TabL tabl) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = (flip d) tabl -- this returns a RTable -> RTable function, note that flip ensures that left table will be the second argument in d function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        -- this is a generic unary operation on a RTable
        GenUnaryOp (On tabExpr) (ByUnaryOp unRTabOp) ->  
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = unRTabOp -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        -- this is a generic binary operation on a RTable 
        GenBinaryOp (TabL tabl) tabExpr (ByBinaryOp binRTabOp) -> 
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = binRTabOp tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is produced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        -- this is the RTable update operation
        Update tabExpr (Set colValuePairs) (FilterBy filterPred) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = updateRTab colValuePairs filterPred -- this returns an RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression

            -- the current RCombinedOp is produced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)                    

        -- this is the RTable insert values operation (inserts a single RTuple)
        Insert (Into tabExpr (Values colValuePairs)) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = insertAppendRTab (createRTuple colValuePairs) -- this returns an RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression

            -- the current RCombinedOp is produced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)                    

        -- this is the RTable insert rtuples operation (inserts a whole rtable)
        Insert (Into tabExpr (RTuples (TabSrc rtabsrc))) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = insertRTabToRTab rtabsrc -- this returns an RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression

            -- the current RCombinedOp is produced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)                    

        -- this is the Upsert (Update+Insert, aka Merge) Operation.
        Upsert (    
                    MergeInto tabExpr ( 
                                        Using (TabSrc srcTab) ( 
                                                                MergeOn upsPred ( 
                                                                                   WhenMatchedThen ( 
                                                                                                        UpdateCols cols (FilterBy fpred)
                                                                                                    )
                                                                                )
                                                              )
                                      )
                ) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = upsertRTab srcTab upsPred cols fpred -- this returns an RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression

            -- the current RCombinedOp is produced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)                    


-- | turns the  list of agg operation expressions to a list of RAggOperation data type
aggOpExprToAggOp :: [AggOp] -> [RAggOperation]
aggOpExprToAggOp [] = []
aggOpExprToAggOp (aggopExpr : rest) = 
    let aggop = case aggopExpr of
                    Sum srcCol (As trgCol)                  -> (raggSum srcCol trgCol)
                    Count srcCol (As trgCol)                -> (raggCount srcCol trgCol)
                    CountDist srcCol (As trgCol)            -> (raggCountDist srcCol trgCol)
                    CountStar (As trgCol)                   -> (raggCountStar trgCol)                                        
                    Min srcCol (As trgCol)                  -> (raggMin srcCol trgCol)
                    Max srcCol (As trgCol)                  -> (raggMax srcCol trgCol)
                    Avg srcCol (As trgCol)                  -> (raggAvg srcCol trgCol)
                    StrAgg srcCol (As trgCol) delimiter     -> (raggStrAgg srcCol trgCol delimiter)
                    GenAgg srcCol (As trgCol) (AggBy aggf)  -> (raggGenericAgg aggf srcCol trgCol)
    in aggop : (aggOpExprToAggOp rest)

 
-- and then run the produced ETLMapping
finalRTable = etl $ evalJulius myEtlExpr

-- ##############
-- Various ETL Operations, defined as Generic Unary/Binary RTable Operations
-- ##############

-- | Returns an 'ETLOperation' that adds a surrogate key (SK) column to an 'RTable' and
-- fills each row with a SK value.
-- This function is only exposed for backward compatibility reasons. The recommended function to use instead
-- is 'addSurrogateKeyJ', which can be embedded directly into a Julius expression as a 'UnaryRTableOperation'.
addSurrogateKey :: Integral a =>    
       ColumnName    -- ^ The name of the surrogate key column
    -- -> Integer       -- ^ The initial value of the Surrogate Key will be the value of this parameter    
    -> a             -- ^ The initial value of the Surrogate Key will be the value of this parameter    
    -> ETLOperation  -- ^ Output ETL operation which encapsulates the add surrogate key column mapping
addSurrogateKey cname initVal  =   
    let combOp = RCombinedOp { rcombOp = updateSKvalue . (addColumn cname (RInt (fromIntegral initVal)))  }
            where
                -- updateSKvalue :: RTable -> RTable
                -- updateSKvalue rt = V.foldr' ( \rtup trgRtab ->  let 
                --                                                 newTuple = updateRTuple cname (rtup<!>cname + RInt 1) rtup
                --                                         in appendRTuple newTuple trgRtab 
                --                     ) emptyRTable rt
                updateSKvalue :: RTable -> RTable
                updateSKvalue rt =
                        let     indexedRTab = V.indexed rt -- this returns a: Vector (Int, RTuple) (pairs each RTuple with its index in the RTable)
                        in V.zipWith (\tupsrc (val, _) -> upsertRTuple cname (RInt (fromIntegral initVal) + RInt (fromIntegral val)) tupsrc ) rt indexedRTab
    in ETLrOp combOp 

-- | Returns an 'UnaryRTableOperation' ('RTable' -> 'RTable') that adds a surrogate key (SK) column to an 'RTable' and
-- fills each row with a SK value. It  primarily  is intended to be used within a Julius expression. For example:
--
-- @
--  GenUnaryOp (On Tab rtab1) $ ByUnaryOp (addSurrogateKeyJ "TxSK" 0)
-- @
-- 
addSurrogateKeyJ :: Integral a 
                 =>  ColumnName    -- ^ The name of the surrogate key column
                 -> a             -- ^ The initial value of the Surrogate Key will be the value of this parameter    
                 -> RTable        -- ^ Input RTable
                 -> RTable        -- ^ Output RTable
addSurrogateKeyJ cname initVal  =   
    updateSKvalue . (addColumn cname (RInt (fromIntegral initVal))) 
            where
                -- updateSKvalue :: RTable -> RTable
                -- updateSKvalue rt = V.foldr' ( \rtup trgRtab ->  let 
                --                                                 newTuple = updateRTuple cname (rtup<!>cname + RInt 1) rtup
                --                                         in appendRTuple newTuple trgRtab 
                --                     ) emptyRTable rt
                updateSKvalue :: RTable -> RTable
                updateSKvalue rt =
                        let     indexedRTab = V.indexed rt -- this returns a: Vector (Int, RTuple) (pairs each RTuple with its index in the RTable)
                        in V.zipWith (\tupsrc (val, _) -> upsertRTuple cname (RInt (fromIntegral initVal) + RInt (fromIntegral val)) tupsrc ) rt indexedRTab


-- | Returns an 'ETLOperation' that Appends an 'RTable' to a target 'RTable' 
-- This function is only exposed for backward compatibility reasons. The recommended function to use instead
-- is 'appendRTableJ', which can be embedded directly into a Julius expression as a 'BinaryRTableOperation'.
appendRTable ::
        ETLOperation  -- ^ Output ETL Operation
appendRTable  = 
    let binOp = RBinOp { rbinOp = flip (V.++) } -- we need to flip the input parameters so that when we embed this operation in an ETL mapping
                                                -- then the delta table will be appended to the target table.
    in ETLrOp binOp


-- | Returns a 'BinaryRTableOperation' ('RTable' -> 'RTable' -> 'RTable') that Appends an 'RTable' to a target 'RTable'.
-- It is primarily intended to be used within a Julius expression. For example:
--
-- @
--   GenBinaryOp (TabL rtab1) (Tab $ rtab2) $ ByBinaryOp appendRTableJ
-- @
--
appendRTableJ ::
            RTable -- ^ Target RTable
        ->  RTable -- ^ Input RTable
        ->  RTable -- ^ Output RTable 
appendRTableJ  = (V.++) 
