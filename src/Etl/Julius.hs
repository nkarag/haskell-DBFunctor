{-|
Module      : Julius
Description : A simple Embedded DSL for ETL/ELT data processing in Haskell
Copyright   : (c) Nikos Karagiannidis, 2018
                  
License     : GPL-3
Maintainer  : nkarag@gmail.com
Stability   : stable
Portability : POSIX

__Julius__ is an /Embedded Domain Specific Language (EDSL)/ for ETL/ELT data processing in Haskell.  
Julius enables us to express complex data transformation flows (i.e., an arbitrary combination of ETL operations) in a more friendly manner (a __Julius Expression__), 
with plain Haskell code (no special language for ETL scripting required). For more information read this [Julius Tutorial] (https://github.com/nkarag/haskell-DBFunctor/blob/master/doc/JULIUS-TUTORIAL.md).

= When to use this module
This module should be used whenever one has "tabular data" (e.g., some CSV files, or any type of data that can be an instance of the 'RTabular'
type class and thus define the 'toRTable' function) and wants to analyze them in-memory with the well-known relational algebra operations 
(selection, projection, join, groupby, aggregations etc) that lie behind SQL. 
This data analysis takes place within your haskell code, without the need to import the data into a database (database-less 
data processing) and the result can be turned into the original format (e.g., CSV) with a simple call to the 'fromRTable' function.

"Etl.Julius" provides a simple language for expressing all relational algebra operations and arbitrary combinations of them, and thus is a powerful
tool for expressing complex data transfromations in Haskell. Moreover, the Julius language includes a clause for the __Column Mapping__ ('RColMapping') concept, which
is a construct used in ETL tools and enables arbitrary transformations at the column level and the creation of derived columns based on arbitrary expressions on the existing ones. 
Finally, the ad hoc combination of relational operations and Column Mappings, chained in an data transformation flow, implements the concept of the __ETL Mapping__ ('ETLMapping'), 
which is the core unit in al ETL tools and is implemented in the "ETL.Internal.Core" module. For the relational algebra operations, Julius exploits the functions in the "RTable.Core" 
module, which also exports it.

The Julius EDSL is the recommended method for expressing ETL flows in Haskell, as well as doing any data analysis task within the "DBFunctor" package. "Etl.Julius" is a self-sufficient 
module and imports all neccesary functionality from "RTable.Core" and "Etl.Internal.Core" modules, so a programmer should only import "Etl.Julius" and nothing else, in order to have 
complete functionality.

= Overview
The core data type in the Julius EDSL is the 'ETLMappingExpr'. This data type creates a so-called __Julius Expression__. This Julius expression, it is evaluated to an 'ETLMapping' (see 'ETLMapping')
with the 'evalJulius' function and from then, evaluated into an 'RTable' (see 'juliusToRTable'), which is the final result of our transformation. 
For more information on these core Julius concepts read [DBFunctor Concepts] (https://github.com/nkarag/haskell-DBFunctor/blob/master/doc/CONCEPTS.md).

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
doEtl :: RTable -> IO RTable
doEtl = do
    -- 3. Define your Julius Expression(s)
    let jul = 
        EtlMapStart
        :-> (EtlR $
                ROpStart  
                :. (...)
        ...
    -- 4. Evaluate Julius to the Result RTable
    return $ juliusToRTable jul

main :: IO ()
main = do
    
    -- 5. read source csv files
    -- E.g.,
    srcCSV <- readCSV \".\/app\/test-data.csv\"

    -- 6. Convert CSV to an RTable and do your ETL
    resultRTab <- doETL $ toRTable src_DBTab_MData srcCSV

    -- 7. Print your results on screen
    -- E.g.,
    printfRTable (genRTupleFormat [\"OWNER\", \"TABLE_NAME\",\"LAST_ANALYZED\"] genDefaultColFormatMap) $ resultRTab

    -- 8. Save your result to a CSV file
    -- E.g.,
    writeCSV \".\/app\/result-data.csv\" $ 
                    fromRTable result_tab_MData resultRTab
@

[-- 1.] We define the necessary 'RTable' metadata, for each 'RTable' in our program. This is equivalent to a @CREATE TABLE@ ddl clause in SQL.
[-- 2.] Here is where we define our ETL code. We dont want to do our ETL in the main function, so we separate the ETL code into a separate function (@doETL@). In general,
   in our main, we want to follow the pattern:

        * Read your Input
        * Do your ETL
        * Write your Output

   This function receives as input all the necessary source 'RTable's and outputs the resulting (__Target__) 'RTable', after the all the necessary transformations have been
   executed. Of course an ETL code might produce more than one target 'RTable's, e.g., a target schema (in DB parlance). Moreover, the doETL funciton can be arbitrary complex, 
   depending on the ETL logic that we want to implement in each case. It is essentially the entry point to our ETL implementation

[-- 3.] Our ETL code consists of an arbitrary number of Julius expressions. One can define multiple separate Julius expressions, some of which might depend on others, in order to
   implement ones ETL logic. However, we recommend as a better style to define as few as possible Julius expressions and in order to distinguish between distinct steps
   in the data processing to use the 'NamedMap' constructor. This way you have your data flow logic in a single expression, with identifiable separate steps, which you can
   reference and exploit further as intermediate results in other expressions (see also relevant example below).
[-- 4.] Each Julius expression must be evaluated into an 'RTable' and returned to the caller of the ETL code (in our case this is @main@)
[-- 5.] Here is where we read our input for the ETL. In our case, this is a simple CSV file that we read with the helop of the 'readCSV' function.
[-- 6.] We convert our input CSV to an 'RTable', with the 'toRTable' and pass it as input to our ETL code.
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

{-# LANGUAGE OverloadedStrings #-}
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
    -- ** The Relational Operation Clause
    ,ROpExpr(..)    
    ,RelationalOp (..)    
    ,FromRTable (..)
    ,Aggregate (..)    
    ,AggOp (..)    
    ,AsColumn (..)    
    ,GroupOnPred (..)    
    ,TabLiteral (..)
    ,TabExprJoin (..)    
    ,ByGenUnaryOperation (..)
    ,ByGenBinaryOperation (..)
    -- ** Julius Expression Evaluation
    ,evalJulius
    ,juliusToRTable
    ,juliusToResult
    ,takeNamedResult
    ) where

-- Data.RTable
import RTable.Core
-- Data.RTable.Etl
import Etl.Internal.Core

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
        |   TabLiteral `Intersect` TabExpr                  -- ^ Intersection clause
        |   TabLiteral `Union` TabExpr                      -- ^ Union clause
        |   TabLiteral `Minus` TabExpr                      -- ^ Minus clause (set Difference operation)
        |   TabExpr `MinusP` TabLiteral                     --  ^ This is a Minus operation to be used when the left table must be the 'Previous' value.
        |   GenUnaryOp OnRTable ByGenUnaryOperation         -- ^ This is a generic unary operation on a RTable ('UnaryRTableOperation'). It is used to define an arbitrary unary operation on an 'RTable'
        |   GenBinaryOp TabLiteral TabExpr ByGenBinaryOperation -- ^ This is a generic binary operation on a RTable ('BinaryRTableOperation'). It is used to define an arbitrary binary operation on an 'RTable'
        |   OrderBy [(ColumnName, OrderingSpec)] FromRTable     -- ^ Order By clause.

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
            |   Count ColumnName AsColumn    
            |   Min ColumnName AsColumn
            |   Max ColumnName AsColumn
            |   Avg ColumnName AsColumn

-- | Defines the name of the column that will hold the aggregate operation result.
-- It resembles the \"AS\" clause in SQL.
data AsColumn = As ColumnName            

-- | A grouping predicate clause. It defines an arbitrary function ('RGroupPRedicate'), which drives when two 'RTuple's should belong in the same group.
data GroupOnPred = GroupOn RGroupPredicate

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

-- | Evaluates (parses) the Julius DSL and produces an 'ETLMapping'.
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


-- | Receives an input Julius expression, evaluates it to an ETL Mapping ('ETLMapping') and executes it, in order to return an 'RTable' storing the result of the ETL Mapping
juliusToRTable :: ETLMappingExpr -> RTable
juliusToRTable = etl . evalJulius


-- | Receives an input Julius expression, evaluates it to an ETL Mapping ('ETLMapping') and executes it, 
-- in order to return an 'RTabResult' containing an 'RTable' storing the result of the ETL Mapping, as well as the number of 'RTuple's returned 
juliusToResult :: ETLMappingExpr -> RTabResult
juliusToResult = etlRes . evalJulius


-- | Internal type: We use this data type in order to identify unary vs binary operations and if the table is coming from the left or right branch
data TabExprEnhanced = TXE TabExpr | EmptyTab

-- | Evaluates (parses) a Relational Operation Expression of the form 
--
-- @
--  ROp :. ROp :. ... :. ROpStart
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


-- | turns the  list of agg operation expressions to a list of RAggOperation data type
aggOpExprToAggOp :: [AggOp] -> [RAggOperation]
aggOpExprToAggOp [] = []
aggOpExprToAggOp (aggopExpr : rest) = 
    let aggop = case aggopExpr of
                    Sum srcCol (As trgCol)      -> (raggSum srcCol trgCol)
                    Count srcCol (As trgCol)    -> (raggCount srcCol trgCol)
                    Min srcCol (As trgCol)      -> (raggMin srcCol trgCol)
                    Max srcCol (As trgCol)      -> (raggMax srcCol trgCol)
                    Avg srcCol (As trgCol)      -> (raggAvg srcCol trgCol)
    in aggop : (aggOpExprToAggOp rest)

            
 
-- and then run the produced ETLMapping
finalRTable = etl $ evalJulius myEtlExpr