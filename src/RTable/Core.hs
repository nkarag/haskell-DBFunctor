{-|
Module      : RTable
Description : Implements the relational Table concept. Defines all necessary data types like RTable and RTuple as well as basic relational algebra operations on RTables.
Copyright   : (c) Nikos Karagiannidis, 2018
                  
License     : BSD3
Maintainer  : nkarag@gmail.com
Stability   : stable
Portability : POSIX


This is the core module that implements the relational Table concept with the 'RTable' data type. 
It defines all necessary data types like 'RTable' and 'RTuple' as well as all the basic relational algebra operations (selection -i.e., filter-
, projection, inner/outer join, aggregation, grouping etc.) on 'RTable's.


= When to use this module
This module should be used whenever one has "tabular data" (e.g., some CSV files, or any type of data that can be an instance of the 'RTabular'
type class and thus define the 'toRTable'  and 'fromRTable' functions) and wants to analyze them in-memory with the well-known relational algebra operations 
(selection, projection, join, groupby, aggregations etc) that lie behind SQL. 
This data analysis takes place within your haskell code, without the need to import the data into a database (database-less 
data processing) and the result can be turned into the original format (e.g., CSV) with a simple call to the 'fromRTable' function.

"RTable.Core" gives you an interface for all common relational algebra operations, which are expressed as functions over
the basic 'RTable' data type. Of course, since each relational algebra operation is a function that returns a new RTable (immutability), one
can compose these operations and thus express an arbitrary complex query. Immutability also holds for DML operations also (e.g., 'updateRTab'). This
means that any update on an RTable operates like a @CREATE AS SELECT@ statement in SQL, creating a new 'RTable' and not modifying an existing one.

Note that the recommended method in order to perform data analysis via relational algebra operations is to use the type-level __Embedded Domain Specific Language__
__(EDSL) Julius__, defined in module "Etl.Julius", which exports the "RTable.Core" module. This provides a standard way of expressing queries and is 
simpler for expressing more complex queries (with many relational algebra operations). Moreover it supports intermediate results (i.e., subqueries). Finally, 
if you need to implement some __ETL/ELT data flows__, that will use the relational operations defined in "RTable.Core" to analyze data but also
to combine them with various __Column Mappings__ ('RColMapping'), in order to achieve various data transformations, then Julius is the appropriate tool for this job.

See this [Julius Tutorial] (https://github.com/nkarag/haskell-DBFunctor/blob/master/doc/JULIUS-TUTORIAL.md)

= Overview
An 'RTable' is logically a container of 'RTuple's (similar to the concept of a Relation being a set of Tuples) and is the core data type in this 
module. The 'RTuple' is a map of (Column-Name, Column-Value) pairs. A Column-Name is modeled with the 'ColumnName' data type, while the 
Column-Value is modelled with the 'RDataType', which is a wrapper over the most common data types that one would expect to find in a column 
of a Table (e.g., integers, rational numbers, strings, dates etc.).

We said that the 'RTable' is a container of 'RTuple's and thus the 'RTable' is a 'Monad'! So one can write monadic code to implement RTable operations. For example:

 @
    -- | Return an new RTable after modifying each RTuple of the input RTable.
    myRTableOperation :: RTable -> RTable
    myRTableOperation rtab = do
            rtup <- rtab
            let new_rtup = doStuff rtup
            return new_rtup
        where
            doStuff :: RTuple -> RTuple
            doStuff = ...  -- to be defined
 @

Many different types of data can be turned into an 'RTable'. For example, CSV data can be easily turn into an 'RTable' via the 'toRTable' function. Many other types of data
could be represented as "tabular data" via the 'RTable' data type, as long as they adhere to the interface posed by the 'RTabular' type class. In other words, any data type
that we want to convert into an RTable and vice-versa, must become an instance of the 'RTabular' type class and thus define the basic 'toRTable'
and 'fromRTable' functions.

== An Example
In this example we read a CSV file with the use of the 'readCSV' function from the "RTable.Data.CSV" module. Then, with the use of the 'toRTable' function, implemented in the
'RTabular' instance of the 'CSV' data type, we convert the CSV file into an 'RTable'. The data of the CSV file consist of metadata from an imaginary Oracle database and each 
row represents an entry for a table stored in this database, with information (i.e., columns) pertaining to the owner of the table, the tablespace name, the status of the table 
and various statistics, such as the number of rows and number of blocks.

In this example, we apply three \"transformations\" to the input data and we print the result after each one, with the use of the 'printfRTable' function. The transfomrations
are: 

1. a 'limit' operation, where we return the first N number of 'RTuple's, 
2. an 'RFilter' operation that returns only the tables that start with a \'B\', followed by a projection operation ('RPrj')
3. an inner-join ('RInJoin'), where we pair the 'RTuple's from the previous results based on a join predicate ('RJoinPredicate'): the tables that have been analyzed the same day

Finally, we store the results of the 2nd operation into a new CSV file, with the use of the 'fromRTable' function implemented for the 'RTabular' instance of the 'CSV' data type.

@
{-# LANGUAGE OverloadedStrings #-}

import  RTable.Core
import  RTable.Data.CSV     (CSV, readCSV, toRTable)
import  Data.Text as T          (take, pack)

-- This is the input source table metadata
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
result_tab_MData = 
    createRTableMData   (   \"resultTab\"  -- table name
                            ,[  (\"OWNER\", Varchar)                                        -- Owner of the table
                                ,(\"TABLE_NAME\", Varchar)                                  -- Name of the table
                                ,(\"LAST_ANALYZED\", Timestamp \"MM/DD/YYYY HH24:MI:SS\")   -- Timestamp of the last time the table was analyzed (i.e., gathered statistics) 
                            ]
                        )
                        [\"OWNER\", \"TABLE_NAME\"] -- primary key
                        [] -- (alternative) unique keys


main :: IO()
main = do
     -- read source csv file
    srcCSV <- readCSV ".\/app\/test-data.csv"

    putStrLn "\\nHow many rows you want to print from the source table? :\\n"
    n <- readLn :: IO Int    
    
    -- RTable A
    printfRTable (  -- define the order by which the columns will appear on screen. Use the default column formatting.
                    genRTupleFormat [\"OWNER\", \"TABLE_NAME\", \"TABLESPACE_NAME\", \"STATUS\", \"NUM_ROWS\", \"BLOCKS\", \"LAST_ANALYZED\"] genDefaultColFormatMap) $ 
                        limit n $ toRTable src_DBTab_MData srcCSV 

    putStrLn "\\nThese are the tables that start with a \"B\":\\n"    
    
    -- RTable B
    printfRTable ( genRTupleFormat [\"OWNER\", \"TABLE_NAME\",\"LAST_ANALYZED\"] genDefaultColFormatMap) $ 
        tabs_start_with_B $ toRTable src_DBTab_MData srcCSV 
    
    putStrLn "\\nThese are the tables that were analyzed the same day:\\n"    
    
    -- RTable C = A InnerJoin B
    printfRTable ( genRTupleFormat [\"OWNER\", \"TABLE_NAME\", \"LAST_ANALYZED\", \"OWNER_1\", \"TABLE_NAME_1\", \"LAST_ANALYZED_1\"] genDefaultColFormatMap) $ 
        ropB  myJoin
                    (limit n $ toRTable src_DBTab_MData srcCSV) 
                    (tabs_start_with_B $ toRTable src_DBTab_MData srcCSV)

    -- save result of 2nd operation to CSV file
    writeCSV "./app/result-data.csv" $ 
                    fromRTable result_tab_MData $ 
                        tabs_start_with_B $ 
                            toRTable src_DBTab_MData srcCSV 

    where
        -- Return RTuples with a table_name starting with a 'B'
        tabs_start_with_B :: RTable -> RTable
        tabs_start_with_B rtab = (ropU myProjection) . (ropU myFilter) $ rtab
            where
                -- Create a Filter Operation to return only RTuples with table_name starting with a 'B'
                myFilter = RFilter (    \t ->   let 
                                                    tbname = case toText (t \<!\> \"TABLE_NAME\") of
                                                                Just t -> t
                                                                Nothing -> pack \"\"
                                                in (T.take 1 tbname) == (pack \"B\")
                                    )
                -- Create a Projection Operation that projects only two columns
                myProjection = RPrj [\"OWNER\", \"TABLE_NAME\", \"LAST_ANALYZED\"]

        -- Create an Inner Join for tables analyzed in the same day
        myJoin :: ROperation
        myJoin = RInJoin (  \t1 t2 -> 
                                let
                                    RTime {rtime = RTimestampVal {year = y1, month = m1, day = d1, hours24 = hh1, minutes = mm1, seconds = ss1}} = t1\<!\>\"LAST_ANALYZED\"
                                    RTime {rtime = RTimestampVal {year = y2, month = m2, day = d2, hours24 = hh2, minutes = mm2, seconds = ss2}} = t2\<!\>\"LAST_ANALYZED\"
                                in y1 == y2 && m1 == m2 && d1 == d2
                        )
@

And here is the output:

@
:l ./src/RTable/example.hs
:set -XOverloadedStrings
main
@

@
How many rows you want to print from the source table? :

10
---------------------------------------------------------------------------------------------------------------------------------
OWNER           TABLE_NAME                        TABLESPACE_NAME     STATUS     NUM_ROWS     BLOCKS     LAST_ANALYZED
~~~~~           ~~~~~~~~~~                        ~~~~~~~~~~~~~~~     ~~~~~~     ~~~~~~~~     ~~~~~~     ~~~~~~~~~~~~~
APEX_030200     SYS_IOT_OVER_71833                SYSAUX              VALID      0            0          06/08/2012 16:22:36
APEX_030200     WWV_COLUMN_EXCEPTIONS             SYSAUX              VALID      3            3          06/08/2012 16:22:33
APEX_030200     WWV_FLOWS                         SYSAUX              VALID      10           3          06/08/2012 22:01:21
APEX_030200     WWV_FLOWS_RESERVED                SYSAUX              VALID      0            0          06/08/2012 16:22:33
APEX_030200     WWV_FLOW_ACTIVITY_LOG1$           SYSAUX              VALID      1            29         07/20/2012 19:07:57
APEX_030200     WWV_FLOW_ACTIVITY_LOG2$           SYSAUX              VALID      14           29         07/20/2012 19:07:57
APEX_030200     WWV_FLOW_ACTIVITY_LOG_NUMBER$     SYSAUX              VALID      1            3          07/20/2012 19:08:00
APEX_030200     WWV_FLOW_ALTERNATE_CONFIG         SYSAUX              VALID      0            0          06/08/2012 16:22:33
APEX_030200     WWV_FLOW_ALT_CONFIG_DETAIL        SYSAUX              VALID      0            0          06/08/2012 16:22:33
APEX_030200     WWV_FLOW_ALT_CONFIG_PICK          SYSAUX              VALID      37           3          06/08/2012 16:22:33


10 rows returned
---------------------------------------------------------------------------------------------------------------------------------

These are the tables that start with a "B":

-------------------------------------------------------------
OWNER      TABLE_NAME                LAST_ANALYZED
~~~~~      ~~~~~~~~~~                ~~~~~~~~~~~~~
DBSNMP     BSLN_BASELINES            04/15/2018 16:14:51
DBSNMP     BSLN_METRIC_DEFAULTS      06/08/2012 16:06:41
DBSNMP     BSLN_STATISTICS           04/15/2018 17:41:33
DBSNMP     BSLN_THRESHOLD_PARAMS     06/08/2012 16:06:41
SYS        BOOTSTRAP$                04/14/2014 13:53:43


5 rows returned
-------------------------------------------------------------

These are the tables that were analyzed the same day:

-------------------------------------------------------------------------------------------------------------------------------------
OWNER           TABLE_NAME                     LAST_ANALYZED           OWNER_1     TABLE_NAME_1              LAST_ANALYZED_1
~~~~~           ~~~~~~~~~~                     ~~~~~~~~~~~~~           ~~~~~~~     ~~~~~~~~~~~~              ~~~~~~~~~~~~~~~
APEX_030200     SYS_IOT_OVER_71833             06/08/2012 16:22:36     DBSNMP      BSLN_THRESHOLD_PARAMS     06/08/2012 16:06:41
APEX_030200     SYS_IOT_OVER_71833             06/08/2012 16:22:36     DBSNMP      BSLN_METRIC_DEFAULTS      06/08/2012 16:06:41
APEX_030200     WWV_COLUMN_EXCEPTIONS          06/08/2012 16:22:33     DBSNMP      BSLN_THRESHOLD_PARAMS     06/08/2012 16:06:41
APEX_030200     WWV_COLUMN_EXCEPTIONS          06/08/2012 16:22:33     DBSNMP      BSLN_METRIC_DEFAULTS      06/08/2012 16:06:41
APEX_030200     WWV_FLOWS                      06/08/2012 22:01:21     DBSNMP      BSLN_THRESHOLD_PARAMS     06/08/2012 16:06:41
APEX_030200     WWV_FLOWS                      06/08/2012 22:01:21     DBSNMP      BSLN_METRIC_DEFAULTS      06/08/2012 16:06:41
APEX_030200     WWV_FLOWS_RESERVED             06/08/2012 16:22:33     DBSNMP      BSLN_THRESHOLD_PARAMS     06/08/2012 16:06:41
APEX_030200     WWV_FLOWS_RESERVED             06/08/2012 16:22:33     DBSNMP      BSLN_METRIC_DEFAULTS      06/08/2012 16:06:41
APEX_030200     WWV_FLOW_ALTERNATE_CONFIG      06/08/2012 16:22:33     DBSNMP      BSLN_THRESHOLD_PARAMS     06/08/2012 16:06:41
APEX_030200     WWV_FLOW_ALTERNATE_CONFIG      06/08/2012 16:22:33     DBSNMP      BSLN_METRIC_DEFAULTS      06/08/2012 16:06:41
APEX_030200     WWV_FLOW_ALT_CONFIG_DETAIL     06/08/2012 16:22:33     DBSNMP      BSLN_THRESHOLD_PARAMS     06/08/2012 16:06:41
APEX_030200     WWV_FLOW_ALT_CONFIG_DETAIL     06/08/2012 16:22:33     DBSNMP      BSLN_METRIC_DEFAULTS      06/08/2012 16:06:41
APEX_030200     WWV_FLOW_ALT_CONFIG_PICK       06/08/2012 16:22:33     DBSNMP      BSLN_THRESHOLD_PARAMS     06/08/2012 16:06:41
APEX_030200     WWV_FLOW_ALT_CONFIG_PICK       06/08/2012 16:22:33     DBSNMP      BSLN_METRIC_DEFAULTS      06/08/2012 16:06:41


14 rows returned
-------------------------------------------------------------------------------------------------------------------------------------
@

Check the output CSV file

@
$ head .\/app\/result-data.csv
OWNER,TABLE_NAME,LAST_ANALYZED
DBSNMP,BSLN_BASELINES,04/15/2018 16:14:51
DBSNMP,BSLN_METRIC_DEFAULTS,06/08/2012 16:06:41
DBSNMP,BSLN_STATISTICS,04/15/2018 17:41:33
DBSNMP,BSLN_THRESHOLD_PARAMS,06/08/2012 16:06:41
SYS,BOOTSTRAP$,04/14/2014 13:53:43
@

-}

{-# LANGUAGE OverloadedStrings #-}
-- :set -XOverloadedStrings
--  :set -XRecordWildCards
{-# LANGUAGE GeneralizedNewtypeDeriving  -- In order to be able to derive from non-standard derivable classes (such as Num)
            ,BangPatterns
            ,RecordWildCards 
            ,DeriveGeneric       -- Allow automatic deriving of instances for the Generic typeclass  (see Text.PrettyPrint.Tabulate.Example)
            ,DeriveDataTypeable  -- Enable automatic deriving of instances for the Data typeclass    (see Text.PrettyPrint.Tabulate.Example)
            {--
                :set -XDeriveGeneric
                :set -XDeriveDataTypeable
            --}
            -- Allow definition of type class instances for type synonyms. (used for RTuple instance of Tabulate)
            --,TypeSynonymInstances  
            --,FlexibleInstances
#-}  

-- {-# LANGUAGE  DuplicateRecordFields #-}

module RTable.Core ( 

    -- * The Relational Table Concept
    -- ** RTable Data Types
    RTable (..)
    ,RTuple (..)
    ,RDataType (..)
    ,RTimestamp (..)
    -- ** RTable Metadata Data Types
    ,RTableMData (..)
    ,RTupleMData (..)    
    ,ColumnInfo (..) 
    ,ColumnOrder   
    ,Name
    ,ColumnName
    ,RTableName
    ,ColumnDType (..)
    ,Delimiter

    -- * Type Classes for "Tabular Data"
    ,RTabular (..)

    -- * Relational Algebra Operations
    -- ** Operations Data Types
    ,ROperation (..)
    ,UnaryRTableOperation    
    ,BinaryRTableOperation    
    ,RAggOperation (..)
    -- *** Available Aggregate Operations
    ,AggFunction (..)
    ,raggGenericAgg
    ,raggSum
    ,raggCount
    ,raggCountDist
    ,raggCountStar
    ,raggAvg
    ,raggMax
    ,raggMin
    ,raggStrAgg    

    -- ** Predicates
    ,RPredicate
    ,RGroupPredicate
    ,RJoinPredicate
    ,RUpsertPredicate (..)

    -- ** Operation Execution
    ,runUnaryROperation
    ,ropU
    ,runUnaryROperationRes
    ,ropUres
    ,runBinaryROperation
    ,ropB  
    ,runBinaryROperationRes
    ,ropBres

    -- ** Operation Result
    ,RTuplesRet
    ,RTabResult
    ,rtabResult
    ,runRTabResult
    ,execRTabResult
    ,rtuplesRet
    ,getRTuplesRet
    -- ** Operation Composition
    {-|
    === An Example of Operation Composition
    >>> -- define a simple RTable with four RTuples of a single column "col1"
    >>> let tab1 = rtableFromList [rtupleFromList [("col1", RInt 1)], rtupleFromList [("col1", RInt 2)], rtupleFromList [("col1", RInt 3)], rtupleFromList [("col1", RInt 4)] ]

    >>>  printRTable tab1

    @
    col1
    ~~~~
    1
    2
    3
    4


    4 rows returned
    ---------
    @
    >>> -- define a filter operation col1 > 2
    >>> let rop1 = RFilter (\t-> t<!>"col1" > 2)  

    >>> -- define another filter operation col1 > 3
    >>> let rop2 = RFilter (\t-> t<!>"col1" > 3)  

    >>> -- Composition of RTable operations via (.) (rop1 returns 2 RTuples and rop2 returns 1 RTuple)
    >>> printRTable $ (ropU rop2) . (ropU rop1) $ tab1

    @
    col1
    ~~~~
    4


    1 row returned
    ---------
    @    
    >>> -- Composition of RTabResult operations via (<=<) (Note: that the result includes the sum of the returned RTuples in each operation, i.e., 2+1 = 3)
    >>>  execRTabResult $ (ropUres rop2) <=< (ropUres rop1) $ tab1
    Sum {getSum = 3}
    >>> printRTable $ fst.runRTabResult $ (ropUres rop2) <=< (ropUres rop1) $ tab1

    @
    col1
    ~~~~
    4


    1 row returned
    ---------
    @
    -}
    , (.)
    , (<=<)

    -- * RTable Functions
    -- ** Relational Algebra Functions
    ,runRfilter
    ,f
    ,runInnerJoinO
    ,iJ
    ,runLeftJoin
    ,lJ
    ,runRightJoin
    ,rJ
    ,runFullOuterJoin
    ,foJ
    ,sJ
    ,runSemiJoin
    ,aJ
    ,runAntiJoin
    ,joinRTuples    
    ,runUnion
    ,runUnionAll
    ,u
    ,runIntersect
    ,i
    ,runDiff
    ,d 
    ,runProjection
    ,runProjectionMissedHits
    ,p   
    ,runAggregation
    ,rAgg
    ,runGroupBy
    ,rG
    ,groupNoAggList
    ,groupNoAgg
    ,runOrderBy 
    ,rO
    ,runCombinedROp 
    ,rComb
    -- ** Decoding
    ,IgnoreDefault (..)    
    ,decodeRTable
    ,decodeColValue
    -- ** Date/Time
    ,toRTimestamp
    ,createRTimestamp    
    ,rTimestampToRText
    ,stdTimestampFormat
    ,stdDateFormat
    -- ** Character/Text  
    ,instrRText
    ,instr
    ,instrText
    ,rdtappend  
    ,stripRText
    ,removeCharAroundRText
    ,isText
    -- ** NULL-Related
    ,nvlRTable        
    ,nvlRTuple    
    ,isNullRTuple        
    ,isNull
    ,isNotNull
    ,nvl
    ,nvlColValue
    -- ** Access RTable
    ,isRTabEmpty
    ,headRTup    
    ,limit
    --,restrictNrows        
    ,isRTupEmpty
    ,getRTupColValue        
    ,rtupLookup
    ,rtupLookupDefault
    , (<!>)
    , (<!!>)

    -- ** Conversions
    ,rtableToList
    ,concatRTab
    ,rtupleToList
    ,toListRDataType
    ,toText
    ,fromText    
    -- ** Container Functions
    ,rtabMap    
    ,rtabFoldr'
    ,rtabFoldl'
    ,rtupleMap
    ,rtupleMapWithKey
    ,rdatatypeFoldr'
    ,rdatatypeFoldl'
    -- * Modify RTable (DML)
    ,insertAppendRTab    
    ,insertPrependRTab
    ,insertRTabToRTab
    ,deleteRTab    
    ,updateRTab    
    ,upsertRTab  
    ,updateRTuple
    ,upsertRTuple    
    -- * Create/Alter RTable (DDL)
    ,emptyRTable
    ,createSingletonRTable
    ,rtableFromList    
    ,addColumn    
    ,removeColumn    
    ,emptyRTuple
    ,createNullRTuple
    ,createRTuple
    ,rtupleFromList    
    ,createRDataType
    -- * Metadata Functions
    ,createRTableMData    
    ,getColumnNamesFromRTab
    ,getColumnNamesFromRTuple
    ,getColumnInfoFromRTab
    ,getColumnInfoFromRTuple
    ,getTheType
    ,listOfColInfoRDataType
    ,toListColumnName
    ,toListColumnInfo
    ,rtabsSameStructure
    ,rtuplesSameStructure
    ,getUniqueColumnNamesAfterJoin
    -- * Exceptions
    ,ColumnDoesNotExist (..)
    ,ConflictingRTableStructures (..)
    ,EmptyInputStringsInToRTimestamp (..)    
    ,UnsupportedTimeStampFormat (..)
    ,UniquenessViolationInUpsert (..)
    --,RTimestampFormatLengthMismatch (..)

    -- * RTable IO Operations
    -- ** RTable Printing and Formatting
    {-|
    === An Example of RTable printing
    >>> -- define a simple RTable from a list
    >>> :set -XOverloadedStrings
    >>> :{    
    let tab1 =  rtableFromList [   rtupleFromList [("ColInteger", RInt 1), ("ColDouble", RDouble 2.3), ("ColText", RText "We dig dig dig dig dig dig dig")]
                            ,rtupleFromList [("ColInteger", RInt 2), ("ColDouble", RDouble 5.36879), ("ColText", RText "From early morn to night")]
                            ,rtupleFromList [("ColInteger", RInt 3), ("ColDouble", RDouble 999.9999), ("ColText", RText "In a mine the whole day through")]
                            ,rtupleFromList [("ColInteger", RInt 4), ("ColDouble", RDouble 0.9999), ("ColText", RText "Is what we like to do")]
                          ]
    :}                          
    >>> -- print without format specification
    >>> printRTable tab1

    @
    -----------------------------------------------------------------
    ColInteger     ColText                             ColDouble
    ~~~~~~~~~~     ~~~~~~~                             ~~~~~~~~~
    1              We dig dig dig dig dig dig dig      2.30
    2              From early morn to night            5.37
    3              In a mine the whole day through     1000.00
    4              Is what we like to do               1.00

    4 rows returned
    -----------------------------------------------------------------
    @
    >>> -- print with format specification (define column printing order and value formatting per column)
    >>> printfRTable (genRTupleFormat ["ColInteger","ColDouble","ColText"] $ genColFormatMap [("ColInteger", Format "%d"),("ColDouble", Format "%1.1e"),("ColText", Format "%50s\n")]) tab1
    
    @
    -----------------------------------------------------------------
    ColInteger     ColDouble     ColText
    ~~~~~~~~~~     ~~~~~~~~~     ~~~~~~~
    1              2.3e0                             We dig dig dig dig dig dig dig

    2              5.4e0                                   From early morn to night

    3              1.0e3                            In a mine the whole day through

    4              1.0e0                                      Is what we like to do


    4 rows returned
    -----------------------------------------------------------------    
    @
    -}    
    ,printRTable
    ,eitherPrintRTable
    ,printfRTable
    ,eitherPrintfRTable
    ,RTupleFormat (..)
    ,ColFormatMap
    ,FormatSpecifier (..)
    ,OrderingSpec (..)
    ,genRTupleFormat
    ,genRTupleFormatDefault    
    ,genColFormatMap
    ,genDefaultColFormatMap    

    ) where

import Debug.Trace

-- Data.Serialize (Cereal package)  
--                                  https://hackage.haskell.org/package/cereal
--                                  https://hackage.haskell.org/package/cereal-0.5.4.0/docs/Data-Serialize.html
--                                  http://stackoverflow.com/questions/2283119/how-to-convert-a-integer-to-a-bytestring-in-haskell
import Data.Serialize (decode, encode)

-- Vector
import qualified Data.Vector as V      

-- HashMap                          -- https://hackage.haskell.org/package/unordered-containers-0.2.7.2/docs/Data-HashMap-Strict.html
import Data.HashMap.Strict as HM

-- Text
import Data.Text as T

--import Data.Text.IO as TIO 

-- ByteString
import qualified Data.ByteString as BS

-- Typepable                        -- https://hackage.haskell.org/package/base-4.9.1.0/docs/Data-Typeable.html
                                    -- http://stackoverflow.com/questions/6600380/what-is-haskells-data-typeable
                                    -- http://alvinalexander.com/source-code/haskell/how-determine-type-object-haskell-program
import qualified Data.Typeable as TB --(typeOf, Typeable)

-- Dynamic
import qualified Data.Dynamic as D  -- https://hackage.haskell.org/package/base-4.9.1.0/docs/Data-Dynamic.html

-- Data.List
import Data.List (find, filter, last, all, elem, break, span, map, null, zip, zipWith, elemIndex, sortOn, union, intersect, (\\), take, length, repeat, groupBy, sort, sortBy, foldl', foldr, foldr1, foldl',head, findIndex, tails, isPrefixOf)
-- Data.Maybe
import Data.Maybe (fromJust, fromMaybe)
-- Data.Char
import Data.Char (toUpper,digitToInt, isDigit, isAlpha)
-- Data.Monoid
import Data.Monoid as M
-- Control.Monad
import Control.Monad ((<=<))
-- Control.Monad.Trans.Writer.Strict
import Control.Monad.Trans.Writer.Strict (Writer, writer, runWriter, execWriter)
-- Text.Printf
import Text.Printf      (printf)

import Control.Exception

import GHC.Generics     (Generic)
import Control.DeepSeq
import Data.String.Utils (replace)

-- import Control.Monad.IO.Class (liftIO)


{--- Text.PrettyPrint.Tabulate
import qualified Text.PrettyPrint.Tabulate as PP
import qualified GHC.Generics as G
import Data.Data
-}
--import qualified Text.PrettyPrint.Boxes as BX
--import Data.Map (fromList)


--import qualified Data.Map.Strict as Map -- Data.Map.Strict  https://www.stackage.org/haddock/lts-7.4/containers-0.5.7.1/Data-Map-Strict.html
--import qualified Data.Set as Set        -- https://www.stackage.org/haddock/lts-7.4/containers-0.5.7.1/Data-Set.html#t:Set
--import qualified Data.ByteString as BS  -- Data.ByteString  https://www.stackage.org/haddock/lts-7.4/bytestring-0.10.8.1/Data-ByteString.html

{--
-- | Definition of the Relation entity
data Relation 
--    = RelToBeDefined deriving (Show)
    =  Relation -- ^ A Relation is essentially a set of tuples
                {   
                    relname :: String  -- ^ The name of the Relation (metadata)
                    ,fields :: [RelationField]   -- ^ The list of fields (i.e., attributes) of the relation (metadata)
                    ,tuples ::   Set.Set Rtuple     -- ^ A relation is essentially a set o tuples (data)
                }
    |  EmptyRel -- ^ An empty relation
    deriving Show
--}


myRTableOperation :: RTable -> RTable
myRTableOperation rtab = do
       rtup <- rtab
       let new_rtup = doStuff rtup
       return new_rtup
   where
       doStuff :: RTuple -> RTuple
       doStuff = undefined  

-- * ########## Type Classes ##############

-- | Basic class to represent a data type that can be turned into an 'RTable'.
-- It implements the concept of "tabular data" 
class RTabular a where 
    
    toRTable :: RTableMData -> a -> RTable
    
    fromRTable :: RTableMData -> RTable -> a
    
    {-# MINIMAL toRTable, fromRTable #-}

-- * ########## Data Types ##############

-- | Definition of the Relational Table entity
--   An 'RTable' is a "container" of 'RTuple's.
type RTable = V.Vector RTuple 


-- | Definition of the Relational Tuple.
--   An 'RTuple' is implemented as a 'HashMap' of ('ColumnName', 'RDataType') pairs. This ensures fast access of the column value by column name.
--   Note that this implies that the 'RTuple' CANNOT have more than one columns with the same name (i.e. hashmap key) and more importantly that
--   it DOES NOT have a fixed order of columns, as it is usual in RDBMS implementations.
--   This gives us the freedom to perform column change operations very fast.
--   The only place were we need fixed column order is when we try to load an 'RTable' from a fixed-column structure such as a CSV file.
--   For this reason, we have embedded the notion of a fixed column-order in the 'RTuple' metadata. See 'RTupleMData'.
--   
type RTuple = HM.HashMap ColumnName RDataType

-- | Turns an 'RTable' to a list of 'RTuple's
rtableToList :: RTable -> [RTuple]
rtableToList = V.toList

-- | Creates an RTable from a list of RTuples
rtableFromList :: [RTuple] -> RTable
rtableFromList = V.fromList

-- | Turns an RTuple to a List
rtupleToList :: RTuple -> [(ColumnName, RDataType)]
rtupleToList = HM.toList

-- | Create an RTuple from a list
rtupleFromList :: [(ColumnName, RDataType)] -> RTuple 
rtupleFromList = HM.fromList

{-
instance Data RTuple
instance G.Generic RTuple
instance PP.Tabulate RTuple
-}

-- | Definition of the Name type
type Name = Text

-- | Definition of the Column Name
type ColumnName = Name

-- instance PP.Tabulate ColumnName

-- | Definition of the Table Name
type RTableName = Name

-- | This is used only for metadata purposes (see 'ColumnInfo'). The actual data type of a value is an RDataType
-- The Text component of Date and Timestamp data constructors is the date format e.g., "DD\/MM\/YYYY", "DD\/MM\/YYYY HH24:MI:SS"
data ColumnDType = UknownType | Integer | Varchar | Date Text | Timestamp Text | Double  deriving (Show, Eq)

-- | Definition of the Relational Data Type. This is the data type of the values stored in each 'RTable'.
-- This is a strict data type, meaning whenever we evaluate a value of type 'RDataType', 
-- there must be also evaluated all the fields it contains.
data RDataType = 
      RInt { rint :: !Integer }
    -- RChar { rchar :: Char }
      | RText { rtext :: !T.Text }
    -- RString {rstring :: [Char]}
      | RDate { 
                rdate :: !T.Text
               ,dtformat :: !Text  -- ^ e.g., "DD\/MM\/YYYY"
            }
      | RTime { rtime :: !RTimestamp  }
      | RDouble { rdouble :: !Double }
    -- RFloat  { rfloat :: Float }
      | Null
      deriving (Show,TB.Typeable, Read, Generic)   -- http://stackoverflow.com/questions/6600380/what-is-haskells-data-typeable

-- | In order to be able to force full evaluation up to Normal Form (NF)
-- https://www.fpcomplete.com/blog/2017/09/all-about-strictness
instance NFData RDataType


-- | We need to explicitly specify equation of RDataType due to SQL NULL logic (i.e., anything compared to NULL returns false):
-- @
-- Null == _ = False,
-- _ == Null = False,
-- Null /= _ = False,
-- _ /= Null = False.
-- @
-- IMPORTANT NOTE:
-- Of course this means that anywhere in your code where you have something like this: 
-- @
-- x == Null or x /= Null, 
-- @
-- will always return False and thus it is futile to do this comparison. 
-- You have to use the is 'isNull' function instead.
--
instance Eq RDataType where

    RInt i1 == RInt i2 = i1 == i2
    -- RInt i == _ = False
    RText t1 == RText t2 = t1 == t2
    -- RText t1 == _ = False
    RDate t1 s1 == RDate t2 s2 = toRTimestamp (unpack s1) (unpack t1) == toRTimestamp (unpack s2) (unpack t2)   -- (t1 == t1) && (s1 == s2)
    -- RDate t1 s1 == _ = False
    RTime t1 == RTime t2 = t1 == t2
    -- RTime t1 == _ = False
    RDouble d1 == RDouble d2 = d1 == d2
    -- RDouble d1 == _ = False
    -- Watch out: NULL logic (anything compared to NULL returns false)
    Null == Null = False
    _ == Null = False
    Null == _ = False
    -- anything else is just False
    _ == _ = False

    Null /= Null = False
    _ /= Null = False
    Null /= _ = False
    x /= y = not (x == y) 

-- Need to explicitly specify due to "Null logic" (see Eq)
instance Ord RDataType where    
    compare Null _ = GT     --    Null <= _ = False
    compare _ Null = GT     --  _ <= Null = False
    -- Null <= Null = False -- Comment out due to redundant warning
    compare (RInt i1) (RInt i2) = compare i1 i2                                             --  RInt i1 <= RInt i2 = i1 <= i2
    compare (RText t1) (RText t2) = compare t1 t2                                           --  RText t1 <= RText t2 = t1 <= t2
    compare (RDate t1 s1) (RDate t2 s2) = compare (toRTimestamp (unpack s1) (unpack t1)) (toRTimestamp (unpack s2) (unpack t2)) --  RDate t1 s1 <= RDate t2 s2 = (t1 <= t1) && (s1 == s2)
    compare (RTime t1) (RTime t2) = compare t1 t2                                           --  RTime t1 <= RTime t2 = t1 <= t2
    -- RTime t1 <= _ = False
    compare (RDouble d1) (RDouble d2) = compare d1 d2                                       --  RDouble d1 <= RDouble d2 = d1 <= d2
    -- anything else is just False
    compare _ _ = GT                                                                        --  _ <= _ = False


-- | Use this function to compare an RDataType with the Null value because due to Null logic
--  x == Null or x /= Null, will always return False.
-- It returns True if input value is Null
isNull :: RDataType -> Bool
isNull x = 
    case x of
        Null -> True
        _    -> False

-- | Use this function to compare an RDataType with the Null value because deu to Null logic
--  x == Null or x /= Null, will always return False.
-- It returns True if input value is Not Null
isNotNull = not . isNull

instance Num RDataType where
    (+) (RInt i1) (RInt i2) = RInt (i1 + i2)
    (+) (RDouble d1) (RDouble d2) = RDouble (d1 + d2)
    (+) (RDouble d1) (RInt i2) = RDouble (d1 + fromIntegral i2)
    (+) (RInt i1) (RDouble d2) = RDouble (fromIntegral i1 + d2)
    -- (+) (RInt i1) (Null) = RInt i1  -- ignore Null - just like in SQL
    -- (+) (Null) (RInt i2) = RInt i2  -- ignore Null - just like in SQL
    -- (+) (RDouble d1) (Null) = RDouble d1  -- ignore Null - just like in SQL
    -- (+) (Null) (RDouble d2) = RDouble d2  -- ignore Null - just like in SQL    
    (+) (RInt i1) (Null) = Null
    (+) (Null) (RInt i2) = Null
    (+) (RDouble d1) (Null) = Null
    (+) (Null) (RDouble d2) = Null
    (+) _ _ = Null
    (*) (RInt i1) (RInt i2) = RInt (i1 * i2)
    (*) (RDouble d1) (RDouble d2) = RDouble (d1 * d2)
    (*) (RDouble d1) (RInt i2) = RDouble (d1 * fromIntegral i2)
    (*) (RInt i1) (RDouble d2) = RDouble (fromIntegral i1 * d2)
    -- (*) (RInt i1) (Null) = RInt i1  -- ignore Null - just like in SQL
    -- (*) (Null) (RInt i2) = RInt i2  -- ignore Null - just like in SQL
    -- (*) (RDouble d1) (Null) = RDouble d1  -- ignore Null - just like in SQL
    -- (*) (Null) (RDouble d2) = RDouble d2  -- ignore Null - just like in SQL
    (*) (RInt i1) (Null) = Null
    (*) (Null) (RInt i2) = Null
    (*) (RDouble d1) (Null) = Null
    (*) (Null) (RDouble d2) = Null
    (*) _ _ = Null
    abs (RInt i) = RInt (abs i)
    abs (RDouble i) = RDouble (abs i)
    abs _ = Null
    signum (RInt i) = RInt (signum i)
    signum (RDouble i) = RDouble (signum i)
    signum _ = Null
    fromInteger i = RInt i
    negate (RInt i) = RInt (negate i)
    negate (RDouble i) = RDouble (negate i)
    negate _ = Null

-- | In order to be able to use (/) with RDataType
instance Fractional RDataType where
    (/) (RInt i1) (RInt i2) = RDouble $ (fromIntegral i1)/(fromIntegral i2)
    (/) (RDouble d1) (RInt i2) = RDouble $ (d1)/(fromIntegral i2)
    (/) (RInt i1) (RDouble d2) = RDouble $ (fromIntegral i1)/(d2)
    (/) (RDouble d1) (RDouble d2) = RDouble $ d1/d2
    (/) _ _ = Null

    -- In order to be able to turn a Rational number into an RDataType, e.g. in the case: totamnt / 12.0
    -- where totamnt = RDouble amnt
    -- fromRational :: Rational -> a
    fromRational r = RDouble (fromRational r)

-- | Standard date format
stdDateFormat = "DD/MM/YYYY"

-- | Get the Column Names of an RTable
getColumnNamesFromRTab :: RTable -> [ColumnName]
getColumnNamesFromRTab rtab = getColumnNamesFromRTuple $ headRTup rtab

-- | Returns the Column Names of an RTuple
getColumnNamesFromRTuple :: RTuple -> [ColumnName]
getColumnNamesFromRTuple t = HM.keys t

-- Get the column metadata of an 'RTable'
getColumnInfoFromRTab :: RTable -> [ColumnInfo]
getColumnInfoFromRTab rtab = getColumnInfoFromRTuple $ headRTup rtab

-- Get the column metadata of an 'RTuple'
getColumnInfoFromRTuple :: RTuple -> [ColumnInfo]
getColumnInfoFromRTuple  t = 
   HM.elems $ HM.mapWithKey (\c v -> ColumnInfo { name = c, dtype = getTheType v}) t 

-- | Take a column value and return its type
getTheType :: RDataType -> ColumnDType
getTheType v = 
    case v of
        RInt _                           ->  Integer
        RText _                          ->  Varchar
        RDate {rdate = d, dtformat = f}  ->  Date f 
        RTime _                          ->  Timestamp (pack stdTimestampFormat)
        RDouble _                        ->  Double
        Null                             ->  UknownType

-- | Get the first RTuple from an RTable
headRTup ::
        RTable
    ->  RTuple
headRTup = V.head    

-- | Returns the value of an RTuple column based on the ColumnName key
--   if the column name is not found, then it returns Nothing
rtupLookup ::
       ColumnName    -- ^ ColumnName key
    -> RTuple        -- ^ Input RTuple
    -> Maybe RDataType     -- ^ Output value
rtupLookup =  HM.lookup

-- | Returns the value of an RTuple column based on the ColumnName key
--   if the column name is not found, then it returns a default value
rtupLookupDefault ::
       RDataType     -- ^ Default value to return in the case the column name does not exist in the RTuple
    -> ColumnName    -- ^ ColumnName key
    -> RTuple        -- ^ Input RTuple
    -> RDataType     -- ^ Output value
rtupLookupDefault =  HM.lookupDefault


-- | getRTupColValue :: Returns the value of an RTuple column based on the ColumnName key
--   if the column name is not found, then it returns Null.
--   !!!Note that this might be confusing since there might be an existing column name with a Null value!!!
getRTupColValue ::
       ColumnName    -- ^ ColumnName key
    -> RTuple        -- ^ Input RTuple
    -> RDataType     -- ^ Output value
getRTupColValue =  rtupLookupDefault Null  -- HM.lookupDefault Null


-- | Operator for getting a column value from an RTuple
--   Throws a 'ColumnDoesNotExist' exception, if this map contains no mapping for the key.
(<!>) ::
       RTuple        -- ^ Input RTuple
    -> ColumnName    -- ^ ColumnName key
    -> RDataType     -- ^ Output value
(<!>) t c = -- flip getRTupColValue
       -- (HM.!)
       case rtupLookup c t of
            Just v -> v
            Nothing -> throw $ ColumnDoesNotExist c  
                       -- error $ "*** Error in function Data.RTable.(<!>): Column \"" ++ (T.unpack c) ++ "\" does not exist! ***" 


-- | Safe Operator for getting a column value from an RTuple
--   if the column name is not found, then it returns Nothing
(<!!>) ::
       RTuple        -- ^ Input RTuple
    -> ColumnName    -- ^ ColumnName key
    -> Maybe RDataType     -- ^ Output value
(<!!>) t c = rtupLookup c t 

-- | Returns the 1st parameter if this is not Null, otherwise it returns the 2nd. 
nvl ::
       RDataType  -- ^ input value
    -> RDataType  -- ^ default value returned if input value is Null
    -> RDataType  -- ^ output value
nvl v defaultVal = 
    if isNull v
        then defaultVal
        else v

-- | Returns the value of a specific column (specified by name) if this is not Null. 
-- If this value is Null, then it returns the 2nd parameter.
-- If you pass an empty RTuple, then it returns Null.
-- Throws a 'ColumnDoesNotExist' exception, if this map contains no mapping for the key.
nvlColValue ::
        ColumnName  -- ^ ColumnName key
    ->  RDataType   -- ^ value returned if original value is Null
    ->  RTuple      -- ^ input RTuple
    ->  RDataType   -- ^ output value
nvlColValue col defaultVal tup = 
    if isRTupEmpty tup
        then Null
        else 
            case tup <!> col of
                Null   -> defaultVal
                val    -> val 

data IgnoreDefault = Ignore | NotIgnore deriving (Eq, Show)

-- | It receives an RTuple and lookups the value at a specfic column name.
-- Then it compares this value with the specified search value. If it is equal to the search value
-- then it returns the specified Return Value. If not, then it returns the specified default Value, if the ignore indicator is not set,
-- otherwise (if the ignore indicator is set) it returns the existing value.
-- If you pass an empty RTuple, then it returns Null.
-- Throws a 'ColumnDoesNotExist' exception, if this map contains no mapping for the key.
decodeColValue ::
        ColumnName  -- ^ ColumnName key
    ->  RDataType   -- ^ Search value
    ->  RDataType   -- ^ Return value
    ->  RDataType   -- ^ Default value   
    ->  IgnoreDefault -- ^ Ignore default indicator     
    ->  RTuple      -- ^ input RTuple
    ->  RDataType
decodeColValue cname searchVal returnVal defaultVal ignoreInd tup = 
    if isRTupEmpty tup
        then Null
        else 
{-
            case tup <!> cname of
                searchVal   -> returnVal
                v           -> if ignoreInd == Ignore then v else defaultVal 
-}
            if tup <!> cname == searchVal
                then returnVal
                else
                    if ignoreInd == Ignore
                        then tup <!> cname
                        else defaultVal

-- | It receives an RTuple and a default value. It returns a new RTuple which is identical to the source one
-- but every Null value in the specified colummn has been replaced by a default value
nvlRTuple ::
        ColumnName  -- ^ ColumnName key
    ->  RDataType   -- ^ Default value in the case of Null column values
    ->  RTuple      -- ^ input RTuple    
    ->  RTuple      -- ^ output RTuple
nvlRTuple c defaultVal tup  = 
    if isRTupEmpty tup
       then emptyRTuple
       else HM.map (\v -> nvl v defaultVal) tup


-- | It receives an RTable and a default value. It returns a new RTable which is identical to the source one
-- but for each RTuple, for the specified column every Null value in every RTuple has been replaced by a default value
-- If you pass an empty RTable, then it returns an empty RTable
-- Throws a 'ColumnDoesNotExist' exception, if the column does not exist
nvlRTable ::
        ColumnName  -- ^ ColumnName key
    ->  RDataType -- ^ Default value        
    ->  RTable    -- ^ input RTable
    ->  RTable
nvlRTable c defaultVal tab  = 
    if isRTabEmpty tab
        then emptyRTable
        else
            V.map (\t -> upsertRTuple c (nvlColValue c defaultVal t) t) tab
            --V.map (\t -> nvlRTuple c defaultVal t) tab    

-- | It receives an RTable, a search value and a default value. It returns a new RTable which is identical to the source one
-- but for each RTuple, for the specified column:
-- if the search value was found then the specified Return Value is returned
-- else the default value is returned  (if the ignore indicator is not set), otherwise (if the ignore indicator is set),
-- it returns the existing value for the column for each 'RTuple'. 
-- If you pass an empty RTable, then it returns an empty RTable
-- Throws a 'ColumnDoesNotExist' exception, if the column does not exist
decodeRTable ::            
        ColumnName  -- ^ ColumnName key
    ->  RDataType   -- ^ Search value
    ->  RDataType   -- ^ Return value
    ->  RDataType   -- ^ Default value        
    ->  IgnoreDefault -- ^ Ignore default indicator     
    ->  RTable      -- ^ input RTable
    ->  RTable
decodeRTable cName searchVal returnVal defaultVal ignoreInd tab = 
    if isRTabEmpty tab
        then emptyRTable
        else
            V.map (\t -> upsertRTuple cName (decodeColValue cName searchVal returnVal defaultVal ignoreInd t) t) tab   

-- newtype NumericRDT = NumericRDT { getRDataType :: RDataType } deriving (Eq, Ord, Read, Show, Num)


-- | stripRText : O(n) Remove leading and trailing white space from a string.
-- If the input RDataType is not an RText, then Null is returned
stripRText :: 
           RDataType  -- ^ input string
        -> RDataType
stripRText (RText t) = RText $ T.strip t
stripRText _ = Null

-- | Concatenates two Text 'RDataTypes', in all other cases of 'RDataType' it returns 'Null'.
rdtappend :: 
    RDataType 
    -> RDataType
    -> RDataType
rdtappend (RText t1) (RText t2) = RText (t1 `T.append` t2)
rdtappend _ _ = Null


-- | Helper function to remove a character around (from both beginning and end) of an (RText t) value
removeCharAroundRText :: Char -> RDataType -> RDataType
removeCharAroundRText ch (RText t) = RText $ T.dropAround (\c -> c == ch) t
removeCharAroundRText ch _ = Null

-- | Basic data type to represent time.
-- This is a strict data type, meaning whenever we evaluate a value of type 'RTimestamp', 
-- there must be also evaluated all the fields it contains.
data RTimestamp = RTimestampVal {
            year :: !Int
            ,month :: !Int
            ,day :: !Int
            ,hours24 :: !Int
            ,minutes :: !Int
            ,seconds :: !Int
        } deriving (Show, Read, Generic)


-- | In order to be able to force full evaluation up to Normal Form (NF)
instance NFData RTimestamp


instance Eq RTimestamp where
        RTimestampVal y1 m1 d1 h1 mi1 s1 ==  RTimestampVal y2 m2 d2 h2 mi2 s2 = 
            y1 == y2 && m1 == m2 && d1 == d2 && h1 == h2 && mi1 == mi2 && s1 == s2

instance Ord RTimestamp where
    -- compare :: a -> a -> Ordering
    compare (RTimestampVal y1 m1 d1 h1 mi1 s1) (RTimestampVal y2 m2 d2 h2 mi2 s2) = 
        if compare y1 y2 /= EQ 
            then compare y1 y2
            else 
                if compare m1 m2 /= EQ 
                    then compare m1 m2
                    else if compare d1 d2 /= EQ
                            then compare d1 d2
                            else if compare h1 h2 /= EQ
                                    then compare h1 h2
                                    else if compare mi1 mi2 /= EQ
                                            then compare mi1 mi2
                                            else if compare s1 s2 /= EQ
                                                    then compare s1 s2
                                                    else EQ


-- | Returns an 'RTimestamp' from an input 'String' and a format 'String'.
--
-- Valid format patterns are:
--
-- * For year: @YYYY@, e.g., @"0001"@, @"2018"@
-- * For month: @MM@, e.g., @"01"@, @"1"@, @"12"@
-- * For day: @DD@, e.g.,  @"01"@, @"1"@, @"31"@
-- * For hours: @HH@, @HH24@ e.g., @"00"@, @"23"@ I.e., hours must be specified in 24 format
-- * For minutes: @MI@, e.g., @"01"@, @"1"@, @"59"@
-- * For seconds: @SS@, e.g., @"01"@, @"1"@, @"59"@
--
-- Example of a typical format string is: @"DD\/MM\/YYYY HH:MI:SS@
-- 
-- If no valid format pattern is found then an 'UnsupportedTimeStampFormat' exception is thrown
--
toRTimestamp ::
    String      -- ^ Format string e.g., "DD\/MM\/YYYY HH:MI:SS"
    -> String   -- ^ Timestamp string
    -> RTimestamp
toRTimestamp fmt stime = 
    -- if (Data.List.length fmt) /= (Data.List.length stime)
    --     then throw $ RTimestampFormatLengthMismatch fmt stime
    --     else
    if fmt == [] || stime == []
        then throw $ EmptyInputStringsInToRTimestamp fmt stime 
        else 
            let 
                -- replace HH24 to HH
                formatSpec = Data.String.Utils.replace "HH24" "HH" 

            ------ New logic
 
                -- build a hashmap of "format elements" to "time elements"
                elemMap = parseFormat2 fmt stime HM.empty

                -- year
                y = case HM.lookup "YYYY" elemMap of
                        Nothing -> 1 :: Int 
                        Just yyyy  ->   (abs $ (digitToInt $ (yyyy !! 0)) * 1000)
                                        +   (abs $ (digitToInt $ (yyyy !! 1)) * 100) 
                                        +   (abs $ (digitToInt $ (yyyy !! 2)) * 10)
                                        +   (abs $ digitToInt $ (yyyy !! 3))
                -- round to 1 - 9999
                year = case y `rem` 9999 of 
                    0 -> 9999
                    yv -> yv

                -- month
                mo = case HM.lookup "MM" elemMap of
                        Nothing ->  1 :: Int
                        Just mm  ->  -- Also take care the case where mm < 10 and is not given as two digits e.g., '03' but '3'
                                    if Data.List.length mm == 1
                                        then (abs $ (digitToInt $ (mm !! 0)) * 1)
                                        else
                                            (abs $ (digitToInt $ (mm !! 0)) * 10)
                                            +  (abs $ digitToInt $ (mm !! 1)) 
                -- round to 1 - 12 values 
                month = case mo `rem` 12 of
                            0  -> 12
                            mv -> mv

                -- day                            
                d = case HM.lookup "DD" elemMap of
                        Nothing ->  1 :: Int
                        Just dd ->  -- Also take care the case where dd < 10 and is not given as two digits e.g., '03' but '3'
                                    if Data.List.length dd == 1
                                        then (abs $ (digitToInt $ (dd !! 0)) * 1)
                                        else
                                            (abs $ (digitToInt $ (dd !! 0)) * 10)
                                            +  (abs $ digitToInt $ (dd !! 1)) 
                -- round to 1 - 31 values 
                day = case d `rem` 31 of
                            0  -> 31
                            dv -> dv

                -- hour
                h = case HM.lookup "HH" elemMap of
                        Nothing ->  0 :: Int
                        Just hh ->  -- Also take care the case where hh < 10 and is not given as two digits e.g., '03' but '3'
                                    if Data.List.length hh == 1
                                        then (abs $ (digitToInt $ (hh !! 0)) * 1)
                                        else
                                            (abs $ (digitToInt $ (hh !! 0)) * 10)
                                            +  (abs $ digitToInt $ (hh !! 1)) 
                -- round to 0 - 23 values 
                hour = h `rem` 24 

                -- minutes
                m = case HM.lookup "MI" elemMap of
                        Nothing ->  0 :: Int
                        Just mi ->  -- Also take care the case where mi < 10 and is not given as two digits e.g., '03' but '3'
                                    if Data.List.length mi == 1
                                        then (abs $ (digitToInt $ (mi !! 0)) * 1)
                                        else
                                            (abs $ (digitToInt $ (mi !! 0)) * 10)
                                            +  (abs $ digitToInt $ (mi !! 1)) 
                -- round to 0 - 59 values 
                min = m `rem` 60 


                -- seconds
                s = case HM.lookup "SS" elemMap of
                        Nothing ->  0 :: Int
                        Just ss ->  -- Also take care the case where mi < 10 and is not given as two digits e.g., '03' but '3'
                                    if Data.List.length ss == 1
                                        then (abs $ (digitToInt $ (ss !! 0)) * 1)
                                        else
                                            (abs $ (digitToInt $ (ss !! 0)) * 10)
                                            +  (abs $ digitToInt $ (ss !! 1)) 
                -- round to 0 - 59 values 
                sec = s `rem` 60 


-------------- old logic
{-                -- parse format string and get positions of key timestamp format fields in the format string
                posmap = parseFormat formatSpec

                -- year
                posY = fromMaybe (-1) $ posmap ! "YYYY"
                y = case posY of
                    -1  ->  1 :: Int  
                    _   ->      (abs $ (digitToInt $ (stime !! posY)) * 1000)
                            +   (abs $ (digitToInt $ (stime !! (posY+1))) * 100) 
                            +   (abs $ (digitToInt $ (stime !! (posY+2))) * 10)
                            +   (abs $ digitToInt $ (stime !! (posY+3)))
                -- round to 1 - 9999
                year = case y `rem` 9999 of 
                    0 -> 9999
                    yv -> yv

                -- month
                posMO = fromMaybe (-1) $ posmap ! "MM"
                mo = case posMO of
                    -1  ->  1 :: Int  
                    _   ->     (abs $ (digitToInt $ (stime !! posMO)) * 10)
                            +  (abs $ digitToInt $ (stime !! (posMO+1))) 
                -- round to 1 - 12 values 
                month = case mo `rem` 12 of
                            0  -> 12
                            mv -> mv

                -- day
                posD = fromMaybe (-1) $ posmap ! "DD"
                d = case posD of
                    -1  ->  1 :: Int  
                    _   ->  (abs $ (digitToInt $ (stime !! posD)) * 10)
                            +  (abs $ digitToInt $ (stime !! (posD+1)))


                -- round to 1 - 31 values 
                day = case d `rem` 31 of
                            0  -> 31
                            dv -> dv

                -- hour
                posH = fromMaybe (-1) $ posmap ! "HH"
                h = case posH of
                    -1  ->  0 :: Int  
                    _   ->     (abs $ (digitToInt $ (stime !! posH)) * 10)
                            +  (abs $ digitToInt $ (stime !! (posH+1)))
                -- round to 0 - 23 values 
                hour = h `rem` 24 

                -- minutes
                posMI = fromMaybe (-1) $ posmap ! "MI"  -- subtract 2 positions due to 24 in "HH24"
                mi = case posMI of
                    -1  ->  0 :: Int  
                    _   ->     (abs $ (digitToInt $ (stime !! posMI)) * 10)
                            +  (abs $ digitToInt $ (stime !! (posMI+1))) 
                -- round to 0 - 59 values 
                min = mi `rem` 60 

                -- seconds
                posS = fromMaybe (-1) $ posmap ! "SS"
                s = case posS of
                    -1  ->  0 :: Int  
                    _   ->     (abs $ (digitToInt $ (stime !! posS)) * 10)
                            +  (abs $ digitToInt $ (stime !! (posS+1))) 
                -- round to 0 - 59 values 
                sec = s `rem` 60 
-}
            in RTimestampVal {
                                year = year
                                ,month = month
                                ,day = day
                                ,hours24 = hour
                                ,minutes = min
                                ,seconds = sec
                }
    where
        -- the map returns the position of the first character of the corresponding timestamp element
{-        parseFormat :: String -> HashMap String (Maybe Int)
        parseFormat fmt = 
            let 
                keywords = ["YYYY","MM", "DD", "HH", "MI", "SS"]
                positions = Data.List.map (\subs -> instr subs fmt) keywords
            in 
                -- if no keyword found then throw an exception
                if Data.List.all (\t -> t == Nothing) $ positions
                    then throw $ UnsupportedTimeStampFormat fmt
                    else
                        HM.fromList $ Data.List.zip keywords positions

-}
        parseFormat2 :: 
            String      -- Format string e.g., "DD/MM/YYYY HH:MI:SS"
            -> String   --  Timestamp string
            -> HashMap String String -- current map
            -> HashMap String String -- output map
        parseFormat2 [] _ currMap = currMap
        parseFormat2 fmt tstamp currMap =
            let
                -- search for the format keywords (in each iteration)
                keywords = ["YYYY","MM", "DD", "HH", "MI", "SS"]
                positions = Data.List.map (\subs -> instr subs fmt) keywords

                -- get from format string the first substring  of letter characters
                (fmtElement, restFormat) = Data.List.span (\c -> isAlpha c) fmt
                -- remove prefix non-Alpha characters from rest
                restFormatFinal = snd $ Data.List.span (\c -> not $ isAlpha c) restFormat
                -- get from tstamp string the first substring  of number characters
                (tmElement, restTstamp) =  Data.List.span (\c -> isDigit c) tstamp
                -- remove prefix non-Digit characters from rest
                restTstampFinal = snd $ Data.List.span (\c -> not $ isDigit c) restTstamp
                -- insert into map the pair 
                newMap = HM.insert fmtElement tmElement currMap
            in 
                -- if no keyword found then throw an exception
                if Data.List.all (\t -> t == Nothing) $ positions
                    then throw $ UnsupportedTimeStampFormat fmt
                    else parseFormat2 restFormatFinal restTstampFinal newMap


-- | Search for the first occurence of a substring within a 'String' and return the 1st character position,
-- or 'Nothing' if the substring is not found.
---- See :  
----          https://stackoverflow.com/questions/24349038/finding-the-position-of-some-substrings-in-a-string
----          https://docs.oracle.com/cd/B28359_01/server.111/b28286/functions073.htm#SQLRF00651
instr :: Eq a =>
        [a] -- ^ substring to search for
    ->  [a] -- ^ string to be searched
    ->  Maybe Int    -- ^ Position within input string of substr 1st character 
instr subs s = Data.List.findIndex (Data.List.isPrefixOf subs) $ Data.List.tails s

-- | Search for the first occurence of a substring within a 'Text' string and return the 1st character position,
-- or 'Nothing' if the substring is not found.
---- See :  
----          https://stackoverflow.com/questions/24349038/finding-the-position-of-some-substrings-in-a-string
----          https://docs.oracle.com/cd/B28359_01/server.111/b28286/functions073.htm#SQLRF00651
instrText :: 
        Text -- ^ substring to search for
    ->  Text -- ^ string to be searched
    ->  Maybe Int    -- ^ Position within input string of substr 1st character 
instrText subs s = instr (T.unpack subs) $ T.unpack s


-- | Search for the first occurence of a substring within a 'RText' string and return the 1st character position,
-- or 'Nothing' if the substring is not found, or if an non-text 'RDataType', is given as input.
---- See :  
----          https://stackoverflow.com/questions/24349038/finding-the-position-of-some-substrings-in-a-string
----          https://docs.oracle.com/cd/B28359_01/server.111/b28286/functions073.htm#SQLRF00651
instrRText :: 
        RDataType -- ^ substring to search for
    ->  RDataType -- ^ string to be searched
    ->  Maybe Int    -- ^ Position within input string of substr 1st character 
instrRText (RText subs) (RText s) = instrText subs s
instrRText _ _ = Nothing 


-- | Creates an RTimestamp data type from an input timestamp format string and a timestamp value represented as a `String`.
-- Valid format patterns are:
--
-- * For year: @YYYY@, e.g., @"0001"@, @"2018"@
-- * For month: @MM@, e.g., @"01"@, @"1"@, @"12"@
-- * For day: @DD@, e.g.,  @"01"@, @"1"@, @"31"@
-- * For hours: @HH@, @HH24@ e.g., @"00"@, @"23"@ I.e., hours must be specified in 24 format
-- * For minutes: @MI@, e.g., @"01"@, @"1"@, @"59"@
-- * For seconds: @SS@, e.g., @"01"@, @"1"@, @"59"@
--
-- Example of a typical format string is: @"DD\/MM\/YYYY HH:MI:SS@
-- 
-- If no valid format pattern is found then an 'UnsupportedTimeStampFormat' exception is thrown
--
createRTimestamp :: 
    String      -- ^ Format string e.g., "DD\/MM\/YYYY HH24:MI:SS"
    -> String   -- ^ Timestamp string
    -> RTimestamp
createRTimestamp fmt timeVal = toRTimestamp fmt timeVal
    {-case Prelude.map (Data.Char.toUpper) fmt of
        "DD/MM/YYYY HH24:MI:SS"     -> parseTime timeVal
        "\"DD/MM/YYYY HH24:MI:SS\"" -> parseTime timeVal
        "MM/DD/YYYY HH24:MI:SS"     -> parseTime timeVal
        "\"MM/DD/YYYY HH24:MI:SS\"" -> parseTime timeVal        
    where
        parseTime :: String -> RTimestamp
        -- DD/MM/YYYY HH24:MI:SS
        parseTime (d1:d2:'/':m1:m2:'/':y1:y2:y3:y4:' ':h1:h2:':':mi1:mi2:':':s1:s2:_) = RTimestampVal {    
                                                                                                year = (digitToInt y1) * 1000 + (digitToInt y2) * 100 + (digitToInt y3) * 10 + (digitToInt y4)
                                                                                                ,month = (digitToInt m1) * 10 + (digitToInt m2)
                                                                                                ,day = (digitToInt d1) * 10 + (digitToInt d2)
                                                                                                ,hours24 = (digitToInt h1) * 10 + (digitToInt h2)
                                                                                                ,minutes = (digitToInt mi1) * 10 + (digitToInt mi2)
                                                                                                ,seconds = (digitToInt s1) * 10 + (digitToInt s2)   
                                                                                                }                                                                                                                                 
        parseTime (d1:'/':m1:m2:'/':y1:y2:y3:y4:' ':h1:h2:':':mi1:mi2:':':s1:s2:_) = RTimestampVal {    
                                                                                                year = (digitToInt y1) * 1000 + (digitToInt y2) * 100 + (digitToInt y3) * 10 + (digitToInt y4)
                                                                                                ,month = (digitToInt m1) * 10 + (digitToInt m2)
                                                                                                ,day = (digitToInt d1)
                                                                                                ,hours24 = (digitToInt h1) * 10 + (digitToInt h2)
                                                                                                ,minutes = (digitToInt mi1) * 10 + (digitToInt mi2)
                                                                                                ,seconds = (digitToInt s1) * 10 + (digitToInt s2)                                                                                                                                    
                                                                                               }
        parseTime (d1:'/':m1:'/':y1:y2:y3:y4:' ':h1:h2:':':mi1:mi2:':':s1:s2:_) = RTimestampVal {    
                                                                                                year = (digitToInt y1) * 1000 + (digitToInt y2) * 100 + (digitToInt y3) * 10 + (digitToInt y4)
                                                                                                ,month = (digitToInt m1) 
                                                                                                ,day = (digitToInt d1) 
                                                                                                ,hours24 = (digitToInt h1) * 10 + (digitToInt h2)
                                                                                                ,minutes = (digitToInt mi1) * 10 + (digitToInt mi2)
                                                                                                ,seconds = (digitToInt s1) * 10 + (digitToInt s2)
                                                                                               }
        parseTime (d1:d2:'/':m1:'/':y1:y2:y3:y4:' ':h1:h2:':':mi1:mi2:':':s1:s2:_) = RTimestampVal {    
                                                                                                year = (digitToInt y1) * 1000 + (digitToInt y2) * 100 + (digitToInt y3) * 10 + (digitToInt y4)
                                                                                                ,month = (digitToInt m1)
                                                                                                ,day = (digitToInt d1) * 10 + (digitToInt d2)
                                                                                                ,hours24 = (digitToInt h1) * 10 + (digitToInt h2)
                                                                                                ,minutes = (digitToInt mi1) * 10 + (digitToInt mi2)
                                                                                                ,seconds = (digitToInt s1) * 10 + (digitToInt s2)                                                                                                                                    
                                                                                                }

        -- -- MM/DD/YYYY HH24:MI:SS                                                                                                
        parseTime (m1:m2:'/':d1:d2:'/':y1:y2:y3:y4:' ':h1:h2:':':mi1:mi2:':':s1:s2:_) = RTimestampVal {    
                                                                                                year = (digitToInt y1) * 1000 + (digitToInt y2) * 100 + (digitToInt y3) * 10 + (digitToInt y4)
                                                                                                ,month = (digitToInt m1) * 10 + (digitToInt m2)
                                                                                                ,day = (digitToInt d1) * 10 + (digitToInt d2)
                                                                                                ,hours24 = (digitToInt h1) * 10 + (digitToInt h2)
                                                                                                ,minutes = (digitToInt mi1) * 10 + (digitToInt mi2)
                                                                                                ,seconds = (digitToInt s1) * 10 + (digitToInt s2)   
                                                                                                }                                                                                                                                 
        parseTime (m1:'/':d1:d2:'/':y1:y2:y3:y4:' ':h1:h2:':':mi1:mi2:':':s1:s2:_) = RTimestampVal {    
                                                                                                year = (digitToInt y1) * 1000 + (digitToInt y2) * 100 + (digitToInt y3) * 10 + (digitToInt y4)
                                                                                                ,day = (digitToInt d1) * 10 + (digitToInt d2)
                                                                                                ,month = (digitToInt m1)
                                                                                                ,hours24 = (digitToInt h1) * 10 + (digitToInt h2)
                                                                                                ,minutes = (digitToInt mi1) * 10 + (digitToInt mi2)
                                                                                                ,seconds = (digitToInt s1) * 10 + (digitToInt s2)                                                                                                                                    
                                                                                               }
        parseTime (m1:'/':d1:'/':y1:y2:y3:y4:' ':h1:h2:':':mi1:mi2:':':s1:s2:_) = RTimestampVal {    
                                                                                                year = (digitToInt y1) * 1000 + (digitToInt y2) * 100 + (digitToInt y3) * 10 + (digitToInt y4)
                                                                                                ,month = (digitToInt m1) 
                                                                                                ,day = (digitToInt d1) 
                                                                                                ,hours24 = (digitToInt h1) * 10 + (digitToInt h2)
                                                                                                ,minutes = (digitToInt mi1) * 10 + (digitToInt mi2)
                                                                                                ,seconds = (digitToInt s1) * 10 + (digitToInt s2)
                                                                                               }
        parseTime (m1:m2:'/':d1:'/':y1:y2:y3:y4:' ':h1:h2:':':mi1:mi2:':':s1:s2:_) = RTimestampVal {    
                                                                                                year = (digitToInt y1) * 1000 + (digitToInt y2) * 100 + (digitToInt y3) * 10 + (digitToInt y4)
                                                                                                ,day = (digitToInt d1)
                                                                                                ,month = (digitToInt m1) * 10 + (digitToInt m2)
                                                                                                ,hours24 = (digitToInt h1) * 10 + (digitToInt h2)
                                                                                                ,minutes = (digitToInt mi1) * 10 + (digitToInt mi2)
                                                                                                ,seconds = (digitToInt s1) * 10 + (digitToInt s2)                                                                                                                                    
                                                                                                }

        parseTime _ = RTimestampVal {year = 2999, month = 12, day = 31, hours24 = 11, minutes = 59, seconds = 59}
-}

-- Convert from an RDate or RTimestamp to a UTCTime
{-
toUTCTime :: RDataType -> Maybe UTCTime
toUTCTime rdt = 
    case rdt of 
        RDate { rdate = dt, dtformat = fmt } ->
        RTime { rtime = RTimestamp {year = y, month = m, day = d, hours24 = hh, minutes = m } }

fromUTCTime :: UTCTime -> RDataType
-}

-- | Return the Text out of an RDataType
-- If a non-text RDataType is given then Nothing is returned.
toText :: RDataType -> Maybe T.Text
toText (RText t) = Just t
toText _ = Nothing

-- | Return an 'RDataType' from 'Text'
fromText :: T.Text -> RDataType
fromText t = RText t

-- | Returns 'True' only if this is an 'RText'
isText :: RDataType -> Bool
isText (RText t) = True
isText _ = False

-- | Standard timestamp format. For example: \"DD/MM/YYYY HH24:MI:SS\"
stdTimestampFormat = "DD/MM/YYYY HH24:MI:SS" :: String

-- | rTimeStampToText: converts an RTimestamp value to RText
-- Valid input formats are:
--
-- * 1. @ "DD\/MM\/YYYY HH24:MI:SS" @
-- * 2. @ \"YYYYMMDD-HH24.MI.SS\" @
-- * 3. @ \"YYYYMMDD\" @
-- * 4. @ \"YYYYMM\" @
-- * 5. @ \"YYYY\" @
--
rTimestampToRText :: 
    String  -- ^ Output format e.g., "DD\/MM\/YYYY HH24:MI:SS"
    -> RTimestamp -- ^ Input RTimestamp 
    -> RDataType  -- ^ Output RText
rTimestampToRText "DD/MM/YYYY HH24:MI:SS" ts =  let -- timeString = show (day ts) ++ "/" ++ show (month ts) ++ "/" ++ show (year ts) ++ " " ++ show (hours24 ts) ++ ":" ++ show (minutes ts) ++ ":" ++ show (seconds ts)
                                                    timeString = expand (day ts) ++ "/" ++ expand (month ts) ++ "/" ++ expand (year ts) ++ " " ++ expand (hours24 ts) ++ ":" ++ expand (minutes ts) ++ ":" ++ expand (seconds ts)
                                                    expand i = if i < 10 then "0"++ (show i) else show i
                                                in RText $ T.pack timeString
rTimestampToRText "YYYYMMDD-HH24.MI.SS" ts =    let -- timeString = show (year ts) ++ show (month ts) ++ show (day ts) ++ "-" ++ show (hours24 ts) ++ "." ++ show (minutes ts) ++ "." ++ show (seconds ts)
                                                    timeString = expand (year ts) ++ expand (month ts) ++ expand (day ts) ++ "-" ++ expand (hours24 ts) ++ "." ++ expand (minutes ts) ++ "." ++ expand (seconds ts)
                                                    expand i = if i < 10 then "0"++ (show i) else show i
                                                    {-
                                                    !dummy1 = trace ("expand (year ts) : " ++ expand (year ts)) True
                                                    !dummy2 = trace ("expand (month ts) : " ++ expand (month ts)) True
                                                    !dummy3 =  trace ("expand (day ts) : " ++ expand (day ts)) True
                                                    !dummy4 = trace ("expand (hours24 ts) : " ++ expand (hours24 ts)) True
                                                    !dummy5 = trace ("expand (minutes ts) : " ++ expand (minutes ts)) True
                                                    !dummy6 = trace ("expand (seconds ts) : " ++ expand (seconds ts)) True-}
                                                in RText $ T.pack timeString
rTimestampToRText "YYYYMMDD" ts =               let 
                                                    timeString = expand (year ts) ++ expand (month ts) ++ expand (day ts) 
                                                    expand i = if i < 10 then "0"++ (show i) else show i
                                                in RText $ T.pack timeString
rTimestampToRText "YYYYMM" ts =                 let 
                                                    timeString = expand (year ts) ++ expand (month ts) 
                                                    expand i = if i < 10 then "0"++ (show i) else show i
                                                in RText $ T.pack timeString
rTimestampToRText "YYYY" ts =                   let 
                                                    timeString = show $ year ts -- expand (year ts)
                                                    -- expand i = if i < 10 then "0"++ (show i) else show i
                                                in RText $ T.pack timeString

rTimestampToRText _ ts =                        let -- timeString = show (day ts) ++ "/" ++ show (month ts) ++ "/" ++ show (year ts) ++ " " ++ show (hours24 ts) ++ ":" ++ show (minutes ts) ++ ":" ++ show (seconds ts)
                                                    timeString = expand (day ts) ++ "/" ++ expand (month ts) ++ "/" ++ expand (year ts) ++ " " ++ expand (hours24 ts) ++ ":" ++ expand (minutes ts) ++ ":" ++ expand (seconds ts)
                                                    expand i = if i < 10 then "0"++ (show i) else show i
                                                in RText $ T.pack timeString


-- | Metadata for an RTable
data RTableMData =  RTableMData {
                        rtname :: RTableName        -- ^  Name of the 'RTable'
                        ,rtuplemdata :: RTupleMData  -- ^ Tuple-level metadata                    
                        -- other metadata
                        ,pkColumns :: [ColumnName] -- ^ Primary Key
                        ,uniqueKeys :: [[ColumnName]] -- ^ List of unique keys i.e., each sublist is a unique key column combination
                    } deriving (Show, Eq)


-- | createRTableMData : creates RTableMData from input given in the form of a list
--   We assume that the column order of the input list defines the fixed column order of the RTuple.
createRTableMData ::
        (RTableName, [(ColumnName, ColumnDType)])
        -> [ColumnName]     -- ^ Primary Key. [] if no PK exists
        -> [[ColumnName]]   -- ^ list of unique keys. [] if no unique keys exists
        -> RTableMData
createRTableMData (n, cdts) pk uks = 
        RTableMData { rtname = n, rtuplemdata = createRTupleMdata cdts, pkColumns = pk, uniqueKeys = uks }


-- | createRTupleMdata : Creates an RTupleMData instance based on a list of (Column name, Column Data type) pairs.
-- The order in the input list defines the fixed column order of the RTuple
createRTupleMdata ::  [(ColumnName, ColumnDType)] -> RTupleMData
-- createRTupleMdata clist = Prelude.map (\(n,t) -> (n, ColumnInfo{name = n, colorder = fromJust (elemIndex (n,t) clist),  dtype = t })) clist 
--HM.fromList $ Prelude.map (\(n,t) -> (n, ColumnInfo{name = n, colorder = fromJust (elemIndex (n,t) clist),  dtype = t })) clist
createRTupleMdata clist = 
    let colNamecolInfo = Prelude.map (\(n,t) -> (n, ColumnInfo{    name = n 
                                                                    --,colorder = fromJust (elemIndex (n,t) clist)
                                                                    ,dtype = t 
                                                                })) clist
        colOrdercolName = Prelude.map (\(n,t) -> (fromJust (elemIndex (n,t) clist), n)) clist
    in (HM.fromList colOrdercolName, HM.fromList colNamecolInfo)




-- Old - Obsolete:
-- Basic Metadata of an RTuple
-- Initially design with a HashMap, but HashMaps dont guarantee a specific ordering when turned into a list.
-- We implement the fixed column order logic for an RTuple, only at metadata level and not at the RTuple implementation, which is a HashMap (see @ RTuple)
-- So the fixed order of this list equals the fixed column order of the RTuple.
--type RTupleMData =  [(ColumnName, ColumnInfo)] -- HM.HashMap ColumnName ColumnInfo         




-- | Basic Metadata of an 'RTuple'.
--   The 'RTuple' metadata are accessed through a 'HashMap' 'ColumnName' 'ColumnInfo'  structure. I.e., for each column of the 'RTuple',
--   we access the 'ColumnInfo' structure to get Column-level metadata. This access is achieved by 'ColumnName'.
--   However, in order to provide the "impression" of a fixed column order per tuple (see 'RTuple' definition), we provide another 'HashMap',
--   the 'HashMap' 'ColumnOrder' 'ColumnName'. So in the follwoing example, if we want to access the 'RTupleMData' tupmdata ColumnInfo by column order, 
--   (assuming that we have N columns) we have to do the following:
--    
-- @
--      (snd tupmdata)!((fst tupmdata)!0)
--      (snd tupmdata)!((fst tupmdata)!1)
--      ...
--      (snd tupmdata)!((fst tupmdata)!(N-1))
-- @
--
--  In the same manner in order to access the column of an 'RTuple' (e.g., tup) by column order, we do the following:
--
-- @
--      tup!((fst tupmdata)!0)
--      tup!((fst tupmdata)!1)
--      ...
--      tup!((fst tupmdata)!(N-1))
-- @
-- 
type RTupleMData =  (HM.HashMap ColumnOrder ColumnName, HM.HashMap ColumnName ColumnInfo) 

type ColumnOrder = Int 

-- | toListColumnName: returns a list of RTuple column names, in the fixed column order of the RTuple.
toListColumnName :: 
    RTupleMData
    -> [ColumnName]
toListColumnName rtupmd = 
    let mapColOrdColName = fst rtupmd
        listColOrdColName = HM.toList mapColOrdColName  -- generate a list of [ColumnOrdr, ColumnName] in random order
        -- order list based on ColumnOrder
        ordlistColOrdColName = sortOn (\(o,c) -> o) listColOrdColName   -- Data.List.sortOn :: Ord b => (a -> b) -> [a] -> [a]. Sort a list by comparing the results of a key function applied to each element. 
    in Prelude.map (snd) ordlistColOrdColName

-- | toListColumnInfo: returns a list of RTuple columnInfo, in the fixed column order of the RTuple
toListColumnInfo :: 
    RTupleMData
    -> [ColumnInfo]
toListColumnInfo rtupmd = 
    let mapColNameColInfo = snd rtupmd
    in Prelude.map (\cname -> mapColNameColInfo HM.! cname) (toListColumnName rtupmd)


-- | toListRDataType: returns a list of RDataType values of an RTuple, in the fixed column order of the RTuple
toListRDataType :: 
    RTupleMData
    -> RTuple
    -> [RDataType]
toListRDataType rtupmd rtup = Prelude.map (\cname -> rtup <!> cname) (toListColumnName rtupmd)


-- | Basic metadata for a column of an RTuple
data ColumnInfo =   ColumnInfo { 
                        name :: ColumnName  
                      --  ,colorder :: Int  -- ^ ordering of column within the RTuple (each new column added takes colorder+1)
                                          --   Since an RTuple is implemented as a HashMap ColumnName RDataType, ordering of columns has no meaning.
                                          --   However, with this columns we can "pretend" that there is a fixed column order in each RTuple.
                        ,dtype :: ColumnDType
                    } deriving (Show,Eq)

-- | Define equality for two 'ColumnInfo' structures
-- For two column two have \"equal structure\" they must have the same name
-- and the same type. If one of the two (or both) have an 'UknownType', then they are still considered of equal structure.
{-
instance Eq (ColumnInfo) where
    (==) ci1 ci2 = 
        if (name ci1) == (name ci2)
            then
                if ((dtype ci1) == (dtype ci2))
                    ||
                    (dtype ci1 == UknownType)
                    ||
                    (dtype ci2 == UknownType)
                    then True
                    else False
            else
                False
-}

-- | Creates a list of the form [(ColumnInfo, RDataType)]  from a list of ColumnInfo and an RTuple. The returned list respects the order of the [ColumnInfo].
-- It guarantees that RDataTypes will be in the same column order as [ColumnInfo], i.e., the correct RDataType for the correct column
listOfColInfoRDataType :: [ColumnInfo] -> RTuple -> [(ColumnInfo, RDataType)]  
listOfColInfoRDataType (ci:[]) rtup = [(ci, rtup HM.!(name ci))]  -- rt HM.!(name ci) -> this returns the RDataType by column name
listOfColInfoRDataType (ci:colInfos) rtup = (ci, rtup HM.!(name ci)):listOfColInfoRDataType colInfos rtup


-- | createRDataType:  Get a value of type a and return the corresponding RDataType.
-- The input value data type must be an instance of the Typepable typeclass from Data.Typeable
createRDataType ::
    TB.Typeable a
    => a            -- ^ input value
    -> RDataType    -- ^ output RDataType
createRDataType val = 
        case show (TB.typeOf val) of
                        --"Int"     -> RInt $ D.fromDyn (D.toDyn val) 0
                        "Int"     -> case (D.fromDynamic (D.toDyn val)) of   -- toDyn :: Typeable a => a -> Dynamic
                                                Just v -> RInt v             -- fromDynamic :: Typeable a    => Dynamic  -> Maybe a  
                                                Nothing -> Null
                        --"Char"    -> RChar $ D.fromDyn (D.toDyn val) 'a'
                     {--   "Char"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> RChar v
                                                Nothing -> Null                        --}
                        --"Text"    -> RText $ D.fromDyn (D.toDyn val) ""
                        "Text"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> RText v
                                                Nothing -> Null                                                
                        --"[Char]"  -> RString $ D.fromDyn (D.toDyn val) ""
                      {--  "[Char]"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> RString v
                                                Nothing -> Null                        --}
                        --"Double"  -> RDouble $ D.fromDyn (D.toDyn val) 0.0
                        "Double"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> RDouble v
                                                Nothing -> Null                                                
                        --"Float"   -> RFloat $ D.fromDyn (D.toDyn val) 0.0
                {--        "Float"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> RFloat v
                                                Nothing -> Null                       --}
                        _         -> Null


{--createRDataType ::
    TB.Typeable a
    => a            -- ^ input value
    -> RDataType    -- ^ output RDataType
createRDataType val = 
        case show (TB.typeOf val) of
                        --"Int"     -> RInt $ D.fromDyn (D.toDyn val) 0
                        "Int"     -> case (D.fromDynamic (D.toDyn val)) of   -- toDyn :: Typeable a => a -> Dynamic
                                                Just v ->  v             -- fromDynamic :: Typeable a    => Dynamic  -> Maybe a  
                                                Nothing -> Null
                        --"Char"    -> RChar $ D.fromDyn (D.toDyn val) 'a'
                        "Char"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> v
                                                Nothing -> Null                        
                        --"Text"    -> RText $ D.fromDyn (D.toDyn val) ""
                        "Text"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> v
                                                Nothing -> Null                                                
                        --"[Char]"  -> RString $ D.fromDyn (D.toDyn val) ""
                        "[Char]"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> v
                                                Nothing -> Null                                                
                        --"Double"  -> RDouble $ D.fromDyn (D.toDyn val) 0.0
                        "Double"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> v
                                                Nothing -> Null                                                
                        --"Float"   -> RFloat $ D.fromDyn (D.toDyn val) 0.0
                        "Float"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> v
                                                Nothing -> Null                                                
                        _         -> Null
--}


{--
-- | Definition of a relational tuple
data Rtuple 
--    = TupToBeDefined deriving (Show)
    = Rtuple {
                fieldMap :: Map.Map RelationField Int -- ^ provides a key,value mapping  between the field and the Index (i.e., the offset) in the bytestring
                ,fieldValues :: BS.ByteString          -- ^ tuple values are stored in a bytestring
            }
    deriving Show
--}


{--

-- | Definition of a relation's field
data RelationField
    = RelationField {
                        fldname :: String
                        ,dataType :: DataType
                    }
    deriving Show                    

-- | Definition of a data type
data DataType 
    = Rinteger  -- ^ an integer data type
    | Rstring   -- ^ a string data type
    | Rdate     -- ^ a date data type
    deriving Show



-- | Definition of a predicate
type Predicate a
    =  a -> Bool    -- ^ a predicate is a polymorphic type, which is a function that evaluates an expression over an 'a' 
                    -- (e.g., a can be an Rtuple, thus Predicate Rtuple) and returns either true or false.


-- |    The selection operator.
--      It filters the tuples of a relation based on a predicate
--      and returns a new relation with the tuple that satisfy the predicate
selection :: 
        Relation  -- ^ input relation
    ->  Predicate Rtuple  -- ^ input predicate      
    ->  Relation  -- ^ output relation
selection r p = undefined  

--}

-- | A Predicate. It defines an arbitrary condition over the columns of an 'RTuple'. It is used primarily in the filter 'RFilter' operation and used in the filter function 'f'.
type RPredicate = RTuple -> Bool

-- | Definition of Relational Algebra operations.
-- These are the valid operations between RTables
data ROperation = 
      ROperationEmpty
    | RUnion   -- ^ Union 
    | RInter     -- ^ Intersection
    | RDiff    -- ^ Difference
    | RPrj    { colPrjList :: [ColumnName] }   -- ^ Projection
    | RFilter { fpred :: RPredicate }   -- ^ Filter operation (an 'RPredicate' can be any function of the signature
                                        -- @
                                        -- RTuple -> Bool
                                        -- @
                                        -- so it is much more powerful than a typical SQL filter expression, which is a boolean expression of comparison operators)
    | RInJoin { jpred :: RJoinPredicate }     -- ^ Inner Join (any type of join predicate allowed. Any function with a signature of the form:
                                              -- @
                                              -- RTuple -> RTuple -> Bool
                                              -- @
                                              -- is a valid join predicate. I.e., a function which returns 'True' when two 'RTuples' must be paired)
    | RLeftJoin { jpred :: RJoinPredicate }   -- ^ Left Outer Join    
    | RRightJoin { jpred :: RJoinPredicate }  -- ^ Right Outer Join
    | RSemiJoin { jpred :: RJoinPredicate }     -- ^ Semi-Join
    | RAntiJoin { jpred :: RJoinPredicate }     -- ^ Anti-Join    
    | RAggregate { aggList :: [RAggOperation] -- ^ list of aggregates 
                 } -- ^ Performs aggregation operations on specific columns and returns a singleton RTable
    | RGroupBy  { 
                    gpred :: RGroupPredicate        -- ^ the grouping predicate
                    ,aggList :: [RAggOperation]     -- ^ the list of aggregates
                    ,colGrByList :: [ColumnName]    -- ^ the Group By list of columns
                }   -- ^ A Group By operation
                    -- The SQL equivalent is: 
                    -- @
                    -- SELECT colGrByList, aggList FROM... GROUP BY colGrByList
                    -- @
                    -- Note that compared to SQL, we can have a more generic grouping predicate (i.e.,
                    -- when two 'RTuple's should belong in the same group) than just the equality of 
                    -- values on the common columns between two 'RTuple's.
                    -- Also note, that in the case of an aggregation without grouping (equivalent to
                    -- a single-group group by), then the grouping predicate should be: 
                    -- @
                    -- \_ _ -> True
                    -- @
    | RCombinedOp { rcombOp :: UnaryRTableOperation  }   -- ^ A combination of unary 'ROperation's e.g., 
                                                         -- @
                                                         --  (p plist).(f pred)  (i.e., RPrj . RFilter)
                                                         -- @
                                                         -- , in the form of an 
                                                         -- @
                                                         -- RTable -> RTable function.
                                                         -- @
                                                         --  In this sense we can also include a binary operation (e.g. join), if we partially apply the join to one 'RTable', e.g.,
                                                         --   
                                                         -- @ 
                                                         -- (ij jpred rtab) . (p plist) . (f pred)
                                                         -- @
    | RBinOp { rbinOp :: BinaryRTableOperation } -- ^ A generic binary 'ROperation'.
    | ROrderBy { colOrdList :: [(ColumnName, OrderingSpec)] }   -- ^ Order the 'RTuple's of the 'RTable' acocrding to the specified list of Columns.
                                                                -- First column in the input list has the highest priority in the sorting order.

-- | A sum type to help the specification of a column ordering (Ascending, or Descending)
data OrderingSpec = Asc | Desc deriving (Show, Eq)

-- | A generic unary operation on a RTable
type UnaryRTableOperation = RTable -> RTable

-- | A generic binary operation on RTable
type BinaryRTableOperation = RTable -> RTable -> RTable


-- | The Join Predicate. It defines when two 'RTuple's should be paired.
type RJoinPredicate = RTuple -> RTuple -> Bool

-- | The Upsert Predicate. It defines when two 'RTuple's should be paired in a merge operation.
-- The matching predicate must be applied on a specific set of matching columns. The source 'RTable'
-- in the Upsert operation must return a unique set of 'RTuple's, if grouped by this set of matching columns.
-- Otherwise an exception ('UniquenessViolationInUpsert') is thrown.
data RUpsertPredicate = RUpsertPredicate {
                            matchCols :: [ColumnName]
                            ,matchPred :: RTuple -> RTuple -> Bool
                        }

-- type RUpsertPredicate = RTuple -> RTuple -> Bool
                        

-- | The Group By Predicate
-- It defines the condition for two 'RTuple's to be included in the same group.
type RGroupPredicate = RTuple -> RTuple -> Bool


-- | This data type represents all possible aggregate operations over an RTable.
-- Examples are : Sum, Count, Average, Min, Max but it can be any other "aggregation".
-- The essential property of an aggregate operation is that it acts on an RTable (or on 
-- a group of RTuples - in the case of the RGroupBy operation) and produces a single RTuple.
-- 
-- An aggregate operation is applied on a specific column (source column) and the aggregated result
-- will be stored in the target column. It is important to understand that the produced aggregated RTuple 
-- is different from the input RTuples. It is a totally new RTuple, that will consist of the 
-- aggregated column(s) (and the grouping columns in the case of an RGroupBy).

-- Also, note that following SQL semantics, an aggregate operation ignores  Null values.
-- So for example, a SUM(column) will just ignore them and also will COUNT(column), i.e., it 
-- will not sum or count the Nulls. If all columns are Null, then a Null will be returned.
--
-- With this data type one can define his/her own aggregate operations and then execute them
-- with the 'runAggregation' (or 'rAgg') functions.
--
data RAggOperation = RAggOperation {
                         sourceCol :: ColumnName        -- ^ Source column
                        ,targetCol :: ColumnName        -- ^ Target column
                        ,aggFunc   :: RTable -> RTuple  -- ^ here we define the aggegate function to be applied on an RTable
                    }

-- | Aggregation Function type.
-- An aggregation function receives as input a source column (i.e., a 'ColumnName') of a source 'RTable' and returns
-- an aggregated value, which is the result of the aggregation on the values of the source column.
type AggFunction = ColumnName -> RTable -> RDataType 

-- | Returns an 'RAggOperation' with a custom aggregation function provided as input
raggGenericAgg ::
        AggFunction -- ^ custom aggregation function 
    ->  ColumnName -- ^ source column
    ->  ColumnName -- ^ target column
    ->  RAggOperation
raggGenericAgg aggf src trg = RAggOperation {
                 sourceCol = src
                ,targetCol = trg                                 
                ,aggFunc = \rtab -> createRTuple [(trg, aggf src rtab)]  
        }

type Delimiter = String

-- | The StrAgg aggregate operation
-- This is known as \"string_agg\"" in Postgresql and \"listagg\" in Oracle.
-- It aggregates the values of a text 'RDataType' column with a specified delimiter
raggStrAgg ::
        ColumnName -- ^ source column
    ->  ColumnName -- ^ target column
    ->  Delimiter  -- ^ delimiter string 
    ->  RAggOperation
raggStrAgg src trg delimiter =  RAggOperation {
                 sourceCol = src
                ,targetCol = trg                       
                ,aggFunc = \rtab -> createRTuple [(trg, strAggFold delimiter src rtab)]  
        }

-- | A helper function that implements the basic fold for the raggStrAgg aggregation        
strAggFold :: Delimiter -> AggFunction
strAggFold dlmt col rtab = 
    rdatatypeFoldr' ( \rtup accValue -> 
        if isNotNull (rtup <!> col)  && (isNotNull accValue)
            then
                rtup <!> col `rdtappend` (RText delimiter) `rdtappend` accValue
            else
                --if (getRTupColValue src) rtup == Null && accValue /= Null 
                if isNull (rtup <!> col) && (isNotNull accValue)
                    then
                        accValue  -- ignore Null value
                    else
                        --if (getRTupColValue src) rtup /= Null && accValue == Null 
                        if isNotNull (rtup <!> col) && (isNull accValue)
                            then
                                case isText (rtup <!> col) of
                                    True ->  (rtup <!> col) 
                                    False -> Null
                            else
                                Null -- agg of Nulls is Null
                    )
                    Null
                    rtab
    where
        delimiter = pack dlmt -- convert String to Text


-- | The Sum aggregate operation
raggSum :: 
        ColumnName -- ^ source column
    ->  ColumnName -- ^ target column
    ->  RAggOperation
raggSum src trg = RAggOperation {
                 sourceCol = src
                ,targetCol = trg                                 
                ,aggFunc = \rtab -> createRTuple [(trg, sumFold src rtab)]  
        }

-- | A helper function in raggSum that implements the basic fold for sum aggregation        
sumFold :: AggFunction -- ColumnName -> RTable -> RDataType
sumFold src rtab =         
    V.foldr' ( \rtup accValue ->                                                                     
                    --if (getRTupColValue src) rtup /= Null && accValue /= Null
                    if (isNotNull $ (getRTupColValue src) rtup)  && (isNotNull accValue)
                        then
                            (getRTupColValue src) rtup + accValue
                        else
                            --if (getRTupColValue src) rtup == Null && accValue /= Null 
                            if (isNull $ (getRTupColValue src) rtup) && (isNotNull accValue)
                                then
                                    accValue  -- ignore Null value
                                else
                                    --if (getRTupColValue src) rtup /= Null && accValue == Null 
                                    if (isNotNull $ (getRTupColValue src) rtup) && (isNull accValue)
                                        then
                                            (getRTupColValue src) rtup + RInt 0  -- ignore so far Null agg result
                                                                                 -- add RInt 0, so in the case of a non-numeric rtup value, the result will be a Null
                                                                                 -- while if rtup value is numeric the addition of RInt 0 does not cause a problem
                                        else
                                            Null -- agg of Nulls is Null
             ) (Null) rtab


-- | The Count aggregate operation
-- Count aggregation (no distinct)
raggCount :: 
        ColumnName -- ^ source column
    ->  ColumnName -- ^ target column
    ->  RAggOperation
raggCount src trg = RAggOperation {
                 sourceCol = src
                ,targetCol = trg
                ,aggFunc = \rtab -> createRTuple [(trg, countFold src rtab)]  
        }


-- | A helper function in raggCount that implements the basic fold for Count aggregation        
countFold :: AggFunction -- ColumnName -> RTable -> RDataType
countFold src rtab =         
    V.foldr' ( \rtup accValue ->                                                                     
                            --if (getRTupColValue src) rtup /= Null && accValue /= Null
                            if (isNotNull $ (getRTupColValue src) rtup)  && (isNotNull accValue)  
                                then
                                    RInt 1 + accValue
                                else
                                    --if (getRTupColValue src) rtup == Null && accValue /= Null 
                                    if (isNull $ (getRTupColValue src) rtup) && (isNotNull accValue)  
                                        then
                                            accValue  -- ignore Null value
                                        else
                                            --if (getRTupColValue src) rtup /= Null && accValue == Null 
                                            if (isNotNull $ (getRTupColValue src) rtup) && (isNull accValue)
                                                then
                                                    RInt 1  -- ignore so far Null agg result
                                                else
                                                    Null -- agg of Nulls is Null
             ) (Null) rtab


-- | The CountStar aggregate operation 
--  Returns the number of 'RTuple's in the 'RTable' (i.e., @count(*)@ in SQL) 
raggCountStar ::         
        ColumnName -- ^ target column to save the result aggregated value
    ->  RAggOperation
raggCountStar trg  = RAggOperation {
                sourceCol = ""  -- no source column required
                ,targetCol = trg
                ,aggFunc = \rtab -> createRTuple [(trg, countStarFold rtab)]  
        }
-- | A helper function in raggCountStar that implements the basic fold for CountStar aggregation        
countStarFold :: RTable -> RDataType
countStarFold rtab = RInt $ toInteger $ Data.List.length $ rtableToList rtab


-- | The CountDist aggregate operation 
-- Count distinct aggregation (i.e., @count(distinct col)@ in SQL). Returns the distinct number of values for this column.
raggCountDist :: 
        ColumnName -- ^ source column
    ->  ColumnName -- ^ target column
    ->  RAggOperation
raggCountDist src trg = RAggOperation {
                 sourceCol = src
                ,targetCol = trg
                ,aggFunc = \rtab -> createRTuple [(trg, countDistFold src rtab)]  
        }

-- | A helper function in raggCountDist that implements the basic fold for CountDist aggregation        
countDistFold :: AggFunction -- ColumnName -> ColumnName -> RTable -> RDataType
countDistFold src rtab = 
    let
        -- change the input rtable to fold, to be the distinct list of values
        -- implement it with a group by on the src column
        rtabDist = runGroupBy   (\t1 t2 -> t1 <!> src == t2 <!> src)
                                [raggCount src "dummy"]  -- this will be omitted anyway
                                [src]
                                rtab
    in V.foldr' ( \rtup accValue ->                                                                     
                            --if (getRTupColValue src) rtup /= Null && accValue /= Null
                            if (isNotNull $ (getRTupColValue src) rtup)  && (isNotNull accValue)  
                                then
                                    RInt 1 + accValue
                                else
                                    --if (getRTupColValue src) rtup == Null && accValue /= Null 
                                    if (isNull $ (getRTupColValue src) rtup) && (isNotNull accValue)  
                                        then
                                            accValue  -- ignore Null value
                                        else
                                            --if (getRTupColValue src) rtup /= Null && accValue == Null 
                                            if (isNotNull $ (getRTupColValue src) rtup) && (isNull accValue)
                                                then
                                                    RInt 1  -- ignore so far Null agg result
                                                else
                                                    Null -- agg of Nulls is Null
             ) (Null) rtabDist


-- | The Average aggregate operation
raggAvg :: 
        ColumnName -- ^ source column
    ->  ColumnName -- ^ target column
    ->  RAggOperation
raggAvg src trg = RAggOperation {
                 sourceCol = src
                ,targetCol = trg
                ,aggFunc = \rtab -> createRTuple [(trg, let 
                                                             sum = sumFold src rtab
                                                             cnt =  countFold src rtab
                                                        in case (sum,cnt) of
                                                                (RInt s, RInt c) -> RDouble (fromIntegral s / fromIntegral c)
                                                                (RDouble s, RInt c) -> RDouble (s / fromIntegral c)
                                                                (_, _)           -> Null
                                                 )]  
        }        

-- | The Max aggregate operation
raggMax :: 
        ColumnName -- ^ source column
    ->  ColumnName -- ^ target column
    ->  RAggOperation
raggMax src trg = RAggOperation {
                 sourceCol = src
                ,targetCol = trg
                ,aggFunc = \rtab -> createRTuple [(trg, maxFold src rtab)]  
        }        


-- | A helper function in raggMax that implements the basic fold for Max aggregation        
maxFold :: AggFunction -- ColumnName -> ColumnName -> RTable -> RDataType
maxFold src rtab =         
    V.foldr' ( \rtup accValue ->         
                                --if (getRTupColValue src) rtup /= Null && accValue /= Null
                                if (isNotNull $ (getRTupColValue src) rtup)  && (isNotNull accValue)  
                                    then
                                        max ((getRTupColValue src) rtup) accValue
                                    else
                                        --if (getRTupColValue src) rtup == Null && accValue /= Null 
                                        if (isNull $ (getRTupColValue src) rtup) && (isNotNull accValue)  
                                            then
                                                accValue  -- ignore Null value
                                            else
                                                --if (getRTupColValue src) rtup /= Null && accValue == Null 
                                                if (isNotNull $ (getRTupColValue src) rtup) && (isNull accValue)
                                                    then
                                                        (getRTupColValue src) rtup  -- ignore so far Null agg result
                                                    else
                                                        Null -- agg of Nulls is Null
             ) Null rtab


-- | The Min aggregate operation
raggMin :: 
        ColumnName -- ^ source column
    ->  ColumnName -- ^ target column
    ->  RAggOperation
raggMin src trg = RAggOperation {
                 sourceCol = src
                ,targetCol = trg
                ,aggFunc = \rtab -> createRTuple [(trg, minFold src rtab)]  
        }        

-- | A helper function in raggMin that implements the basic fold for Min aggregation        
minFold :: AggFunction -- ColumnName -> ColumnName -> RTable -> RDataType
minFold src rtab =         
    V.foldr' ( \rtup accValue ->         
                                --if (getRTupColValue src) rtup /= Null && accValue /= Null
                                if (isNotNull $ (getRTupColValue src) rtup)  && (isNotNull accValue)
                                    then
                                        min ((getRTupColValue src) rtup) accValue
                                    else
                                        --if (getRTupColValue src) rtup == Null && accValue /= Null 
                                        if (isNull $ (getRTupColValue src) rtup) && (isNotNull accValue)  
                                            then
                                                accValue  -- ignore Null value
                                            else
                                                --if (getRTupColValue src) rtup /= Null && accValue == Null 
                                                if (isNotNull $ (getRTupColValue src) rtup) && (isNull accValue)
                                                    then
                                                        (getRTupColValue src) rtup  -- ignore so far Null agg result
                                                    else
                                                        Null -- agg of Nulls is Null
             ) Null rtab

{--
data RAggOperation = 
          RSum ColumnName  -- ^  sums values in the specific column
        | RCount ColumnName -- ^ count of values in the specific column
        | RCountDist ColumnName -- ^ distinct count of values in the specific column
        | RAvg ColumnName  -- ^ average of values in the specific column
        | RMin ColumnName -- ^ minimum of values in the specific column
        | RMax ColumnName -- ^ maximum of values in the specific column
--}

-- | ropU operator executes a unary ROperation. A short name for the 'runUnaryROperation' function
ropU = runUnaryROperation

-- | Execute a Unary ROperation
runUnaryROperation :: 
    ROperation -- ^ input ROperation
    -> RTable  -- ^ input RTable
    -> RTable  -- ^ output RTable
runUnaryROperation rop irtab = 
    case rop of
        RFilter { fpred = rpredicate }                                                  ->  runRfilter rpredicate irtab
        RPrj { colPrjList = colNames }                                                  ->  runProjection colNames irtab
        RAggregate { aggList = aggFunctions }                                           ->  runAggregation aggFunctions irtab
        RGroupBy  { gpred = groupingpred, aggList = aggFunctions, colGrByList = cols }  ->  runGroupBy groupingpred aggFunctions cols irtab
        RCombinedOp { rcombOp = comb }                                                  ->  runCombinedROp comb irtab 
        ROrderBy { colOrdList = colist }                                                ->  runOrderBy colist irtab

-- | ropUres operator executes a unary ROperation. A short name for the 'runUnaryROperationRes' function
ropUres = runUnaryROperationRes

-- | Execute a Unary ROperation and return an 'RTabResult'
runUnaryROperationRes :: 
    ROperation -- ^ input ROperation
    -> RTable  -- ^ input RTable
    -> RTabResult  -- ^ output: Result of operation
runUnaryROperationRes rop irtab = 
    let resultRtab = runUnaryROperation rop irtab
        returnedRtups = rtuplesRet $ V.length resultRtab
    in rtabResult (resultRtab, returnedRtups)

-- | ropB operator executes a binary ROperation. A short name for the 'runBinaryROperation' function
ropB = runBinaryROperation

-- | Execute a Binary ROperation
runBinaryROperation :: 
    ROperation -- ^ input ROperation
    -> RTable  -- ^ input RTable1
    -> RTable  -- ^ input RTable2    
    -> RTable  -- ^ output RTabl
runBinaryROperation rop irtab1 irtab2 = 
    case rop of
        RInJoin    { jpred = jpredicate } -> runInnerJoinO jpredicate irtab1 irtab2
        RLeftJoin  { jpred = jpredicate } -> runLeftJoin jpredicate irtab1 irtab2
        RRightJoin { jpred = jpredicate } -> runRightJoin jpredicate irtab1 irtab2
        RSemiJoin    { jpred = jpredicate } -> runSemiJoin jpredicate irtab1 irtab2
        RAntiJoin    { jpred = jpredicate } -> runAntiJoin jpredicate irtab1 irtab2        
        RUnion -> runUnion irtab1 irtab2
        RInter -> runIntersect irtab1 irtab2
        RDiff  -> runDiff irtab1 irtab2  
        RBinOp { rbinOp = bop } -> bop irtab1 irtab2      

-- | ropBres operator executes a binary ROperation. A short name for the 'runBinaryROperationRes' function
ropBres = runBinaryROperationRes

-- | Execute a Binary ROperation and return an 'RTabResult'
runBinaryROperationRes :: 
    ROperation -- ^ input ROperation
    -> RTable  -- ^ input RTable1
    -> RTable  -- ^ input RTable2    
    -> RTabResult  -- ^ output: Result of operation
runBinaryROperationRes rop irtab1 irtab2 = 
    let resultRtab = runBinaryROperation rop irtab1 irtab2
        returnedRtups = rtuplesRet $ V.length resultRtab
    in rtabResult (resultRtab, returnedRtups)


-- * #########  Construction ##########

-- | Test whether an RTable is empty
isRTabEmpty :: RTable -> Bool
isRTabEmpty = V.null

-- | Test whether an RTuple is empty
isRTupEmpty :: RTuple -> Bool
isRTupEmpty = HM.null

-- | emptyRTable: Create an empty RTable
emptyRTable :: RTable
emptyRTable = V.empty :: RTable

-- | Creates an empty RTuple (i.e., one with no column,value mappings)
emptyRTuple :: RTuple 
emptyRTuple = HM.empty


-- | Creates an RTable with a single RTuple
createSingletonRTable ::
       RTuple 
    -> RTable 
createSingletonRTable rt = V.singleton rt

-- | createRTuple: Create an Rtuple from a list of column names and values
createRTuple ::
      [(ColumnName, RDataType)]  -- ^ input list of (columnname,value) pairs
    -> RTuple 
createRTuple l = HM.fromList l



-- | Creates a Null 'RTuple' based on a list of input Column Names.
-- A 'Null' 'RTuple' is an 'RTuple' where all column names correspond to a 'Null' value ('Null' is a data constructor of 'RDataType')
createNullRTuple ::
       [ColumnName]
    -> RTuple
createNullRTuple cnames = HM.fromList $ zipped
    where zipped = Data.List.zip cnames (Data.List.take (Data.List.length cnames) (repeat Null))

-- | Returns 'True' if the input 'RTuple' is a Null RTuple, otherwise it returns 'False'
-- Note that a Null RTuple has all its values equal with 'Null' but it still has columns. This is different from an empty 'RTuple', which
-- is an 'RTuple' withi no columns and no values whatsoever. See 'isRTupEmpty'.
isNullRTuple ::
       RTuple 
    -> Bool    
isNullRTuple t = 
    let -- if t is really Null, then the following must return an empty RTuple (since a Null RTuple has all its values equal with Null)
        checkt = HM.filter (\v -> isNotNull  v) t  --v /= Null) t 
    in if isRTupEmpty checkt 
            then True
            else False


-- * ########## RTable "Functional" Operations ##############

-- | This is a fold operation on a 'RTable' that returns an 'RTable'.
-- It is similar with :
-- @
--  foldr' :: (a -> b -> b) -> b -> Vector a -> b 
-- @
-- of Vector, which is an O(n) Right fold with a strict accumulator
rtabFoldr' :: (RTuple -> RTable -> RTable) -> RTable -> RTable -> RTable
rtabFoldr' f accum rtab = V.foldr' f accum rtab

-- | This is a fold operation on a 'RTable' that returns an 'RDataType' value.
-- It is similar with :
-- @
--  foldr' :: (a -> b -> b) -> b -> Vector a -> b 
-- @
-- of Vector, which is an O(n) Right fold with a strict accumulator
rdatatypeFoldr' :: (RTuple -> RDataType -> RDataType) -> RDataType -> RTable -> RDataType
rdatatypeFoldr' f accum rtab = V.foldr' f accum rtab

-- | This is a fold operation on 'RTable' that returns an 'RTable'.
-- It is similar with :
-- @
--  foldl' :: (a -> b -> a) -> a -> Vector b -> a
-- @
-- of Vector, which is an O(n) Left fold with a strict accumulator
rtabFoldl' :: (RTable -> RTuple -> RTable) -> RTable -> RTable -> RTable
rtabFoldl' f accum rtab = V.foldl' f accum rtab


-- | This is a fold operation on 'RTable' that returns an 'RDataType' value
-- It is similar with :
-- @
--  foldl' :: (a -> b -> a) -> a -> Vector b -> a
-- @
-- of Vector, which is an O(n) Left fold with a strict accumulator
rdatatypeFoldl' :: (RDataType -> RTuple -> RDataType) -> RDataType -> RTable -> RDataType
rdatatypeFoldl' f accum rtab = V.foldl' f accum rtab


-- | Map function over an 'RTable'.
rtabMap :: (RTuple -> RTuple) -> RTable -> RTable
rtabMap f rtab = V.map f rtab 

-- Map function over an 'RTuple'.
rtupleMap :: (RDataType -> RDataType) -> RTuple -> RTuple
rtupleMap f t = HM.map f t 

rtupleMapWithKey :: (ColumnName -> RDataType -> RDataType) -> RTuple -> RTuple
rtupleMapWithKey f t = HM.mapWithKey f t

-- * ########## RTable Relational Operations ##############

-- | Number of RTuples returned by an RTable operation
type RTuplesRet = Sum Int

-- | Creates an RTuplesRet type
rtuplesRet :: Int -> RTuplesRet
rtuplesRet i = (M.Sum i) :: RTuplesRet

-- | Return the number embedded in the RTuplesRet data type
getRTuplesRet :: RTuplesRet -> Int 
getRTuplesRet = M.getSum


-- | RTabResult is the result of an RTable operation and is a Writer Monad, that includes the new RTable, 
-- as well as the number of RTuples returned by the operation.
type RTabResult = Writer RTuplesRet RTable

-- | Creates an RTabResult (i.e., a Writer Monad) from a result RTable and the number of RTuples that it returned
rtabResult :: 
       (RTable, RTuplesRet)  -- ^ input pair 
    -> RTabResult -- ^ output Writer Monad
rtabResult (rtab, rtupRet) = writer (rtab, rtupRet)

-- | Returns the info "stored" in the RTabResult Writer Monad
runRTabResult ::
       RTabResult
    -> (RTable, RTuplesRet)
runRTabResult rtr = runWriter rtr

-- | Returns the "log message" in the RTabResult Writer Monad, which is the number of returned RTuples
execRTabResult ::
       RTabResult
    -> RTuplesRet
execRTabResult rtr = execWriter rtr  


-- | removeColumn : removes a column from an RTable.
--   The column is specified by ColumnName.
--   If this ColumnName does not exist in the RTuple of the input RTable
--   then nothing is happened, the RTuple remains intact.
removeColumn ::
       ColumnName  -- ^ Column to be removed
    -> RTable      -- ^ input RTable 
    -> RTable      -- ^ output RTable 
removeColumn col rtabSrc = do
      srcRtup <- rtabSrc
      let targetRtup = HM.delete col srcRtup  
      return targetRtup

-- | addColumn: adds a column to an RTable
addColumn ::
      ColumnName      -- ^ name of the column to be added
  ->  RDataType       -- ^ Default value of the new column. All RTuples will initially have this value in this column
  ->  RTable          -- ^ Input RTable
  ->  RTable          -- ^ Output RTable
addColumn name initVal rtabSrc = do
    srcRtup <- rtabSrc
    let targetRtup = HM.insert name initVal srcRtup
    return targetRtup


-- | Filter (i.e. selection operator). A short name for the 'runRFilter' function
f = runRfilter

-- | Executes an RFilter operation
runRfilter ::
    RPredicate
    -> RTable
    -> RTable
runRfilter rpred rtab = 
    if isRTabEmpty rtab
        then emptyRTable
        else 
            V.filter rpred rtab

-- | RTable Projection operator. A short name for the 'runProjection' function
p = runProjection

-- | Implements RTable projection operation.
-- If a column name does not exist, then an empty RTable is returned.
runProjection :: 
    [ColumnName]  -- ^ list of column names to be included in the final result RTable
    -> RTable
    -> RTable
runProjection colNamList irtab = 
    if isRTabEmpty irtab
        then
            emptyRTable
        else
            do -- RTable is a Monad
                srcRtuple <- irtab
                let
                    -- 1. get original column value (in this case it is a list of values :: Maybe RDataType)
                    srcValueL = Data.List.map (\colName -> rtupLookup colName srcRtuple) colNamList

                -- if there is at least one Nothing value then an non-existing column name has been asked. 
                if Data.List.elem Nothing srcValueL 
                    then -- return an empty RTable
                        emptyRTable
                    else
                        let 
                            -- 2. create the new RTuple                        
                            valList = Data.List.map (\(Just v) -> v) srcValueL -- get rid of Maybe
                            targetRtuple = rtupleFromList (Data.List.zip colNamList valList) -- HM.fromList
                        in return targetRtuple                        


-- | Implements RTable projection operation.
-- If a column name does not exist, then the returned RTable includes this column with a Null
-- value. This projection implementation allows missed hits.
runProjectionMissedHits :: 
    [ColumnName]  -- ^ list of column names to be included in the final result RTable
    -> RTable
    -> RTable
runProjectionMissedHits colNamList irtab = 
    if isRTabEmpty irtab
        then
            emptyRTable
        else
            do -- RTable is a Monad
                srcRtuple <- irtab
                let
                    -- 1. get original column value (in this case it is a list of values)
                    srcValueL = Data.List.map (\src -> HM.lookupDefault       Null -- return Null if value cannot be found based on column name 
                                                                    src   -- column name to look for (source) - i.e., the key in the HashMap
                                                                    srcRtuple  -- source RTuple (i.e., a HashMap ColumnName RDataType)
                                    ) colNamList
                    -- 2. create the new RTuple
                    targetRtuple = HM.fromList (Data.List.zip colNamList srcValueL)
                return targetRtuple

-- | returns the N first 'RTuple's of an 'RTable'
limit ::
       Int          -- ^ number of N 'RTuple's to return
    -> RTable       -- ^ input 'RTable'
    -> RTable       -- ^ output 'RTable'
limit n r1 = V.take n r1

{-
-- | restrictNrows: returns the N first rows of an RTable
restrictNrows ::
       Int          -- ^ number of N rows to select
    -> RTable       -- ^ input RTable
    -> RTable       -- ^ output RTable
restrictNrows n r1 = V.take n r1
-}

-- | 'RTable' anti-join operator. A short name for the 'runAntiJoin' function
aJ = runAntiJoin

-- | Implements the anti-Join operation between two RTables (any type of join predicate is allowed)
-- It returns the 'RTuple's from the left 'RTable' that DONT match with the right 'RTable'.
runAntiJoin ::
    RJoinPredicate
    -> RTable
    -> RTable
    -> RTable
runAntiJoin jpred tabDriver tabProbed = 
    if isRTabEmpty tabDriver || isRTabEmpty tabProbed
        then
            emptyRTable
        else
            d tabDriver $ sJ jpred tabDriver tabProbed 
{-            do 
                rtupDrv <- tabDriver
                -- this is the equivalent of a nested loop with tabDriver playing the role of the driving table and tabProbed the probed table
                V.foldr' (\t accum -> 
                            if (not $ jpred rtupDrv t) 
                                then 
                                    -- insert joined tuple to result table (i.e. the accumulator)
                                    insertAppendRTab (rtupDrv) accum
                                else 
                                    -- keep the accumulator unchanged
                                    accum
                        ) emptyRTable tabProbed 
-}

-- | 'RTable' semi-join operator. A short name for the 'runSemiJoin' function
sJ = runSemiJoin

-- | Implements the semi-Join operation between two RTables (any type of join predicate is allowed)
-- It returns the 'RTuple's from the left 'RTable' that match with the right 'RTable'.
-- Note that if an 'RTuple' from the left 'RTable' matches more than one 'RTuple's from the right 'RTable'
-- the semi join operation will return only a single 'RTuple'.    
runSemiJoin ::
    RJoinPredicate
    -> RTable
    -> RTable
    -> RTable
runSemiJoin jpred tabDriver tabProbed = 
    if isRTabEmpty tabDriver || isRTabEmpty tabProbed
        then
            emptyRTable
        else 
            do 
                rtupDrv <- tabDriver
                -- this is the equivalent of a nested loop with tabDriver playing the role of the driving table and tabProbed the probed table
                V.foldr' (\t accum -> 
                            if (jpred rtupDrv t) 
                                then 
                                    -- insert joined tuple to result table (i.e. the accumulator)
                                    insertAppendRTab (rtupDrv) accum
                                else 
                                    -- keep the accumulator unchanged
                                    accum
                        ) emptyRTable tabProbed 

-- | 'RTable' Inner Join Operator. A short name for the 'runInnerJoinO' function
iJ = runInnerJoinO

-- | Implements an Inner Join operation between two RTables (any type of join predicate is allowed)
-- Note that this operation is implemented as a 'Data.HashMap.Strict' union, which means "the first 
-- Map (i.e., the left RTuple) will be prefered when dublicate keys encountered with different values. That is, in the context of 
-- joining two RTuples the value of the first (i.e., left) RTuple on the common key will be prefered.
runInnerJoin ::
    RJoinPredicate
    -> RTable
    -> RTable
    -> RTable
runInnerJoin jpred irtab1 irtab2 =  
    if (isRTabEmpty irtab1) || (isRTabEmpty irtab2)
        then
            emptyRTable
        else 
            do 
                rtup1 <- irtab1
                rtup2 <- irtab2
                let targetRtuple = 
                        if (jpred rtup1 rtup2)
                        then HM.union rtup1 rtup2                 
                        else HM.empty
                removeEmptyRTuples (return targetRtuple)
                    where removeEmptyRTuples = f (not.isRTupEmpty) 

-- | Implements an Inner Join operation between two RTables (any type of join predicate is allowed)
-- This Inner Join implementation follows Oracle DB's convention for common column names.
-- When we have two tuples t1 and t2 with a common column name (lets say \"Common\"), then the resulting tuple after a join
-- will be \"Common\", \"Common_1\", so a \"_1\" suffix is appended. The tuple from the left table by convention retains the original column name.
-- So \"Column_1\" is the column from the right table. If \"Column_1\" already exists, then \"Column_2\" is used.
runInnerJoinO ::
    RJoinPredicate
    -> RTable
    -> RTable
    -> RTable
runInnerJoinO jpred tabDriver tabProbed =  
    if isRTabEmpty tabDriver || isRTabEmpty tabProbed
        then
            emptyRTable
        else 
            do 
                rtupDrv <- tabDriver
                -- this is the equivalent of a nested loop with tabDriver playing the role of the driving table and tabProbed the probed table
                V.foldr' (\t accum -> 
                            if (jpred rtupDrv t) 
                                then 
                                    -- insert joined tuple to result table (i.e. the accumulator)
                                    insertAppendRTab (joinRTuples rtupDrv t) accum
                                else 
                                    -- keep the accumulator unchanged
                                    accum
                        ) emptyRTable tabProbed 


-- | Joins two RTuples into one. 
-- In this join we follow Oracle DB's convention when joining two tuples with some common column names.
-- When we have two tuples t1 and t2 with a common column name (lets say "Common"), then the resulitng tuple after a join
-- will be "Common", "Common_1", so a "_1" suffix is appended. The tuple from the left table by convention retains the original column name.
-- So "Column_1" is the column from the right table.
-- If "Column_1" already exists, then "Column_2" is used.
joinRTuples :: RTuple -> RTuple -> RTuple
joinRTuples tleft tright = 
    let
        -- change keys in tright what needs to be renamed because also appear in tleft
        -- first keep a copy of the tright pairs that dont need a key change
        dontNeedChange  = HM.difference tright tleft
        changedPart = changeKeys tleft tright
        -- create  a new version of tright, with no common keys with tleft
        new_tright = HM.union dontNeedChange changedPart
    in HM.union tleft new_tright  
        where 
            -- rename keys of right rtuple until there no more common keys with the left rtuple            
            changeKeys :: RTuple -> RTuple -> RTuple
            changeKeys tleft changedPart = 
                if isRTupEmpty (HM.intersection changedPart tleft)
                    then -- we are done, no more common keys
                        changedPart
                    else
                        -- there are still common keys to change
                        let
                            needChange = HM.intersection changedPart tleft -- (k,v) pairs that exist in changedPart and the keys also appear in tleft. Thus these keys have to be renamed
                            dontNeedChange  = HM.difference changedPart tleft -- (k,v) pairs that exist in changedPart and the keys dont appear in tleft. Thus these keys DONT have to be renamed
                            new_changedPart =  fromList $ Data.List.map (\(k,v) -> (newKey k, v)) $ toList needChange
                        in HM.union dontNeedChange (changeKeys tleft new_changedPart)
                          
            -- generate a new key as this:
            -- "hello" -> "hello_1"
            -- "hello_1" -> "hello_2"
            -- "hello_2" -> "hello_3"
            newKey :: ColumnName -> ColumnName
            newKey nameT  = 
                let 
                    name  = T.unpack nameT
                    lastChar = Data.List.last name
                    beforeLastChar = name !! (Data.List.length name - 2)
                in  
                    if beforeLastChar == '_'  &&  Data.Char.isDigit lastChar
                                    then T.pack $ (Data.List.take (Data.List.length name - 2) name) ++ ( '_' : (show $ (read (lastChar : "") :: Int) + 1) )
                                    else T.pack $ name ++ "_1"


-- | RTable Left Outer Join Operator. A short name for the 'runLeftJoin' function
lJ = runLeftJoin

-- | Implements a Left Outer Join operation between two RTables (any type of join predicate is allowed),
-- i.e., the rows of the left RTable will be preserved.
-- Note that when dublicate keys encountered that is, since the underlying structure for an RTuple is a Data.HashMap.Strict,
-- only one value per key is allowed. So in the context of joining two RTuples the value of the left RTuple on the common key will be prefered.
{-runLeftJoin ::
    RJoinPredicate
    -> RTable
    -> RTable
    -> RTable
runLeftJoin jpred leftRTab rtab = do
    rtupLeft <- leftRTab
    rtup <- rtab
    let targetRtuple = 
            if (jpred rtupLeft rtup)
            then HM.union rtupLeft rtup
            else HM.union rtupLeft (createNullRTuple colNamesList)
                    where colNamesList = HM.keys rtup
    --return targetRtuple

    -- remove repeated rtuples and keep just the rtuples of the preserving rtable
    iJ (jpred2) (return targetRtuple) leftRTab
        where 
            -- the left tuple is the extended (result of the outer join)
            -- the right tuple is from the preserving table
            -- we need to return True for those who are equal in the common set of columns
            jpred2 tL tR = 
                let left = HM.toList tL
                    right = HM.toList tR
                    -- in order to satisfy the join pred, all elements of right must exist in left
                in  Data.List.all (\(k,v) -> Data.List.elem (k,v) left) right
-}

-- | Implements a Left Outer Join operation between two RTables (any type of join predicate is allowed),
-- i.e., the rows of the left RTable will be preserved.
-- A Left Join :
-- @ 
-- tabLeft LEFT JOIN tabRight ON joinPred
-- @ 
-- where tabLeft is the preserving table can be defined as:
-- the Union between the following two RTables:
--
-- * The result of the inner join: tabLeft INNER JOIN tabRight ON joinPred
-- * The rows from the preserving table (tabLeft) that DONT satisfy the join condition, enhanced with the columns of tabRight returning Null values.
--
--  The common columns will appear from both tables but only the left table column's will retain their original name. 
runLeftJoin ::
    RJoinPredicate
    -> RTable
    -> RTable
    -> RTable
runLeftJoin jpred preservingTab tab = 
    if isRTabEmpty preservingTab
        then 
            emptyRTable
        else
            if isRTabEmpty tab
                then
                    -- return the preserved tab 
                    preservingTab

                else 
                    -- we know that both the preservingTab and tab are non empty 
                    let 
                        unionFstPart = iJ jpred preservingTab tab 

                        -- debug
                        -- !dummy1 = trace ("unionFstPart:\n" ++ show unionFstPart) True

                        -- project only the preserving tab's columns
                        fstPartProj = p (getColumnNamesFromRTab preservingTab) unionFstPart

                        -- the second part are the rows from the preserving table that dont join
                        -- we will use the Difference operations for this
                        unionSndPart = 
                            let 
                                difftab = d preservingTab fstPartProj -- unionFstPart

                                -- now enhance the result with the columns of the right table
                            in iJ (\t1 t2 -> True) difftab (createSingletonRTable $ createNullRTuple $ (getColumnNamesFromRTab tab))
                        -- debug
                        -- !dummy2 = trace ("unionSndPart :\n" ++ show unionSndPart) True
                        -- !dummy3 = trace ("u unionFstPart unionSndPart :\n" ++ (show $ u unionFstPart unionSndPart)) True

                            {-   -- now enhance the result with the columns of the right table
                                joinedColumnsTab = iJ (\t1 t2 -> True) difftab (createSingletonRTable $ createNullRTuple $ (getColumnNamesFromRTab tab))
                                -- get only the columns from the two rtables that dont overlap
                                finalListOfColumns = getUniqueColumnNames (getColumnNamesFromRTab preservingTab) (getColumnNamesFromRTab tab) 
                                -- project only the columns from both rtables that dont overlap
                                -- otherwise, the union will hit an "ConflictingRTableStructures "Cannot run: Union, due to conflicting RTable structures" exception.
                            in p finalListOfColumns joinedColumnsTab
                            -}
                    in u unionFstPart unionSndPart


-- | Receives two lists of 'ColumnName's and returns the unique list of 'ColumnName's 
-- after concatenating the two and removing the names from the second one that are a prefix of the first one.
-- This function is intended to dedublicate common columns after a join (see 'ij'), where "ColA" for example,
-- will also appear as "ColA_1". This function DOES NOT dedublicate columns "ColA" and "ColAsomeSuffix", only
-- cases like this one "ColName_Num" (e.g., ColName_1, ColName_2, etc.)
-- Here is an example:
--
-- >>> getUniqueColumnNames ["ColA","ColB"] ["ColC","ColA", "ColA_1", "ColA_2", "ColA_A", "ColA_hello", "ColAhello"]
-- >>> ["ColA","ColB","ColC","ColA_A","ColA_hello","ColAhello"]
--
getUniqueColumnNamesAfterJoin :: [ColumnName] -> [ColumnName] -> [ColumnName]
getUniqueColumnNamesAfterJoin cl1 cl2 = 
    -- cl1  ++ Data.List.filter (\n2 -> and $ Data.List.map (\n1 -> not $ T.isPrefixOf n1 n2) cl1) cl2
    cl1  ++ Data.List.filter (\n2 -> and $ Data.List.map (\n1 -> not $ isMyPrefixOf n1 n2) cl1) cl2
    where
        -- We want a prefix test that will return:
        -- isMyPrefixOf "ColA" "ColA_1" == True
        -- isMyPrefixOf "ColA" "ColA_lala" == False
        -- isMyPrefixOf "ColA" "ColAlala" == False
        isMyPrefixOf :: ColumnName -> ColumnName -> Bool
        isMyPrefixOf cn1 cn2 = 
            let
                -- rip off suffixes of the form "_Num" e.g., "Col_1", "Col_2"  -> "Col", "Col"
                cn2_new = T.dropWhileEnd (\c -> c == '_') $ T.dropWhileEnd (\c -> isDigit c) cn2
                -- and now compare for equality
            in cn1 == cn2_new

-- | RTable Right Outer Join Operator. A short name for the 'runRightJoin' function
rJ = runRightJoin

-- | Implements a Right Outer Join operation between two RTables (any type of join predicate is allowed),
-- i.e., the rows of the right RTable will be preserved.
-- A Right Join :
-- @ 
-- tabLeft RIGHT JOIN tabRight ON joinPred
-- @ 
-- where tabRight is the preserving table can be defined as:
-- the Union between the following two RTables:
--
-- * The result of the inner join: tabLeft INNER JOIN tabRight ON joinPred
-- * The rows from the preserving table (tabRight) that DONT satisfy the join condition, enhanced with the columns of tabLeft returning Null values.
--
--  The common columns will appear from both tables but only the right table column's will retain their original name. 
runRightJoin ::
    RJoinPredicate
    -> RTable
    -> RTable
    -> RTable
runRightJoin jpred tab preservingTab =
    if isRTabEmpty preservingTab
        then 
            emptyRTable
        else
            if isRTabEmpty tab
                then
                    preservingTab        
                else     
                    -- we know that both the preservingTab and tab are non empty 
                    let 
                        unionFstPart =                             
                            iJ (flip jpred) preservingTab tab --tab    -- we used the preserving table as the left table in the inner join, 
                                                                    -- in order to retain the original column names for the common columns 
                        -- debug
                        -- !dummy1 = trace ("unionFstPart:\n" ++ show unionFstPart) True

                        -- project only the preserving tab's columns
                        fstPartProj = 
                            p (getColumnNamesFromRTab preservingTab) unionFstPart

                        -- the second part are the rows from the preserving table that dont join
                        -- we will use the Difference operations for this
                        unionSndPart = 
                            let 
                                difftab = 
                                    d preservingTab   fstPartProj -- unionFstPart 
                                -- now enhance the result with the columns of the left table
                            in iJ (\t1 t2 -> True) difftab (createSingletonRTable $ createNullRTuple $ (getColumnNamesFromRTab tab))
                        -- debug
                        -- !dummy2 = trace ("unionSndPart :\n" ++ show unionSndPart) True
                        -- !dummy3 = trace ("u unionFstPart unionSndPart :\n" ++ (show $ u unionFstPart unionSndPart)) True
                    in u unionFstPart unionSndPart



-- | Implements a Right Outer Join operation between two RTables (any type of join predicate is allowed)
-- i.e., the rows of the right RTable will be preserved.
-- Note that when dublicate keys encountered that is, since the underlying structure for an RTuple is a Data.HashMap.Strict,
-- only one value per key is allowed. So in the context of joining two RTuples the value of the right RTuple on the common key will be prefered.
{-runRightJoin ::
    RJoinPredicate
    -> RTable
    -> RTable
    -> RTable
runRightJoin jpred rtab rightRTab = do
    rtupRight <- rightRTab
    rtup <- rtab
    let targetRtuple = 
            if (jpred rtup rtupRight)
            then HM.union rtupRight rtup
            else HM.union rtupRight (createNullRTuple colNamesList)
                    where colNamesList = HM.keys rtup
    return targetRtuple-}

-- | RTable Full Outer Join Operator. A short name for the 'runFullOuterJoin' function
foJ = runFullOuterJoin

-- | Implements a Full Outer Join operation between two RTables (any type of join predicate is allowed)
-- A full outer join is the union of the left and right outer joins respectively.
-- The common columns will appear from both tables but only the left table column's will retain their original name (just by convention).
runFullOuterJoin ::
    RJoinPredicate
    -> RTable
    -> RTable
    -> RTable
runFullOuterJoin jpred leftRTab rightRTab = -- (lJ jpred leftRTab rightRTab) `u` (rJ jpred leftRTab rightRTab) -- (note that `u` eliminates dublicates)
    let
        --
        -- we want to get to this union:
        -- (lJ jpred leftRTab rightRTab) `u` (d (rJ jpred leftRTab rightRTab) (ij (flip jpred) rightRTab leftRTab))
        -- 
        -- The problem is with the change of column names that are common in both tables. In the right part of the union
        -- rightTab has preserved its original column names while in the left part the have changed to "_1" suffix. 
        -- So we cant do the union as is, we need to change the second part of the union (right one) to have the same column names
        -- as the firts part, i.e., original names for leftTab and changed names for rightTab.
        --
        unionFstPart = lJ jpred leftRTab rightRTab -- in unionFstPart rightTab's columns have changed names
        
        -- we need to construct the 2nd part of the union with leftTab columns unchanged and rightTab changed        
        unionSndPartTemp1 = d (rJ jpred leftRTab rightRTab) (iJ (flip jpred) rightRTab leftRTab)        
        -- isolate the columns of the rightTab
        unionSndPartTemp2 = p (getColumnNamesFromRTab rightRTab) unionSndPartTemp1
        -- this join is a trick in order to change names of the rightTab
        unionSndPart = iJ (\t1 t2 -> True) (createSingletonRTable $ createNullRTuple $ (getColumnNamesFromRTab leftRTab)) unionSndPartTemp2
    in unionFstPart `u` unionSndPart

-- | RTable Union Operator. A short name for the 'runUnion' function
u = runUnion

-- We cannot implement Union like the following (i.e., union of lists) because when two identical RTuples that contain Null values are checked for equality
-- equality comparison between Null returns always false. So we have to implement our own union using the isNull function.

-- | Implements the union of two RTables as a union of two lists (see 'Data.List').
-- Duplicates, and elements of the first list, are removed from the the second list, but if the first list contains duplicates, so will the result
{-runUnion :: 
    RTable
    -> RTable
    -> RTable
runUnion rt1 rt2 = 
    let ls1 = V.toList rt1
        ls2 = V.toList rt2
        resultLs = Data.List.union ls1 ls2
    in  V.fromList resultLs
-}

-- | Implements the union of two RTables.
-- Note that dublicate 'RTuple' elimination takes places.
runUnion :: 
    RTable
    -> RTable
    -> RTable
runUnion rt1 rt2 =
    if isRTabEmpty rt1 && isRTabEmpty rt2
        then 
            emptyRTable
        else
            if isRTabEmpty rt1
                then rt2 
                else
                    if isRTabEmpty rt2
                        then rt1
                        else 
                            -- check similarity of rtable structures 
                            if rtabsSameStructure rt1 rt2  
                                then
                                    -- run the union
                                    -- construct the union result by concatenating the left table with the subset of tuple from the right table that do
                                    -- not appear in the left table (i.e, remove dublicates)
                                    rt1 V.++ (V.foldr (un) emptyRTable rt2)
                                else 
                                   throw $ ConflictingRTableStructures "Cannot run: Union, due to conflicting RTable structures." 
    where
        un :: RTuple -> RTable -> RTable
        un tupRight acc = 
            -- Can we find tupRight in the left table?            
            if didYouFindIt tupRight rt1 
                then acc   -- then discard tuplRight ,leave result unchanged          
                else V.snoc acc tupRight  -- else insert tupRight into final result

-- | Implements the union-all of two RTables. I.e., a union without dublicate 'RTuple' elimination. Runs in O(m+n).
runUnionAll :: 
    RTable
    -> RTable
    -> RTable
runUnionAll rt1 rt2 =
    if isRTabEmpty rt1 && isRTabEmpty rt2
        then 
            emptyRTable
        else
            if isRTabEmpty rt1
                then rt2 
                else
                    if isRTabEmpty rt2
                        then rt1
                        else  
                            -- check similarity of rtable structures
                            if rtabsSameStructure rt1 rt2  
                                then
                                    -- run the union
                                    rt1 V.++ rt2  -- Data.Vector concatenation O(n+m)
                                else 
                                   throw $ ConflictingRTableStructures "Cannot run: UnionAll, due to conflicting RTable structures."                             

-- | RTable Intersection Operator. A short name for the 'runIntersect' function
i = runIntersect

-- | Implements the intersection of two RTables 
runIntersect :: 
    RTable
    -> RTable
    -> RTable
runIntersect rt1 rt2 =
    if isRTabEmpty rt1 || isRTabEmpty rt2 
        then
            emptyRTable
        else 
            -- check similarity of rtable structures
            if rtabsSameStructure rt1 rt2  
                then
                    -- run the intersection
                    -- construct the intersect result by traversing the left table and checking if each tuple exists in the right table
                    V.foldr (intsect) emptyRTable rt1
                else 
                   throw $ ConflictingRTableStructures "Cannot run: Intersect, due to conflicting RTable structures." 
    where
        intsect :: RTuple -> RTable -> RTable
        intsect tupLeft acc = 
            -- Can we find tupLeft in the right table?            
            if didYouFindIt tupLeft rt2 
                then V.snoc acc tupLeft  -- then insert tupLeft into final result
                else acc  -- else discard tuplLeft ,leave result unchanged          

{-
runIntersect rt1 rt2 = 
    let ls1 = V.toList rt1
        ls2 = V.toList rt2
        resultLs = Data.List.intersect ls1 ls2
    in  V.fromList resultLs

-}

-- | RTable Difference Operator. A short name for the 'runDiff' function
d = runDiff

-- Test it with this, from ghci:
-- ---------------------------------
-- :set -XOverloadedStrings
-- :m + Data.HashMap.Strict
--
-- let t1 = fromList [("c1",RInt 1),("c2", Null)]
-- let t2 = fromList [("c1",RInt 2),("c2", Null)]
-- let t3 = fromList [("c1",RInt 3),("c2", Null)]
-- let t4 = fromList [("c1",RInt 4),("c2", Null)]
-- :m + Data.Vector
-- let tab1 = Data.Vector.fromList [t1,t2,t3,t4]
-- let tab2 = Data.Vector.fromList [t1,t2]

-- > d tab1 tab2
-- ---------------------------------

-- | Implements the set Difference of two RTables as the diff of two lists (see 'Data.List').
runDiff :: 
    RTable
    -> RTable
    -> RTable
runDiff rt1 rt2 =
    if isRTabEmpty rt1
        then
            emptyRTable
        else 
            if isRTabEmpty rt2
                then
                    rt1
                else  
                    -- check similarity of rtable structures
                    if rtabsSameStructure rt1 rt2  
                        then
                            -- run the minus
                            -- construct the diff result by traversing the left table and checking if each tuple exists in the right table
                            V.foldr (diff) emptyRTable rt1
                        else 
                           throw $ ConflictingRTableStructures "Cannot run: Minus, due to conflicting RTable structures."                             
    where
        diff :: RTuple -> RTable -> RTable
        diff tupLeft acc = 
            -- Can we find tupLeft in the right table?            
            if didYouFindIt tupLeft rt2 
                then acc  -- then discard tuplLeft ,leave result unchanged
                else V.snoc acc tupLeft  -- else insert tupLeft into final result

-- Important Note:
-- we need to implement are own equality comparison function "areTheyEqual" and not rely on the instance of Eq defined for RDataType above
-- because of "Null Logic". If we compare two RTuples that have Null values in any column, then these can never be equal, because
-- Null == Null returns False.
-- However, in SQL when you run a minus or interesection between two tables containing Nulls, it works! For example:
-- with q1
-- as (
--     select rownum c1, Null c2
--     from dual
--     connect by level < 5
-- ),
-- q2
-- as (
--     select rownum c1, Null c2
--     from dual
--     connect by level < 3
-- )
-- select *
-- from q1
-- minus
-- select *
-- from q2
-- 
-- q1:
-- C1  | C2 
-- ---   ---
-- 1   |                                      
-- 2   |                                     
-- 3   |                                      
-- 4   |                                     
--
-- q2:
-- C1  | C2 
-- ---   ---
-- 1   |                                      
-- 2   |                                     
--
-- And it will return:                
-- C1  | C2 
-- ---   ---
-- 3   |                                      
-- 4   |                                     
-- So for Minus and Intersection, when we compare RTuples we need to "bypass" the Null Logic
didYouFindIt :: RTuple -> RTable -> Bool
didYouFindIt searchTup tab = 
    V.foldr (\t acc -> (areTheyEqual searchTup t) || acc) False tab 
areTheyEqual :: RTuple -> RTuple -> Bool
areTheyEqual t1 t2 =  -- foldrWithKey :: (k -> v -> a -> a) -> a -> HashMap k v -> a
    HM.foldrWithKey (accumulator) True t1
        where 
            accumulator :: ColumnName -> RDataType -> Bool -> Bool
            accumulator colName val acc = 
                case val of
                    Null -> 
                            case (t2<!>colName) of
                                Null -> acc -- -- i.e., True && acc ,if both columns are Null then return equality
                                _    -> False -- i.e., False && acc
                    _    -> case (t2<!>colName) of
                                Null -> False -- i.e., False && acc
                                _    -> (val == t2<!>colName) && acc -- else compare them as normal

                        -- NOTE:
                        -- the following piece of code does not work because val == Null always returns False !!!!

                        -- if (val == Null) && (t2<!>colName == Null) 
                        --     then acc -- i.e., True && acc
                        --     -- else compare them as normal
                        --     else  (val == t2<!>colName) && acc

{-
runDiff rt1 rt2 = 
    let ls1 = V.toList rt1
        ls2 = V.toList rt2
        resultLs = ls1 Data.List.\\ ls2
    in  V.fromList resultLs
-}
            
-- | Aggregation Operator. A short name for the 'runAggregation' function
rAgg = runAggregation

-- | Implements the aggregation operation on an RTable
-- It aggregates the specific columns in each AggOperation and returns a singleton RTable 
-- i.e., an RTable with a single RTuple that includes only the agg columns and their aggregated value.
runAggregation ::
        [RAggOperation]  -- ^ Input Aggregate Operations
    ->  RTable          -- ^ Input RTable
    ->  RTable          -- ^ Output singleton RTable
runAggregation [] rtab = rtab
runAggregation aggOps rtab =          
    if isRTabEmpty rtab
        then emptyRTable
        else
            createSingletonRTable (getResultRTuple aggOps rtab)
            where
                -- creates the final aggregated RTuple by applying all agg operations in the list
                -- and UNIONs all the intermediate agg RTuples to a final aggregated Rtuple
                getResultRTuple :: [RAggOperation] -> RTable -> RTuple
                getResultRTuple [] _ = emptyRTuple
                getResultRTuple (agg:aggs) rt =                    
                    let RAggOperation { sourceCol = src, targetCol = trg, aggFunc = aggf } = agg                        
                    in (getResultRTuple aggs rt) `HM.union` (aggf rt)   -- (aggf rt) `HM.union` (getResultRTuple aggs rt) 

-- | Order By Operator. A short name for the 'runOrderBy' function
rO = runOrderBy

-- | Implements the ORDER BY operation.
-- First column in the input list has the highest priority in the sorting order
-- We treat Null as the maximum value (anything compared to Null is smaller).
-- This way Nulls are send at the end (i.e.,  "Nulls Last" in SQL parlance). This is for Asc ordering.
-- For Desc ordering, we have the opposite. Nulls go first and so anything compared to Null is greater.
-- @
--      SQL example
-- with q 
-- as (select case when level < 4 then level else NULL end c1 -- , level c2
-- from dual
-- connect by level < 7
-- ) 
-- select * 
-- from q
-- order by c1
--
-- C1
--    ----
--     1
--     2
--     3
--     Null
--     Null
--     Null
--
-- with q 
-- as (select case when level < 4 then level else NULL end c1 -- , level c2
-- from dual
-- connect by level < 7
-- ) 
-- select * 
-- from q
-- order by c1 desc

--     C1
--     --
--     Null
--     Null    
--     Null
--     3
--     2
--     1
-- @
runOrderBy ::
        [(ColumnName, OrderingSpec)]  -- ^ Input ordering specification
    ->  RTable -- ^ Input RTable
    ->  RTable -- ^ Output RTable
runOrderBy ordSpec rtab = 
    if isRTabEmpty rtab
        then emptyRTable
    else 
        let unsortedRTupList = rtableToList rtab
            sortedRTupList = Data.List.sortBy (\t1 t2 -> compareTuples ordSpec t1 t2) unsortedRTupList
        in rtableFromList sortedRTupList
        where 
            compareTuples :: [(ColumnName, OrderingSpec)] -> RTuple -> RTuple -> Ordering
            compareTuples [] t1 t2 = EQ
            compareTuples ((col, colordspec) : rest) t1 t2 = 
                -- if they are equal or both Null on the column in question, then go to the next column
                if nvl (t1 <!> col) (RText "I am Null baby!") == nvl (t2 <!> col) (RText "I am Null baby!")
                    then compareTuples rest t1 t2
                    else -- Either one of the two is Null or are Not Equal
                         -- so we need to compare t1 versus t2
                         -- the GT, LT below refer to t1 wrt to t2
                         -- In the following we treat Null as the maximum value (anything compared to Null is smaller).
                         -- This way Nulls are send at the end (i.e., the default is "Nulls Last" in SQL parlance)
                        if isNull (t1 <!> col)
                            then 
                                case colordspec of
                                    Asc ->  GT -- t1 is GT than t2 (Nulls go to the end)
                                    Desc -> LT
                            else 
                                if isNull (t2 <!> col)
                                    then 
                                        case colordspec of
                                            Asc ->  LT -- t1 is LT than t2 (Nulls go to the end)
                                            Desc -> GT
                                else
                                    -- here we cant have Nulls
                                    case compare (t1 <!> col) (t2 <!> col) of
                                        GT -> if colordspec == Asc 
                                                then GT else LT
                                        LT -> if colordspec == Asc 
                                                then LT else GT

-- | Group By Operator. A short name for the 'runGroupBy' function
rG = runGroupBy

-- RGroupBy  { gpred :: RGroupPredicate, aggList :: [RAggOperation], colGrByList :: [ColumnName] }

-- hint: use Data.List.GroupBy for the grouping and Data.List.foldl' for the aggregation in each group
{--type RGroupPredicate = RTuple -> RTuple -> Bool

data RAggOperation = 
          RSum ColumnName  -- ^  sums values in the specific column
        | RCount ColumnName -- ^ count of values in the specific column
        | RCountDist ColumnName -- ^ distinct count of values in the specific column
        | RAvg ColumnName  -- ^ average of values in the specific column
        | RMin ColumnName -- ^ minimum of values in the specific column
        | RMax ColumnName
--}

-- | Implement a grouping operation over an 'RTable'. No aggregation takes place.
-- It returns the individual groups as separate 'RTable's in a list. In total the initial set of 'RTuple's is retained.
-- If an empty 'RTable' is provided as input, then a [\"empty RTable\"] is returned.
groupNoAggList :: 
       RGroupPredicate   -- ^ Grouping predicate, in order to form the groups of 'RTuple's (it defines when two 'RTuple's should be included in the same group)
    -> [ColumnName]      -- ^ List of grouping column names (GROUP BY clause in SQL)
                         --   We assume that all RTuples in the same group have the same value in these columns
    -> RTable            -- ^ input 'RTable'
    -> [RTable]          -- ^ output list of 'RTable's where each one corresponds to a group
groupNoAggList gpred cols rtab = 
    if isRTabEmpty rtab
        then [emptyRTable]
        else 
            let 
                -- 1. form the groups of RTuples
                    -- a. first sort the Rtuples based on the grouping columns
                    -- This is a very important step if we want the groupBy operation to work. This is because grouping on lists is
                    -- implemented like this: group "Mississippi" = ["M","i","ss","i","ss","i","pp","i"]
                    -- So we have to sort the list first in order to get the right grouping:
                    -- group (sort "Mississippi") = ["M","iiii","pp","ssss"]
                
                listOfRTupSorted = rtableToList $ runOrderBy (createOrderingSpec cols) rtab

                -- debug
               -- !dummy1 = trace (show listOfRTupSorted) True
                    
                    -- b then produce the groups
                listOfRTupGroupLists = Data.List.groupBy gpred listOfRTupSorted       

                -- debug
                -- !dummy2 = trace (show listOfRTupGroupLists) True

                -- 2. turn each (sub)list of RTuples representing a Group into an RTable 
                --    Note: each RTable in this list will hold a group of RTuples that all have the same values in the input grouping columns
                --    (which must be compatible with the input grouping predicate)
                listofGroupRtabs = Data.List.map (rtableFromList) listOfRTupGroupLists
            in listofGroupRtabs

-- | Concatenates a list of 'RTable's to a single RTable. Essentially, it unions (see 'runUnion') all 'RTable's of the list.
concatRTab :: [RTable] -> RTable
concatRTab rtabsl = Data.List.foldr1 (u) rtabsl

-- | Implement a grouping operation over an 'RTable'. No aggregation takes place.
-- The output 'RTable' has exactly the same 'RTuple's, as the input, but these are grouped based on the input grouping predicate.
-- If an empty 'RTable' is provided as input, then an empty 'RTable' is returned.
groupNoAgg :: 
       RGroupPredicate   -- ^ Grouping predicate, in order to form the groups of 'RTuple's (it defines when two 'RTuple's should be included in the same group)
    -> [ColumnName]      -- ^ List of grouping column names (GROUP BY clause in SQL)
                         --   We assume that all 'RTuple's in the same group have the same value in these columns
    -> RTable            -- ^ input 'RTable'
    -> RTable            -- ^ output 'RTable'
groupNoAgg gpred cols rtab = 
    if isRTabEmpty rtab
        then emptyRTable
        else 
            let
                listofGroupRtabs = groupNoAggList gpred cols rtab
            in concatRTab listofGroupRtabs -- Data.List.foldr1 (u) listofGroupRtabs

-- | Implements the GROUP BY operation over an 'RTable'. 
runGroupBy ::
       RGroupPredicate   -- ^ Grouping predicate, in order to form the groups of RTuples (it defines when two RTuples should be included in the same group)
    -> [RAggOperation]   -- ^ Aggregations to be applied on specific columns
    -> [ColumnName]      -- ^ List of grouping column names (GROUP BY clause in SQL)
                         --   We assume that all RTuples in the same group have the same value in these columns
    -> RTable            -- ^ input RTable
    -> RTable            -- ^ output RTable
runGroupBy gpred aggOps cols rtab =  
    if isRTabEmpty rtab
        then 
            emptyRTable
        else 
            let -- rtupList = V.toList rtab
                
                -- 1. form the groups of RTuples
                    -- a. first sort the Rtuples based on the grouping columns
                    -- This is a very important step if we want the groupBy operation to work. This is because grouping on lists is
                    -- implemented like this: group "Mississippi" = ["M","i","ss","i","ss","i","pp","i"]
                    -- So we have to sort the list first in order to get the right grouping:
                    -- group (sort "Mississippi") = ["M","iiii","pp","ssss"]                
                --listOfRTupSorted = rtableToList $ runOrderBy (createOrderingSpec cols) rtab

                -- debug
                -- !dummy1 = trace (show listOfRTupSorted) True
                    
                    -- b then produce the groups
                --listOfRTupGroupLists = Data.List.groupBy gpred listOfRTupSorted       

                -- debug
                -- !dummy2 = trace (show listOfRTupGroupLists) True

                -- 2. turn each (sub)list of Rtuples representing a Group into an RTable in order to apply aggregation
                --    Note: each RTable in this list will hold a group of RTuples that all have the same values in the input grouping columns
                --    (which must be compatible with the input grouping predicate)
                --listofGroupRtabs = Data.List.map (rtableFromList) listOfRTupGroupLists

                listofGroupRtabs = groupNoAggList gpred cols rtab

                -- 3. We need to keep the values of the grouping columns (e.g., by selecting the fisrt row) from each one of these RTables,
                --    in order to "join them" with the aggregated RTuples that will be produced by the aggregation operations
                --    The following will produce a list of singleton RTables.
                listOfGroupingColumnsRtabs = Data.List.map ( (limit 1) . (p cols) ) listofGroupRtabs

                -- debug
                -- !dummy3 = trace (show listOfGroupingColumnsRtabs) True

                -- 4. Aggregate each group according to input and produce a list of (aggregated singleton RTables)
                listOfAggregatedRtabs = Data.List.map (rAgg aggOps) listofGroupRtabs

                -- debug
                -- !dummy4 = trace (show listOfAggregatedRtabs ) True

                -- 5. Join the two list of singleton RTables
                listOfFinalRtabs = 
                    if Data.List.null aggOps -- if the aggregation list is empty
                        then
                            listOfGroupingColumnsRtabs -- then returned just the group by columns
                        else
                            Data.List.zipWith (iJ (\t1 t2 -> True)) listOfGroupingColumnsRtabs listOfAggregatedRtabs

                -- debug
                -- !dummy5 = trace (show listOfFinalRtabs) True


                -- 6. Union all individual singleton RTables into the final RTable
            in  Data.List.foldr1 (u) listOfFinalRtabs
            

-- | Helper function to returned a fixed Ordering Specification 'OrderingSpec' from a list of 'ColumnName's
createOrderingSpec :: [ColumnName] -> [(ColumnName, OrderingSpec)]
createOrderingSpec cols = Data.List.zip cols (Data.List.take (Data.List.length cols) $ Data.List.repeat Asc)

-- | A short name for the 'runCombinedROp' function
rComb = runCombinedROp

-- | runCombinedROp: A Higher Order function that accepts as input a combination of unary ROperations e.g.,   (p plist).(f pred)
--   expressed in the form of a function (RTable -> Rtable) and applies this function to the input RTable.
--  In this sense we can also include a binary operation (e.g. join), if we partially apply the join to one RTable
--  e.g., (ij jpred rtab) . (p plist) . (f pred)
runCombinedROp ::
       (RTable -> RTable)  -- ^ input combined RTable operation
    -> RTable              -- ^ input RTable that the input function will be applied to
    -> RTable              -- ^ output RTable
runCombinedROp f rtab = f rtab


-- * ########## RTable DML Operations ##############

-- | O(n) append an RTuple to an RTable
-- Please note that this is an __immutable__ implementation of an 'RTable' insert.
-- This simply means that the insert operation returns a new 'RTable' and does not
-- affect the original 'RTable'.
insertAppendRTab :: RTuple -> RTable -> RTable
insertAppendRTab rtup rtab = -- V.snoc rtab rtup
    if isRTabEmpty rtab && isRTupEmpty rtup 
        then emptyRTable
        else
            if isRTabEmpty rtab && not(isRTupEmpty rtup)
                then
                    createSingletonRTable rtup 
                else
                    if not(isRTabEmpty rtab) && isRTupEmpty rtup 
                        then rtab 
                        else  -- non of the two is empty
                            -- check similarity of structure vefore the insert
                            if rtabsSameStructure (createSingletonRTable rtup) rtab
                                then 
                                    V.snoc rtab rtup
                                else 
                                    throw $ ConflictingRTableStructures "Cannot run: Insert Into Values (insertAppendRTab), due to conflicting RTable structures." 

-- | O(n) prepend an RTuple to an RTable
-- Please note that this is an __immutable__ implementation of an 'RTable' insert.
-- This simply means that the insert operation returns a new 'RTable' and does not
-- affect the original 'RTable'.
insertPrependRTab :: RTuple -> RTable -> RTable
insertPrependRTab rtup rtab = 
    if isRTabEmpty rtab && isRTupEmpty rtup 
        then emptyRTable
        else
            if isRTabEmpty rtab && not(isRTupEmpty rtup)
                then
                    createSingletonRTable rtup 
                else
                    if not(isRTabEmpty rtab) && isRTupEmpty rtup 
                        then rtab 
                        else  -- non of the two is empty    
                            -- check similarity of structure vefore the insert
                            if rtabsSameStructure (createSingletonRTable rtup) rtab
                                then 
                                    V.cons rtup rtab 
                                else 
                                    throw $ ConflictingRTableStructures "Cannot run: insertPrependRTab, due to conflicting RTable structures." 


-- | Insert an 'RTable' to an existing 'RTable'. This is equivalent to an @INSERT INTO SELECT@ caluse in SQL.
-- We want to insert into an 'RTable' the results of a \"subquery\", which in our case is materialized via the
-- input 'RTable'.
-- Please note that this is an __immutable__ implementation of an 'RTable' insert.
-- This simply means that the insert operation returns a new 'RTable' and does not
-- affect the original 'RTable'.
-- Also note that the source and target 'RTable's should have the same structure.
-- By \"structure\", we mean that the 'ColumnName's and the corresponding data types must match. Essentially what we record in the 'ColumnInfo'
-- must be the same for the two 'RTable's. Otherwise a 'ConflictingRTableStructures' exception will be thrown.
insertRTabToRTab ::
                RTable -- ^ Source 'RTable' to be inserted
                -> RTable -- ^ Target 'RTable'
                -> RTable -- ^ Final Result
insertRTabToRTab src trg = 
    if isRTabEmpty src
        then trg
        else
            if isRTabEmpty trg
                then src 
                else -- both src and trg are not empty
                    
                    -- check that both rtables have the same structure: 
                    --      num of columns, column data types and column names.
                    if rtabsSameStructure src trg 
                        then
                            -- run the insert (as a union all)
                            runUnionAll src trg
                        else 
                           throw $ ConflictingRTableStructures "Cannot run: Insert Into <TAB> RTuples, due to conflicting RTable structures." 

-- | Upsert (Update+Insert, aka Merge) Operation. We provide a source 'RTable' and a matching condition ('RUpsertPredicate') to the 'RTuple's 
-- of the target 'RTable'. An 'RTuple' from the target 'RTable' might match to a single only 'RTuple' in the source 'RTable', or not match at all. 
-- If it is matched to more than one 'RTuple's then an exception ('UniquenessViolationInUpsert') is thrown. 
-- When an 'RTuple' from the target 'RTable' is matched to a source 'RTuple', then the corresponding columns of the target 'RTuple' are updated
-- with the new values provided in the source 'RTuple'. This takes place for the target 'RTuple's that match but also that satisfy the input 
-- 'RPredicate'. Thus we can restrict further with a filter the 'RTuple's of the target 'RTable' where the update will take place.
-- Finally, the source 'RTuple's that did not match to the target 'RTable', are inserted (appended) to the target 'RTable'
--
-- Please note that this is an __immutable__ implementation of an 'RTable' upsert.
-- This simply means that the upsert operation returns a new 'RTable' and does not
-- affect the original 'RTable'.
-- Moreover, if we have multiple threads updating an 'RTable', due to immutability, each thread \"sees\" its own copy of
-- the 'RTable' and thus there is no need for locking the updated 'RTuple's, as happens in a common RDBMS.
--
-- Also note that the source and target 'RTable's should have the same structure.
-- By \"structure\", we mean that the 'ColumnName's and the corresponding data types must match. Essentially what we record in the 'ColumnInfo'
-- must be the same for the two 'RTable's. Otherwise a 'ConflictingRTableStructures' exception will be thrown.
--
-- @
--  An Example:
--  Source RTable: src = 
--      Id  |   Msg         | Other
--      ----|---------------|-------
--      1   |   "hello2"    |"a"    
--      2   |   "world2"    |"a"    
--      3   |   "new"       |"a"    
--
--  Target RTable: trg = 
--      Id  |   Msg         | Other
--      ----|---------------|-------
--      1   |   "hello1"    |"b"    
--      2   |   "world1"    |"b"    
--      4   |   "old"       |"b"    
--      5   |   "hello"     |"b"    
--
--  >>> upsertRTab  src
--                  RUpsertPredicate {matchCols = [\"Id\"], matchPred = \\t1 t2 -> t1 \<!\> \"Id\" == t2 \<!\> \"Id\" }
--                  [\"Msg\"]
--                  (\\t ->   let 
--                              msg = case toText (t \<!\> \"Msg\") of
--                                          Just t -> t
--                                          Nothing -> pack ""
--                          in (take 5 msg) == (pack "hello")
--                  )  -- Msg like "hello%"
--                  trg
--
--  Result RTable: rslt = 
--      Id  |   Msg         | Other
--      ----|---------------|-------
--      1   |   "hello2"    |"b"   (Note that only column \"Msg\" has been overwritten, as per the 3rd argument) 
--      2   |   "world1"    |"b"    
--      3   |   "new"       |"a"    
--      4   |   "old"       |"b"    
--      5   |   "hello"     |"b"    
-- @
--
upsertRTab ::
    RTable -- ^ Source 'RTable', i.e., the equivalent to an SQL @USING@ subclause 
    -> RUpsertPredicate -- ^ The 'RTuple' matching predicate for the merge operation

    -> [ColumnName] -- ^ List of column names to be updated with the corresponding new values coming from the source 'RTuple's 
                    -- that match with the target 'RTuple's based on the 'RUpsertPredicate' 
    -> RPredicate  -- ^ A filter that specifies the target 'RTuple's to be updated
    -> RTable -- ^ The target 'RTable'
    -> RTable -- ^ Final Result
upsertRTab srcTab upsPred cols fpred trgTab = 
    {-
        README PLEASE: Upsert Algorithm

        upsertRTab srcTab upsPred cols fpred trgTab = 

            insertRTabToRTab (Table S1) $ UNION (Table T1) (Table T2) (Table T3) 

            where
                Table T1 =  let 
                                tab  = p (cols ++ (matchCols upsPred)) $ srcTab <semi-join> (f fpred trgTab) <on> (matchPred upsPred) 
                            
                                -- this projection will ensure that the right columns will be overwritten
                                -- dont forget that srcTab and trgTab have the same structure, thus also the same column names 
                            in  p (getColumnNamesFromRTab trgTab) $ tab <inner-join> (f fpred trgTab) <on> (matchPred upsPred) 
 
                        --      1   |   "hello2"    |"a" 

                Table T2 = (f fpred trgTab) <anti-join> srcTab <on> (matchPred upsPred)
                        --      5   |   "hello"     |"b"    

                Table T3 = f (not . fpred) trgTab
                        --      2   |   "world1"    |"b"    
                        --      4   |   "old"       |"b"    

                Table S1 = srcTab <anti-join> trgTab <on> (matchPred upsPred)
                        --      3   |   "new"       |"a"    
        
        Also, Table T1 must be unique if we group by the columns participating in upsPred (i.e., matchCols upsPred)

    -}
    if isRTabEmpty srcTab
        then trgTab
        else
            if isRTabEmpty trgTab
                then srcTab 
                else -- both src and trg are not empty
                    
                    -- check that both rtables have the same structure: 
                    --      num of columns, column data types and column names.
                    if rtabsSameStructure srcTab trgTab 
                        then
                            -- check uniqueness condition at srcTab: group by the matching columns and make sure there are no dublicates
                            if not $ isRTabEmpty $
                                            f (\t -> t <!> "numOfRows" > 1) $
                                            rG  (\t1 t2 -> Data.List.foldr (\col acc -> (t1 <!> col == t2 <!> col) && acc) True (matchCols upsPred))
                                                [raggCountStar "numOfRows"]
                                                (matchCols upsPred)
                                                srcTab
                                then
                                    throw $ UniquenessViolationInUpsert "Cannot run: Upsert because the source RTable is not unique in the matching columns." 
                                else
                                    -- run the upsert
                                    let
                                        t1 =    let
                                                    tab  = p (cols ++ (matchCols upsPred)) $ sJ (matchPred upsPred) srcTab (f fpred trgTab)
                                                    -- debug
                                                    -- !dummy1 = trace ("tab:\n" ++ show tab) True

                                                    -- this projection will ensure that the right columns will be overwritten
                                                    -- dont forget that srcTab and trgTab have the same structure, thus also the same column names 
                                                in  p (getColumnNamesFromRTab trgTab) $ iJ (matchPred upsPred) tab (f fpred trgTab)  
                                        
                                        -- debug
                                        -- !dummy2 = trace ("t1:\n" ++ show t1) True

                                        t2 =  aJ (matchPred upsPred) (f fpred trgTab) srcTab

                                        -- debug
                                        -- !dummy3 = trace ("t2:\n" ++ show t2) True                                        

                                        t3 = f (not . fpred) trgTab

                                        s1 = aJ (matchPred upsPred) srcTab trgTab 

                                    in insertRTabToRTab s1 $ u t1 $ u t2 t3  
                        else 
                           throw $ ConflictingRTableStructures "Cannot run: Upsert due to conflicting RTable structures." 

    

-- | Compares the structure of the input 'RTable's and returns 'True' if these are the same.
-- By \"structure\", we mean that the 'ColumnName's and the corresponding data types must match. Essentially what we record in the 'ColumnInfo'
-- must be the same for the two 'RTable's.
-- Note that in the case of two columns having the same name but one of the two (or both) have a 'dtype' equal to 'UknownType', then
-- this function assumes that they are the same (i.e., equal 'ColumnInfo's).
rtabsSameStructure :: RTable -> RTable -> Bool
rtabsSameStructure rtab1 rtab2 =
    let 
        cinfo_list1 = getColumnInfoFromRTab rtab1
        cinfo_list2 = getColumnInfoFromRTab rtab2
        -- in the case of two columns with the same name but with one of the two having a Null value,
        -- then the comparison of the column data types will fail, since the column with the Null values
        -- will have an "UknownType" in ColumnDType.
        -- So, we have to normalize the list for this case, so that they pass the equality test
        --
        -- Traverse the list, and if you find a column of UknownType, then make it the same type with 
        -- that of the column with the same name from the other list (if there is such a column)
        cinfo_list1_new = Data.List.map (normalizeColInfoList cinfo_list2) cinfo_list1
        cinfo_list2_new = Data.List.map (normalizeColInfoList cinfo_list1) cinfo_list2
    in  
        -- In order for the two lists to have the same elements (regardless of their order),
        -- then the double minus must result to an empty list.
        ( (cinfo_list1_new \\ cinfo_list2_new) == [] )
                    &&
        ( (cinfo_list2_new \\ cinfo_list1_new) == [] )
    where
        -- Traverse the list, and if you find a column of UknownType, then make it the same type with 
        -- that of the column with the same name from the other list (if there is such a column)        
        normalizeColInfoList :: [ColumnInfo] -> ColumnInfo -> ColumnInfo
        normalizeColInfoList cinfo_list2 ci1 =  --  (\ci1 -> 
            -- check if there is a column in the other list with the same name
            case Data.List.find (\ci2 -> (name ci1) == (name ci2)) cinfo_list2 of
                Nothing -> ci1  -- do nothing
                Just ci ->  -- if the type of the 1st one is Uknown
                            if (dtype ci1) == UknownType
                                then
                                    -- change the type to that of ci2
                                    ColumnInfo {name = (name ci1), dtype = (dtype ci)}
                                else -- do nothing
                                    ci1 
        -- )

-- | Compares the structure of the input 'RTuple's and returns 'True' if these are the same.
-- By \"structure\", we mean that the 'ColumnName's and the corresponding data types must match. Essentially what we record in the 'ColumnInfo'
-- must be the same for the two 'RTuple's
rtuplesSameStructure :: RTuple -> RTuple -> Bool
rtuplesSameStructure t1 t2 =
    let 
        cinfo_list1 = getColumnInfoFromRTuple t1
        cinfo_list2 = getColumnInfoFromRTuple t2
    in  
        -- In order for the two lists to have the same elements (regardless of their order),
        -- then the double minus must result to an empty list.
        ( (cinfo_list1 \\ cinfo_list2) == [] )
                    &&
        ( (cinfo_list2 \\ cinfo_list1) == [] )



-- | Delete 'RTuple's from an 'RTable' based on an 'RPredicate'.
-- Please note that this is an __immutable__ implementation of an 'RTable' update. This simply means that
-- the delete operation returns a new 'RTable'. So, the original 'RTable' remains unchanged and no deletion in-place
-- takes place whatsoever.
-- Moreover, if we have multiple threads deleting an 'RTable', due to immutability, each thread \"sees\" its own copy of
-- the 'RTable' and thus there is no need for locking the deleted 'RTuple's, as happens in a common RDBMS.
deleteRTab ::
        RPredicate  -- ^ Predicate specifying the 'Rtuple's that must be deleted
    ->  RTable      -- ^ 'RTable' that the deletion will be applied
    ->  RTable      -- ^ Result 'RTable'
deleteRTab rpred rtab = f (not . rpred) rtab -- simply omit the rtuples satisfying the predicates    

-- | Update an RTable. The input includes a list of (ColumnName, new Value) pairs.
-- Also a filter predicate is specified, in order to restrict the update only to those
-- 'RTuple's that fulfill the predicate.
-- Please note that this is an __immutable__ implementation of an 'RTable' update. This simply means that
-- the update operation returns a new 'RTable' that includes all the 'RTuple's of the original 'RTable', both the ones
-- that have been updated and the others that have not. So, the original 'RTable' remains unchanged and no update in-place
-- takes place whatsoever.
-- Moreover, if we have multiple threads updating an 'RTable', due to immutability, each thread \"sees\" its own copy of
-- the 'RTable' and thus there is no need for locking the updated 'RTuple's, as happens in a common RDBMS.
updateRTab ::
        [(ColumnName, RDataType)] -- ^ List of column names to be updated with the corresponding new values
    ->  RPredicate  -- ^ An RTuple -> Bool function that specifies the RTuples to be updated
    ->  RTable       -- ^ Input RTable
    ->  RTable       -- ^ Output RTable
updateRTab [] _ inputRtab = inputRtab 
updateRTab ((colName, newVal) : rest) rpred inputRtab = 
    {-  -- READ ME PLEASE --
        Here is the update algorithm:

        FinalRTable = UNION (Table A) (Table B)

        where
            Table A = the subset of input RTable that includes the rtuples that satisfy the input RPredicate, with updated values in the
                        corresponding columns
            Table B = the subset of input RTable that includes all the rtuples that DONT satisfy the input RPredicate
    -}
    if isRTabEmpty inputRtab 
        then emptyRTable
        else
            let 
                tabA = rtabMap (updateRTuple colName newVal) (f rpred inputRtab)
                tabB = f (not . rpred) inputRtab 
            in updateRTab rest rpred (u tabA tabB)

-- | Update an RTuple at a specific column specified by name with a value. If the 'ColumnName'
-- exists, then the value is updated with the input value. If the 'ColumnName' does not exist,
-- then a 'ColumnDoesNotExist' exception is thrown.
updateRTuple ::
           ColumnName  -- ^ key where the update will take place
        -> RDataType   -- ^ new value
        -> RTuple      -- ^ input RTuple
        -> RTuple      -- ^ output RTuple
updateRTuple cname newVal tupsrc = 
    case rtupLookup cname tupsrc of
        Nothing   ->  throw $ ColumnDoesNotExist cname 
        _         ->  HM.insert cname newVal tupsrc
        

-- | Upsert (update/insert) an RTuple at a specific column specified by name with a value
-- If the cname key is not found then the (columnName, value) pair is inserted. If it exists
-- then the value is updated with the input value.
upsertRTuple ::
           ColumnName  -- ^ key where the upsert will take place
        -> RDataType   -- ^ new value
        -> RTuple      -- ^ input RTuple
        -> RTuple      -- ^ output RTuple
upsertRTuple cname newVal tupsrc = HM.insert cname newVal tupsrc

-- * ########## RTable IO Operations ##############

-- | Basic data type for defining the desired formatting of an 'RTuple' when printing an RTable (see 'printfRTable').
data RTupleFormat = RTupleFormat {
    
    colSelectList :: [ColumnName] -- ^ For defining the column ordering (i.e., the SELECT clause in SQL)    
    ,colFormatMap :: ColFormatMap -- ^ For defining the formating per Column in \"'printf' style\"

} deriving (Eq, Show)

-- | A map of ColumnName to Format Specification
type ColFormatMap = HM.HashMap ColumnName FormatSpecifier

-- | Generates a default Column Format Specification
genDefaultColFormatMap :: ColFormatMap
genDefaultColFormatMap = HM.empty

-- | Generates a Column Format Specification
genColFormatMap :: 
    [(ColumnName, FormatSpecifier)]
    -> ColFormatMap
genColFormatMap fs = HM.fromList fs


-- | Format specifier of 'Text.Printf.printf' style
data FormatSpecifier = DefaultFormat | Format String deriving (Eq, Show)

-- | Generate an RTupleFormat data type instance
genRTupleFormat :: 
    [ColumnName]    -- ^ Column Select list 
    -> ColFormatMap -- ^ Column Format Map
    -> RTupleFormat  -- ^ Output
genRTupleFormat colNames colfMap = RTupleFormat { colSelectList = colNames, colFormatMap = colfMap} 

-- | Generate a default RTupleFormat data type instance.
-- In this case the returned column order (Select list), will be unspecified
-- and dependant only by the underlying structure of the 'RTuple' ('HashMap')
genRTupleFormatDefault :: RTupleFormat
genRTupleFormatDefault = RTupleFormat { colSelectList = [], colFormatMap = genDefaultColFormatMap }


-- | Safe 'printRfTable' alternative that returns an 'Either', so as to give the ability to handle exceptions 
-- gracefully, during the evaluation of the input RTable. Example:
--
-- @
-- do 
--  p <- (eitherPrintfRTable printfRTable myFormat myRTab) :: IO (Either SomeException ())
--  case p of
--            Left exc -> putStrLn $ "There was an error in the Julius evaluation: " ++ (show exc)
--            Right _  -> return ()
-- @
--
eitherPrintfRTable :: Exception e => (RTupleFormat -> RTable -> IO()) -> RTupleFormat -> RTable -> IO (Either e ()) 
eitherPrintfRTable printFunc fmt rtab = try $ printFunc fmt rtab

-- | prints an RTable with an RTuple format specification.
-- It can be used instead of 'printRTable' when one of the following two is required:
--
-- * a) When we want to specify the order that the columns will be printed on screen
-- * b) When we want to specify the formatting of the values by using a 'printf'-like 'FormatSpecifier'
--
printfRTable :: RTupleFormat -> RTable -> IO()
printfRTable rtupFmt rtab = -- undefined
    if isRTabEmpty rtab
        then
            do 
                putStrLn "-------------------------------------------"
                putStrLn " 0 rows returned"
                putStrLn "-------------------------------------------"

        else
            do
                -- find the max value-length for each column, this will be the width of the box for this column
                let listOfLengths = getMaxLengthPerColumnFmt rtupFmt rtab

                --debug
                --putStrLn $ "List of Lengths: " ++ show listOfLengths

                printContLineFmt rtupFmt listOfLengths '-' rtab
                -- print the Header
                printRTableHeaderFmt rtupFmt listOfLengths rtab 

                -- print the body
                printRTabBodyFmt rtupFmt listOfLengths $ V.toList rtab

                -- print number of rows returned
                let numrows = V.length rtab
                if numrows == 1
                    then 
                        putStrLn $ "\n"++ (show $ numrows) ++ " row returned"        
                    else
                        putStrLn $ "\n"++ (show $ numrows) ++ " rows returned"        

                printContLineFmt rtupFmt listOfLengths '-' rtab
                where
                    -- [Int] a List of width per column to be used in the box definition        
                    printRTabBodyFmt :: RTupleFormat -> [Int] -> [RTuple] -> IO()
                    printRTabBodyFmt _ _ [] = putStrLn ""            
                    printRTabBodyFmt rtupf ws (rtup : rest) = do
                            printRTupleFmt rtupf ws rtup
                            printRTabBodyFmt rtupf ws rest

-- | Safe 'printRTable' alternative that returns an 'Either', so as to give the ability to handle exceptions 
-- gracefully, during the evaluation of the input RTable. Example:
--
-- @
-- do 
--  p <- (eitherPrintRTable  printRTable myRTab) :: IO (Either SomeException ())
--  case p of
--            Left exc -> putStrLn $ "There was an error in the Julius evaluation: " ++ (show exc)
--            Right _  -> return ()
-- @
--
eitherPrintRTable :: Exception e => (RTable -> IO ()) -> RTable -> IO (Either e ())
eitherPrintRTable printFunc rtab = try $ printFunc rtab 

-- | printRTable : Print the input RTable on screen
printRTable ::
       RTable
    -> IO ()
printRTable rtab = 
    if isRTabEmpty rtab
        then
            do 
                putStrLn "-------------------------------------------"
                putStrLn " 0 rows returned"
                putStrLn "-------------------------------------------"

        else
            do
                -- find the max value-length for each column, this will be the width of the box for this column
                let listOfLengths = getMaxLengthPerColumn rtab

                --debug
                --putStrLn $ "List of Lengths: " ++ show listOfLengths

                printContLine listOfLengths '-' rtab
                -- print the Header
                printRTableHeader listOfLengths rtab 

                -- print the body
                printRTabBody listOfLengths $ V.toList rtab

                -- print number of rows returned
                let numrows = V.length rtab
                if numrows == 1
                    then 
                        putStrLn $ "\n"++ (show $ numrows) ++ " row returned"        
                    else
                        putStrLn $ "\n"++ (show $ numrows) ++ " rows returned"        

                printContLine listOfLengths '-' rtab
                where
                    -- [Int] a List of width per column to be used in the box definition        
                    printRTabBody :: [Int] -> [RTuple] -> IO()
                    printRTabBody _ [] = putStrLn ""            
                    printRTabBody ws (rtup : rest) = do
                            printRTuple ws rtup
                            printRTabBody ws rest

-- | Returns the max length of the String representation of each value, for each column of the input RTable. 
-- It returns the lengths in the column order specified by the input RTupleFormat parameter
getMaxLengthPerColumnFmt :: RTupleFormat -> RTable -> [Int]
getMaxLengthPerColumnFmt rtupFmt rtab = 
    let
        -- Create an RTable where all the values of the columns will be the length of the String representations of the original values
        lengthRTab = do
            rtup <- rtab
            let ls = Data.List.map (\(c, v) -> (c, RInt $ fromIntegral $ Data.List.length . rdataTypeToString $ v) ) (rtupleToList rtup)
                -- create an RTuple with the column names lengths
                headerLengths = Data.List.zip (getColumnNamesFromRTab rtab) (Data.List.map (\c -> RInt $ fromIntegral $ Data.List.length (T.unpack c)) (getColumnNamesFromRTab rtab))
            -- append  to the rtable also the tuple corresponding to the header (i.e., the values will be the names of the column) in order
            -- to count them also in the width calculation
            (return $ createRTuple ls) V.++ (return $ createRTuple headerLengths)

        -- Get the max length for each column
        resultRTab = findMaxLengthperColumn lengthRTab
                where
                    findMaxLengthperColumn :: RTable -> RTable
                    findMaxLengthperColumn rt = 
                        let colNames = (getColumnNamesFromRTab rt) -- [ColumnName]
                            aggOpsList = Data.List.map (\c -> raggMax c c) colNames  -- [AggOp]
                        in runAggregation aggOpsList rt
         -- get the RTuple with the results
        resultRTuple = headRTup resultRTab
    in 
        -- transform it to [Int]
        if rtupFmt /= genRTupleFormatDefault
            then
                -- generate [Int] in the column order specified by the format parameter        
                Data.List.map (\(RInt i) -> fromIntegral i) $ 
                    Data.List.map (\colname -> resultRTuple <!> colname) $ colSelectList rtupFmt  -- [RInt i]
            else
                -- else just choose the default column order (i.e., unspecified)
                Data.List.map (\(colname, RInt i) -> fromIntegral i) (rtupleToList resultRTuple)                

-- | Returns the max length of the String representation of each value, for each column of the input RTable. 
getMaxLengthPerColumn :: RTable -> [Int]
getMaxLengthPerColumn rtab = 
    let
        -- Create an RTable where all the values of the columns will be the length of the String representations of the original values
        lengthRTab = do
            rtup <- rtab
            let ls = Data.List.map (\(c, v) -> (c, RInt $ fromIntegral $ Data.List.length . rdataTypeToString $ v) ) (rtupleToList rtup)
                -- create an RTuple with the column names lengths
                headerLengths = Data.List.zip (getColumnNamesFromRTab rtab) (Data.List.map (\c -> RInt $ fromIntegral $ Data.List.length (T.unpack c)) (getColumnNamesFromRTab rtab))
            -- append  to the rtable also the tuple corresponding to the header (i.e., the values will be the names of the column) in order
            -- to count them also in the width calculation
            (return $ createRTuple ls) V.++ (return $ createRTuple headerLengths)

        -- Get the max length for each column
        resultRTab = findMaxLengthperColumn lengthRTab
                where
                    findMaxLengthperColumn :: RTable -> RTable
                    findMaxLengthperColumn rt = 
                        let colNames = (getColumnNamesFromRTab rt) -- [ColumnName]
                            aggOpsList = Data.List.map (\c -> raggMax c c) colNames  -- [AggOp]
                        in runAggregation aggOpsList rt

{-
        -- create a Julius expression to evaluate the max length per column
        julexpr =  EtlMapStart
                -- Turn each value to an (RInt i) that correposnd to the length of the String representation of the value
                :-> (EtlC $ 
                        Source (getColumnNamesFromRTab rtab)
                        Target (getColumnNamesFromRTab rtab)
                        By (\[value] -> [RInt $ Data.List.length . rdataTypeToString $ value] )
                        (On $ Tab rtab) 
                        RemoveSrc $
                        FilterBy (\rtuple -> True)
                    )
                -- Get the max length for each column
                :-> (EtlR $
                        ROpStart 
                        :. (GenUnaryOp (On Previous) $ ByUnaryOp findMaxLengthperColumn)
                    )
                where
                    findMaxLengthperColumn :: RTable -> RTable
                    findMaxLengthperColumn rt = 
                        let colNames = (getColumnNamesFromRTab rt) -- [ColumnName]
                            aggOpsList = V.map (\c -> raggMax c c) colNames  -- [AggOp]
                        in runAggregation aggOpsList rt

        -- evaluate the xpression and get the result in an RTable
        resultRTab = juliusToRTable julexpr        
-}       
         -- get the RTuple with the results
        resultRTuple = headRTup resultRTab
    in 
        -- transform it to a [Int]
        Data.List.map (\(colname, RInt i) -> fromIntegral i) (rtupleToList resultRTuple)

spaceSeparatorWidth :: Int
spaceSeparatorWidth = 5

-- | helper function in order to format the value of a column
-- It will append at the end of the string n number of spaces.
addSpace :: 
        Int -- ^ number of spaces to add
    ->  String -- ^ input String
    ->  String -- ^ output string
addSpace i s = s ++ Data.List.take i (repeat ' ')    


-- | helper function in order to format the value of a column
-- It will append at the end of the string n number of spaces.
addCharacter :: 
        Int -- ^ number of spaces to add
    ->  Char   -- ^ character to add
    ->  String -- ^ input String
    ->  String -- ^ output string
addCharacter i c s = s ++ Data.List.take i (repeat c)    

-- | helper function that prints a continuous line adjusted to the size of the input RTable
-- The column order is specified by the input RTupleFormat parameter
printContLineFmt ::
       RTupleFormat -- ^ Specifies the appropriate column order
    -> [Int]    -- ^ a List of width per column to be used in the box definition
    -> Char     -- ^ the char with which the line will be drawn
    -> RTable
    -> IO ()
printContLineFmt rtupFmt widths ch rtab = do
    let listOfColNames =    if rtupFmt /= genRTupleFormatDefault
                                then
                                    colSelectList rtupFmt
                                else
                                    getColumnNamesFromRTab rtab -- [ColumnName] 
        listOfLinesCont = Data.List.map (\c -> Data.List.take (Data.List.length (T.unpack c)) (repeat ch)) listOfColNames
        --listOfLinesCont = Data.List.map (\c ->  T.replicate (T.length c) (T.singleton ch)) listOfColNames
        formattedLinesCont = Data.List.map (\(w,l) -> addCharacter (w - (Data.List.length l) + spaceSeparatorWidth) ch l) (Data.List.zip widths listOfLinesCont)
        formattedRowOfLinesCont = Data.List.foldr (\line accum -> line ++ accum) "" formattedLinesCont
    putStrLn formattedRowOfLinesCont


-- | helper function that prints a continuous line adjusted to the size of the input RTable
printContLine ::
       [Int]    -- ^ a List of width per column to be used in the box definition
    -> Char     -- ^ the char with which the line will be drawn
    -> RTable
    -> IO ()
printContLine widths ch rtab = do
    let listOfColNames =  getColumnNamesFromRTab rtab -- [ColumnName] 
        listOfLinesCont = Data.List.map (\c -> Data.List.take (Data.List.length (T.unpack c)) (repeat ch)) listOfColNames
        formattedLinesCont = Data.List.map (\(w,l) -> addCharacter (w - (Data.List.length l) + spaceSeparatorWidth) ch l) (Data.List.zip widths listOfLinesCont)
        formattedRowOfLinesCont = Data.List.foldr (\line accum -> line ++ accum) "" formattedLinesCont
    putStrLn formattedRowOfLinesCont


-- | Prints the input RTable's header (i.e., column names) on screen.
-- The column order is specified by the corresponding RTupleFormat parameter.
printRTableHeaderFmt ::
       RTupleFormat -- ^ Specifies Column order
    -> [Int]    -- ^ a List of width per column to be used in the box definition
    -> RTable
    -> IO ()
printRTableHeaderFmt rtupFmt widths rtab = do -- undefined    
        let listOfColNames = if rtupFmt /= genRTupleFormatDefault then colSelectList rtupFmt else  getColumnNamesFromRTab rtab -- [ColumnName]
            -- format each column name according the input width and return a list of Boxes [Box]
            -- formattedList =  Data.List.map (\(w,c) -> BX.para BX.left (w + spaceSeparatorWidth) c) (Data.List.zip widths listOfColNames)    -- listOfColNames   -- map (\c -> BX.render . BX.text $ c) listOfColNames
            formattedList = Data.List.map (\(w,c) -> addSpace (w - (Data.List.length (T.unpack c)) + spaceSeparatorWidth) (T.unpack c)) (Data.List.zip widths listOfColNames) 
            -- Paste all boxes together horizontally
            -- formattedRow = BX.render $ Data.List.foldr (\colname_box accum -> accum BX.<+> colname_box) BX.nullBox formattedList
            formattedRow = Data.List.foldr (\colname accum -> colname ++ accum) "" formattedList
            
            listOfLines = Data.List.map (\c -> Data.List.take (Data.List.length (T.unpack c)) (repeat '~')) listOfColNames
          --  listOfLinesCont = Data.List.map (\c -> Data.List.take (Data.List.length c) (repeat '-')) listOfColNames
            --formattedLines = Data.List.map (\(w,l) -> BX.para BX.left (w +spaceSeparatorWidth) l) (Data.List.zip widths listOfLines)
            formattedLines = Data.List.map (\(w,l) -> addSpace (w - (Data.List.length l) + spaceSeparatorWidth) l) (Data.List.zip widths listOfLines)
          -- formattedLinesCont = Data.List.map (\(w,l) -> addCharacter (w - (Data.List.length l) + spaceSeparatorWidth) '-' l) (Data.List.zip widths listOfLinesCont)
            -- formattedRowOfLines = BX.render $ Data.List.foldr (\line_box accum -> accum BX.<+> line_box) BX.nullBox formattedLines
            formattedRowOfLines = Data.List.foldr (\line accum -> line ++ accum) "" formattedLines
          ---  formattedRowOfLinesCont = Data.List.foldr (\line accum -> line ++ accum) "" formattedLinesCont

      --  printUnderlines formattedRowOfLinesCont
        printHeader formattedRow
        printUnderlines formattedRowOfLines
        where
            printHeader :: String -> IO()
            printHeader h = putStrLn h

            printUnderlines :: String -> IO()
            printUnderlines l = putStrLn l

-- | printRTableHeader : Prints the input RTable's header (i.e., column names) on screen
printRTableHeader ::
       [Int]    -- ^ a List of width per column to be used in the box definition
    -> RTable
    -> IO ()
printRTableHeader widths rtab = do -- undefined    
        let listOfColNames =  getColumnNamesFromRTab rtab -- [ColumnName]
            -- format each column name according the input width and return a list of Boxes [Box]
            -- formattedList =  Data.List.map (\(w,c) -> BX.para BX.left (w + spaceSeparatorWidth) c) (Data.List.zip widths listOfColNames)    -- listOfColNames   -- map (\c -> BX.render . BX.text $ c) listOfColNames
            formattedList = Data.List.map (\(w,c) -> addSpace (w - (Data.List.length (T.unpack c)) + spaceSeparatorWidth) (T.unpack c)) (Data.List.zip widths listOfColNames) 
            -- Paste all boxes together horizontally
            -- formattedRow = BX.render $ Data.List.foldr (\colname_box accum -> accum BX.<+> colname_box) BX.nullBox formattedList
            formattedRow = Data.List.foldr (\colname accum -> colname ++ accum) "" formattedList
            
            listOfLines = Data.List.map (\c -> Data.List.take (Data.List.length (T.unpack c)) (repeat '~')) listOfColNames
            --listOfLines = Data.List.map (\c -> T.replicate (T.length c) "~") listOfColNames
          --  listOfLinesCont = Data.List.map (\c -> Data.List.take (Data.List.length c) (repeat '-')) listOfColNames
            --formattedLines = Data.List.map (\(w,l) -> BX.para BX.left (w +spaceSeparatorWidth) l) (Data.List.zip widths listOfLines)
            formattedLines = Data.List.map (\(w,l) -> addSpace (w - (Data.List.length l) + spaceSeparatorWidth) l) (Data.List.zip widths listOfLines)
            --formattedLines = Data.List.map (\(w,l) -> T.pack $ addSpace (w - (T.length l) + spaceSeparatorWidth) (T.unpack l)) (Data.List.zip widths listOfLines)
          -- formattedLinesCont = Data.List.map (\(w,l) -> addCharacter (w - (Data.List.length l) + spaceSeparatorWidth) '-' l) (Data.List.zip widths listOfLinesCont)
            -- formattedRowOfLines = BX.render $ Data.List.foldr (\line_box accum -> accum BX.<+> line_box) BX.nullBox formattedLines
            formattedRowOfLines = Data.List.foldr (\line accum -> line ++ accum) "" formattedLines
          ---  formattedRowOfLinesCont = Data.List.foldr (\line accum -> line ++ accum) "" formattedLinesCont

      --  printUnderlines formattedRowOfLinesCont
        printHeader formattedRow
        printUnderlines formattedRowOfLines
        where
            printHeader :: String -> IO()
            printHeader h = putStrLn h

            printUnderlines :: String -> IO()
            printUnderlines l = putStrLn l
{-          printHeader :: [String] -> IO ()
            printHeader [] = putStrLn ""
            printHeader (x:xs) = do
                    putStr $ x ++ "\t"
                    printHeader xs
            
            printUnderlines :: [String] -> IO ()
            printUnderlines [] = putStrLn ""
            printUnderlines (x:xs) = do
                putStr $ (Data.List.take (Data.List.length x) (repeat '~')) ++ "\t" 
                printUnderlines xs
-}

-- | Prints an RTuple on screen (only the values of the columns)
--  [Int] is a List of width per column to be used in the box definition        
-- The column order as well as the formatting specifications are specified by the first parameter.
-- We assume that the order in [Int] corresponds to that of the RTupleFormat parameter.
printRTupleFmt :: RTupleFormat -> [Int] -> RTuple -> IO()
printRTupleFmt rtupFmt widths rtup = do
    -- take list of values of each column and convert to String
    let rtupList =  if rtupFmt == genRTupleFormatDefault
                        then -- then no column ordering is specified nore column formatting
                            Data.List.map (rdataTypeToString . snd) (rtupleToList rtup)  -- [RDataType] --> [String]
                        else 
                            if (colFormatMap rtupFmt) == genDefaultColFormatMap
                                then -- col ordering is specified but no formatting per column
                                    Data.List.map (\colname -> rdataTypeToStringFmt DefaultFormat $ rtup <!> colname ) $ colSelectList rtupFmt
                                else  -- both column ordering, as well as formatting per column is specified
                                    Data.List.map (\colname -> rdataTypeToStringFmt ((colFormatMap rtupFmt) ! colname) $ rtup <!> colname ) $ colSelectList rtupFmt

        -- format each column value according the input width and return a list of [Box]
        -- formattedValueList = Data.List.map (\(w,v) -> BX.para BX.left w v) (Data.List.zip widths rtupList)
        formattedValueList = Data.List.map (\(w,v) -> addSpace (w - (Data.List.length v) + spaceSeparatorWidth) v) (Data.List.zip widths rtupList)
                        -- Data.List.map (\(c,r) -> BX.text . rdataTypeToString $ r) rtupList 
                        -- Data.List.map (\(c,r) -> rdataTypeToString $ r) rtupList --  -- [String]
        -- Paste all boxes together horizontally
        -- formattedRow = BX.render $ Data.List.foldr (\value_box accum -> accum BX.<+> value_box) BX.nullBox formattedValueList
        formattedRow = Data.List.foldr (\value_box accum -> value_box ++ accum) "" formattedValueList
    putStrLn formattedRow

-- | Prints an RTuple on screen (only the values of the columns)
--  [Int] is a List of width per column to be used in the box definition        
printRTuple :: [Int] -> RTuple -> IO()
printRTuple widths rtup = do
    -- take list of values of each column and convert to String
    let rtupList = Data.List.map (rdataTypeToString . snd) (rtupleToList rtup)  -- [RDataType] --> [String]

        -- format each column value according the input width and return a list of [Box]
        -- formattedValueList = Data.List.map (\(w,v) -> BX.para BX.left w v) (Data.List.zip widths rtupList)
        formattedValueList = Data.List.map (\(w,v) -> addSpace (w - (Data.List.length v) + spaceSeparatorWidth) v) (Data.List.zip widths rtupList)
                        -- Data.List.map (\(c,r) -> BX.text . rdataTypeToString $ r) rtupList 
                        -- Data.List.map (\(c,r) -> rdataTypeToString $ r) rtupList --  -- [String]
        -- Paste all boxes together horizontally
        -- formattedRow = BX.render $ Data.List.foldr (\value_box accum -> accum BX.<+> value_box) BX.nullBox formattedValueList
        formattedRow = Data.List.foldr (\value_box accum -> value_box ++ accum) "" formattedValueList
    putStrLn formattedRow

{-    printList formattedValueList
    where 
        printList :: [String] -> IO()
        printList [] = putStrLn ""
        printList (x:xs) = do
            putStr $ x ++ "\t"
            printList xs
-}

-- | Turn the value stored in a RDataType into a String in order to be able to print it wrt to the specified format
rdataTypeToStringFmt :: FormatSpecifier -> RDataType -> String
rdataTypeToStringFmt fmt rdt =
    case fmt of 
        DefaultFormat -> rdataTypeToString rdt
        Format fspec -> 
            case rdt of
                RInt i -> printf fspec i
                RText t -> printf fspec (unpack t)
                RDate {rdate = d, dtformat = f} -> printf fspec (unpack d)
                RTime t -> printf fspec $ unpack $ rtext (rTimestampToRText "DD/MM/YYYY HH24:MI:SS" t)
                -- Round to only two decimal digits after the decimal point
                RDouble db -> printf fspec db -- show db
                Null -> "NULL"           

-- | Turn the value stored in a RDataType into a String in order to be able to print it
-- Values are transformed with a default formatting. 
rdataTypeToString :: RDataType -> String
rdataTypeToString rdt =
    case rdt of
        RInt i -> show i
        RText t -> unpack t
        RDate {rdate = d, dtformat = f} -> unpack d
        RTime t -> unpack $ rtext (rTimestampToRText "DD/MM/YYYY HH24:MI:SS" t)
        -- Round to only two decimal digits after the decimal point
        RDouble db -> printf "%.2f" db -- show db
        Null -> "NULL"



{-

-- | This data type is used in order to be able to print the value of a column of an RTuple
data ColPrint = ColPrint { colName :: String, val :: String } deriving (Data, G.Generic)
instance PP.Tabulate ColPrint

data PrintableRTuple = PrintableRTuple [ColPrint]  deriving (Data, G.Generic)
instance PP.Tabulate PrintableRTuple

instance PP.CellValueFormatter PrintableRTuple where
    -- ppFormatter :: a -> String
    ppFormatter [] = ""
    ppFormatter (colpr:rest)  = (BX.render $ BX.text (val colpr)) ++ "\t" ++ ppFormatter rest

-- | Turn an RTuple to a list of RTuplePrint
rtupToPrintableRTup :: RTuple -> PrintableRTuple
rtupToPrintableRTup rtup = 
    let rtupList = rtupleToList rtup  -- [(ColumnName, RDataType)]
    in PrintableRTuple $ Data.List.map (\(c,r) -> ColPrint { colName = c, val = rdataTypeToString r }) rtupList  -- [ColPrint]

-- | Turn the value stored in a RDataType into a String in order to be able to print it
rdataTypeToString :: RDataType -> String
rdataTypeToString rdt = undefined

-- | printRTable : Print the input RTable on screen
printRTable ::
       RTable
    -> IO ()
printRTable rtab = -- undefined
    do 
        let vectorOfprintableRTups = do 
                                rtup <- rtab
                                let rtupPrint = rtupToPrintableRTup rtup 
                                return rtupPrint
{-

                                let rtupList = rtupleToList rtup  -- [(ColumnName, RDataType)]
                                    colNamesList = Data.List.map (show . fst) rtupList  -- [String]
                                    rdatatypesStringfied = Data.List.map (rdataTypeToString . snd) rtupList  -- [String]
                                    map = Data.Map.fromList $  Data.List.zip colNamesList rdatatypesStringfied -- [(String, String)]                                
                                return colNamesList -- map -}
        PP.ppTable vectorOfprintableRTups

-}

-- #####  Exceptions Definitions

-- | This exception is thrown whenever we try to access a specific column (i.e., 'ColumnName') of an 'RTuple' and the column does not exist.  
data ColumnDoesNotExist = ColumnDoesNotExist ColumnName deriving(Eq,Show)
instance Exception ColumnDoesNotExist

-- | This exception is thrown whenever we provide a Timestamp format with not even  one valid format pattern
data UnsupportedTimeStampFormat = UnsupportedTimeStampFormat String deriving(Eq,Show)
instance Exception UnsupportedTimeStampFormat

-- | Length mismatch between the format 'String' and the input 'String'
-- data RTimestampFormatLengthMismatch = RTimestampFormatLengthMismatch String String deriving(Eq,Show)
-- instance Exception RTimestampFormatLengthMismatch

-- | One (or both) of the input 'String's to function 'toRTimestamp' are empty
data EmptyInputStringsInToRTimestamp = EmptyInputStringsInToRTimestamp String String deriving(Eq, Show)
instance Exception EmptyInputStringsInToRTimestamp

-- | This exception means that we have tried to do some operation between two 'RTables', which requires that
-- the structure of the two is the same. e.g., an @Insert Into <TAB> RTuples@, or a @UNION@ or toher set operations. 
-- By \"structure\", we mean that the 'ColumnName's and the corresponding data types must match. Essentially what we record in the 'ColumnInfo'
-- must be the same for the two 'RTable's
data ConflictingRTableStructures = 
        ConflictingRTableStructures String  -- ^ Error message indicating the operation that failed.
        deriving(Eq, Show)
instance Exception ConflictingRTableStructures

-- | This exception means that we have tried an Upsert operation where the source 'RTable' does not have
-- a unique set of 'Rtuple's if grouped by the columns used in the matching condition.
-- This simply means that we cannot determine which of the dublicate 'RTuple's in the source 'RTable'
-- will overwrite the target 'RTable', when the matching condition is satisfied.
data UniquenessViolationInUpsert =
        UniquenessViolationInUpsert String -- ^ Error message
        deriving(Eq, Show)
instance Exception UniquenessViolationInUpsert