{-# LANGUAGE OverloadedStrings #-}

module Main where

-- import  RTable.Core         (RTableMData ,ColumnDType (..) ,createRTableMData, restrictNrows, printfRTable, genRTupleFormat, genDefaultColFormatMap, toText, (<!>))
import  RTable.Data.CSV     (readCSV, toRTable) -- MyType (..) )
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
        --t = toRTable src_DBTab_MData (MyType (4::Int))
        
        -- create source RTable from source csv 
        src_DBTab = toRTable src_DBTab_MData srcCSV      
        -- get only the first 10 rows
        src_DBTab_10rows = limit 10 src_DBTab
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

    putStrLn "\nThese are the fisrt 10 rows of the source table:\n"
    -- print source RTable first 100 rows
    printfRTable (  
                    -- this is the equivalent when printing on the screen to a list of columns in a SELECT clause in SQL
                    genRTupleFormat ["OWNER", "TABLE_NAME", "TABLESPACE_NAME", "STATUS", "NUM_ROWS", "BLOCKS", "LAST_ANALYZED"] genDefaultColFormatMap
                 ) $ src_DBTab_10rows



    putStrLn "\nThese are the tables that start with a \"B\":\n"
    -- print RTuples with table_names that start with a 'B'
    printfRTable (  
                    -- this is the equivalent when printing on the screen to a list of columns in a SELECT clause in SQL
                    genRTupleFormat ["OWNER", "TABLE_NAME"] genDefaultColFormatMap
                 ) $ tabs_with_B


    -- Do ETL and get result

    -- print result on screen

    -- save result into files
