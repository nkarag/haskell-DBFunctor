-- in order to tun
-- stack ghci
-- :l ./src/RTable/example.hs

{-# LANGUAGE OverloadedStrings #-}

import	RTable.Core
import  RTable.Data.CSV     (CSV, readCSV, toRTable, writeCSV, fromRTable)
import  Data.Text as T			(take, pack)

-- This is the input source table metadata
-- It includes the tables stored in an imaginary database
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

-- Result RTable metadata
result_tab_MData :: RTableMData
result_tab_MData = 
    createRTableMData   (   "resultTab"  -- table name
                            ,[  ("OWNER", Varchar)                                      -- Owner of the table
                                ,("TABLE_NAME", Varchar)                                -- Name of the table
                                ,("LAST_ANALYZED", Timestamp "MM/DD/YYYY HH24:MI:SS")   -- Timestamp of the last time the table was analyzed (i.e., gathered statistics) 
                            ]
                        )
                        ["OWNER", "TABLE_NAME"] -- primary key
                        [] -- (alternative) unique keys

main :: IO()
main = do
	 -- read source csv file
	srcCSV <- readCSV "./app/test-data.csv"

	putStrLn "\nHow many rows you want to print from the source table? :\n"
	n <- readLn :: IO Int
	
	-- print source RTable first n rows
	-- RTable A
	printfRTable (  
	                -- this is the equivalent when printing on the screen a list of columns, defined in a SELECT clause in SQL
	                genRTupleFormat ["OWNER", "TABLE_NAME", "TABLESPACE_NAME", "STATUS", "NUM_ROWS", "BLOCKS", "LAST_ANALYZED"] genDefaultColFormatMap
	             ) $ limit n $ toRTable src_DBTab_MData srcCSV 

	putStrLn "\nThese are the tables that start with a \"B\":\n"
	
	-- print RTuples with table_names that start with a 'B'
	-- RTable B
	printfRTable (  
	                -- this is the equivalent when pinting on the screen a list of columns, defined in a SELECT clause in SQL
	                genRTupleFormat ["OWNER", "TABLE_NAME","LAST_ANALYZED"] genDefaultColFormatMap
	             ) $ tabs_start_with_B $ toRTable src_DBTab_MData srcCSV 
	
	putStrLn "\nThese are the tables that were analyzed the same day:\n"

	-- print tables that were analyzed the same day
	-- RTable C = A InnerJoin B
	printfRTable (  
	                -- this is the equivalent when printing on the screen a list of columns, defined in a SELECT clause in SQL
	                genRTupleFormat ["OWNER", "TABLE_NAME", "LAST_ANALYZED", "OWNER_1", "TABLE_NAME_1", "LAST_ANALYZED_1"] genDefaultColFormatMap
	             ) $ ropB	myJoin
							(limit n $ toRTable src_DBTab_MData srcCSV) 
							(tabs_start_with_B $ toRTable src_DBTab_MData srcCSV)

	-- save result of 2nd operation to CSV file
	writeCSV "./app/result-data.csv" $ 
					fromRTable result_tab_MData $ 
						tabs_start_with_B $ 
							toRTable src_DBTab_MData srcCSV 

	where
		-- Return RTuples with table_name starting with a 'B'
		tabs_start_with_B :: RTable -> RTable
		tabs_start_with_B rtab = (ropU myProjection) . (ropU myFilter) $ rtab
			where
				-- Create a Filter Operation to return only RTuples with table_name starting with a 'B'
				myFilter = RFilter (	\t ->	let 
													tbname = case toText (t <!> "TABLE_NAME") of
																Just t -> t
																Nothing -> pack ""
												in (T.take 1 tbname) == (pack "B")
									)
				-- Create a Projection Operation that projects only two columns
				myProjection = RPrj ["OWNER", "TABLE_NAME", "LAST_ANALYZED"]
		
		-- Create an Inner Join for tables analyzed in the same day
		myJoin :: ROperation
		myJoin = RInJoin ( 	\t1 t2 -> 
								let
									RTime {rtime = RTimestampVal {year = y1, month = m1, day = d1, hours24 = hh1, minutes = mm1, seconds = ss1}} = t1<!>"LAST_ANALYZED"
									RTime {rtime = RTimestampVal {year = y2, month = m2, day = d2, hours24 = hh2, minutes = mm2, seconds = ss2}} = t2<!>"LAST_ANALYZED"
								in y1 == y2 && m1 == m2 && d1 == d2
						)
