-- in order to tun
-- stack ghci
-- :l ./src/Etl/example1.hs

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}

import	Etl.Julius
import  RTable.Core				(limit, emptyRTable, ColumnDoesNotExist)
import  RTable.Data.CSV     	(CSV, readCSV, toRTable, writeCSV, fromRTable)
import  Data.Text as T			(take, pack)
import 	Control.Exception
import  Control.DeepSeq			
import  Debug.Trace				(trace)

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

data MyJulEvalException = 
	MyJulEvalException 
	deriving (Eq, Show)

instance Exception MyJulEvalException	

main :: IO()
main = do
	 -- read source csv file
	srcCSV <- readCSV "./app/test-data.csv"

	putStrLn "\nHow many rows you want to print from the source table? :\n"
	n <- readLn :: IO Int
	

	-- print source RTable first n rows
	-- RTable A
	result1RTab <- runJulius $ julius1 n $ toRTable src_DBTab_MData srcCSV 
	printfRTable (  
	                -- this is the equivalent when printing on the screen a list of columns, defined in a SELECT clause in SQL
	                genRTupleFormat ["OWNER", "TABLE_NAME", "TABLESPACE_NAME", "STATUS", "NUM_ROWS", "BLOCKS", "LAST_ANALYZED"] genDefaultColFormatMap
	             ) $ result1RTab  

	putStrLn "\nThese are the tables that start with a \"B\":\n"
	

	-- print RTuples with table_names that start with a 'B'
	-- RTable B
	-- result2RTab <- runJulius $ julius2 "B" $ toRTable src_DBTab_MData srcCSV 
	
{-	result2RTab <-	catch 	(runJulius $ julius2 "B" $ toRTable src_DBTab_MData srcCSV)
							(\e -> do 
									putStrLn $ "there was an error in Julius evaluation: " ++ (show (e::SomeException))
									return emptyRTable
							)-}


	eitherResult2 <- -- trace "eval 2" $ 
					(eitherRunJulius $ julius2 "B" $ toRTable src_DBTab_MData srcCSV) :: IO (Either SomeException RTable) 
	result2RTab  <- case eitherResult2 of
							Right resRTab2 	-> return resRTab2
							Left exc2		->  do 
													putStrLn $ "there was an error in Julius evaluation: " ++ (show exc2)
													return emptyRTable


	printfRTable (  
	                -- this is the equivalent when pinting on the screen a list of columns, defined in a SELECT clause in SQL
	                genRTupleFormat ["OWNER", "TABLE_NAME","LAST_ANALYZED"] genDefaultColFormatMap
	             ) $ result2RTab 

{-	printResult <-	(eitherPrintfRTable	printfRTable
										(genRTupleFormat ["OWNER", "TABLE_NAME","LAST_ANALYZED"] genDefaultColFormatMap)
										-- result2RTab
										$ juliusToRTable $ julius2 "B" $ toRTable src_DBTab_MData srcCSV								 
		) :: IO (Either SomeException ())
	case printResult of
	       Left exc -> putStrLn $ "There was an error in the evaluation of the input RTable: " ++ (show exc)
	       Right _ -> return ()-}

{-	catch 	(
				printfRTable (  
				                -- this is the equivalent when pinting on the screen a list of columns, defined in a SELECT clause in SQL
				                genRTupleFormat ["OWNER", "TABLE_NAME","LAST_ANALYZED"] genDefaultColFormatMap
				             ) $ result2RTab 
			)
			(\e -> do 
				putStrLn $ "there was an error in Julius evaluation: " ++ (show (e::SomeException))
			)

-}	
	putStrLn "\nThese are the tables that were analyzed the same day:\n"

	-- print tables that were analyzed the same day
	-- RTable C = A InnerJoin B
	eitherResult3 <- -- trace "eval 3" $ 
					(eitherRunJulius $ julius3 result1RTab result2RTab) :: IO (Either SomeException RTable)
	result3RTab <- case eitherResult3  of
						Right resRTab3 	-> return resRTab3
						Left exc3		->	do
												putStrLn $ "there was yet another error in Julius evaluation: " ++ (show exc3)
												return result1RTab -- emptyRTable


	--result3RTab <- runJulius $ julius3 result1RTab result2RTab


	printfRTable (  
	                -- this is the equivalent when printing on the screen a list of columns, defined in a SELECT clause in SQL
	                genRTupleFormat ["OWNER", "TABLE_NAME", "LAST_ANALYZED", "OWNER_1", "TABLE_NAME_1", "LAST_ANALYZED_1"] genDefaultColFormatMap
	             ) $ result3RTab


{-
	catch 	(
				printfRTable (  
				                -- this is the equivalent when printing on the screen a list of columns, defined in a SELECT clause in SQL
				                genRTupleFormat ["OWNER", "TABLE_NAME", "LAST_ANALYZED", "OWNER_1", "TABLE_NAME_1", "LAST_ANALYZED_1"] genDefaultColFormatMap
				             ) $ result3RTab
			)
			(\e -> do 
				putStrLn $ "there was an error in Julius evaluation: " ++ (show (e::SomeException))
			)

-}
	-- save result of 2nd operation to CSV file
	writeCSV "./app/result-data.csv" $ 
					fromRTable result_tab_MData result2RTab
	where
		julius1 n rtab = 
			EtlMapStart 
				:-> (EtlR $
						ROpStart
						:. 	(GenUnaryOp (On $ Tab rtab) $
								ByUnaryOp $ limit n
							)
					)

		julius2 prefix rtab = 
			EtlMapStart
				:-> (EtlR $
						ROpStart
						:.	(Filter (From $ Tab rtab) $
								FilterBy 	(
												\t ->	let 
															tbname = case toText (t <!> "TABLE_NAME") of
																		Just t -> t
																		Nothing -> pack ""
														in (T.take 1 tbname) == (pack prefix)
											)

							)
					)

		julius3 rtab1 rtab2 = 
			EtlMapStart
				:-> (EtlR $
						ROpStart
						:.	(Join (TabL rtab1) (Tab rtab2) $
								JoinOn (	\t1 t2 -> 
												let
													RTime {rtime = RTimestampVal {year = y1, month = m1, day = d1, hours24 = hh1, minutes = mm1, seconds = ss1}} = t1<!>"LAST_ANALYZED"
													RTime {rtime = RTimestampVal {year = y2, month = m2, day = d2, hours24 = hh2, minutes = mm2, seconds = ss2}} = t2<!>"LAST_ANALYZED"
												in y1 == y2 && m1 == m2 && d1 == d2									
									)
							)
					)
