module Main where

import	RTable.Core 		(RTableMData ,ColumnDType (..) ,createRTableMData, restrictNrows, printfRTable, genRTupleFormat, genDefaultColFormatMap)
import 	RTable.Data.CSV		(CSV, readCSV, writeCSV, csvToRTable, rtableToCSV)
import	Etl.Julius


--  Define Source Schema (i.e., a set of tables)

-- | This is the basic source table
-- It includes the tables of an imaginary database
src_DBTab_MData :: RTableMData
src_DBTab_MData = 
	createRTableMData	(	"sourceTab"  -- table name
							,[	("OWNER", Varchar)  									-- Owner of the table
								,("TABLE_NAME", Varchar) 								-- Name of the table
								,("TABLESPACE_NAME", Varchar)							-- Tablespace name
								,("STATUS",Varchar)										-- Status of the table object (VALID/IVALID)
								,("NUM_ROWS", Integer)									-- Number of rows in the table
								,("BLOCKS", Integer)									-- Number of Blocks allocated for this table
								,("LAST_ANALYZED", Timestamp "MM/DD/YYYY HH24:MI:SS")	-- Timestamp of the last time the table was analyzed (i.e., gathered statistics) 
							]
						)
						["OWNER", "TABLE_NAME"] -- primary key
						[] -- (alternative) unique keys

-- | Define Target Schema (i.e., a set of tables)


main :: IO ()
main = do
	-- read source file
	srcCSV <- readCSV "./app/test-data.csv"

	let 
		-- create source RTable 
		src_DBTab = csvToRTable src_DBTab_MData srcCSV
		-- get only the first 100 rows
		src_DBTab_10rows = restrictNrows 10 src_DBTab

	putStrLn "\nThis is the source table:\n"
	-- print source RTable first 100 rows
	printfRTable ( 	
					-- this is the equivalent when pinting on the screen to a list of columns in a SELECT clause in SQL
					genRTupleFormat ["OWNER", "TABLE_NAME", "TABLESPACE_NAME", "STATUS", "NUM_ROWS", "BLOCKS", "LAST_ANALYZED"] genDefaultColFormatMap
				 ) $ src_DBTab_10rows


	-- Do ETL and get result

	-- print result on screen

	-- save result into files
