{-
	This example demonstrates the use of Julius expression for calculating an accumulating total per month
-}

-- in order to tun
-- stack ghci
-- :l ./src/Etl/example2.hs


{-# LANGUAGE OverloadedStrings #-}

import Etl.Julius
import Data.Text 	(pack)

myetl :: [RTable] -> [RTable]
myetl = undefined
	-- tab1 = create new rtable with additional column "AccumAmount" initialized with 0.0 and ordered by month

	-- tab2 = fold over tab1 and at each iteration find current max and update accumulator rtab

main :: IO()
main = do 
	-- 1. read source RTables
	-- create an 12-RTuple RTable. Each Rtuple corresponds to a monthly amount
	let src = rtableFromList $ 
				[createRtuple [("Month", RText m),("Amount", RDouble a)] | (m,a) <- map (pack.show) [201801..201812] `zip` [50.0,55.5..110.5]]

	-- check out our source RTable
	printRTable src

	-- 2. run etl
	--[trg] <- runETL myetl [src]

	-- 3. print target
	--printRTable trg


