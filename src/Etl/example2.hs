{-
	This example demonstrates the use of Julius expression for calculating an accumulating total per month
-}

-- in order to tun
-- stack ghci
-- :l ./src/Etl/example2.hs


{-# LANGUAGE OverloadedStrings #-}
-- {-# LANGUAGE BangPatterns #-}

import Etl.Julius
import Data.Text 	(pack)

myetl :: [RTable] -> [RTable]
myetl [src] = 
	let
		-- tab1 = create new rtable with additional column "AccumAmount" initialized with 0.0 and ordered by month
		tab1 = juliusToRTable $ jul1 src
		-- tab2 = find sum from starting RTuple till current RTuple (i.e., window that our agg function is applied) - start with tab1 as accumulator tab
		trg = juliusToRTable $ jul2 tab1
	in [trg]
	where
		jul1 s =	EtlMapStart
					:-> (EtlC $
							Source [] $
							Target ["AccumAmount"] $
							By (\_ -> [RDouble 0.0]) (On $ Tab s) DontRemoveSrc $
							FilterBy (\_ -> True)
						) 
					:-> (EtlR $
							ROpStart
							:.(OrderBy [("Month", Asc)] $ 
								From Previous 
							)
						)
		jul2 initTab = 	EtlMapStart
						:-> (EtlR $
								ROpStart
								-- :.(GenUnaryOp (On $ Tab src) $
								:.(GenUnaryOp (On $ Tab initTab) $
									ByUnaryOp $ rtabFoldl' accumFunc initTab 
								)
								:.(OrderBy [("Month", Asc)] $
									From Previous
								)
							)
		accumFunc =	\accTab rtup ->
			let
				-- find sum from starting RTuple till current RTuple (i.e., window that our agg function is applied)
				runningSum = (headRTup $ currAggTab) <!> "RunningSum"
				currAggTab = juliusToRTable $
								EtlMapStart 
								:-> (EtlR $
										ROpStart
										-- filter rtuples to get current window of rtuples that sum will be applied
										:.	(Filter (From $ Tab accTab) $
												FilterBy (\t -> t <!> "Month" <= rtup <!> "Month")
										)
										-- apply sum on window of rtuples
										:.	(Agg $
												AggOn [Sum "Amount" $ As "RunningSum"] $ From Previous
										)
									){-
				-- find the current maximum accumulated amount in the accTab
				currMax = (headRTup $ maxTab) <!> "MaxAmount"
				maxTab = juliusToRTable $ 
							EtlMapStart 
							:-> (EtlR $
									ROpStart
									:.(Agg $
										AggOn [Max "AccumAmount" $ As "MaxAmount"] $
										From $ Tab accTab
									)
								)
			in updateRTab	[("AccumAmount", currMax + rtup <!> "Amount")] -- update specific month RTuple with accumulated amount
							(\t -> t <!> "Month" == rtup <!> "Month")
							accTab-}

			in updateRTab	[("AccumAmount", runningSum)] -- update specific month RTuple with accumulated amount
							(\t -> t <!> "Month" == rtup <!> "Month")
							accTab

main :: IO()
main = do 
	-- 1. read source RTables
	-- create an 12-RTuple RTable. Each Rtuple corresponds to a monthly amount
	let src = rtableFromList $ 
				[createRtuple [("Month", RText m),("Amount", RDouble a)] | (m,a) <- map (pack.show) [201801..201812] `zip` [50.0,55.5..110.5]]

	-- check out our source RTable
	printRTable src

	-- 2. run etl
	[trg] <- runETL myetl [src]

	-- 3. print target
	printRTable trg

	-- 4. print formatted
	printfRTable 	(genRTupleFormat 
						["Month", "Amount", "AccumAmount"] $ -- specify column printing order
						genColFormatMap [("Month", Format "< %.4s >"), ("Amount", Format "%-20.1e"), ("AccumAmount", Format "%.4f")]
					) 
					trg


