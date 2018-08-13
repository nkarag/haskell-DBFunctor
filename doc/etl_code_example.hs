import Etl.Julius

etl :: [RTable] -> [RTable]
etl [srcTab1, srcTab2] = 
	let
		trgTab1 = juliusToRTable $ julius1 srcTab1
		trgTab2 = juliusToRTable $ julius2 trgTab1 srcTab2
		trgTab3 = juliusToRTable $ julius3 trgTab1 trgTab2 srcTab1
	in [trgTab1,trgTab2,trgTab3]
	where
		julius1 :: RTable -> ETLMappingExpr
		julius1 = undefined

		julius2 :: RTable -> RTable -> ETLMappingExpr
		julius2 = undefined

		julius3 :: RTable -> RTable -> RTable -> ETLMappingExpr
		julius3 = undefined
