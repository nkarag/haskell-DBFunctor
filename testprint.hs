{-#  LANGUAGE 
     OverloadedStrings 
    ,BangPatterns
   -- ,ScopedTypeVariables
#-}

import RTable.Core

tab1 = rtableFromList [	 rtupleFromList [("ColInteger", RInt 1), ("ColDouble", RDouble 2.3), ("ColText", RText "We dig dig dig dig dig dig dig")]
							,rtupleFromList [("ColInteger", RInt 2), ("ColDouble", RDouble 5.36879), ("ColText", RText "From early morn to night")]
							,rtupleFromList [("ColInteger", RInt 3), ("ColDouble", RDouble 999.9999), ("ColText", RText "In a mine the whole day through")]
							,rtupleFromList [("ColInteger", RInt 4), ("ColDouble", RDouble 0.9999), ("ColText", RText "Is what we like to do")]
					  ]