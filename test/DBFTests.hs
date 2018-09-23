{-
main :: IO ()
main = putStrLn "Test suite not yet implemented"
-}

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ViewPatterns #-}  
--{-# LANGUAGE BangPatterns #-}

module Main where

import qualified RTable.Data.CSV as C
import qualified RTable.Core as T
import qualified Etl.Internal.Core as E
import Etl.Julius

--import RTable
--import QProcessor
--import System.IO
import System.Environment (getArgs, setEnv)
import Data.Char (toLower)
import Data.HashMap.Strict ((!))
-- Data.Maybe
import Data.Maybe (fromJust)
-- Text 
import Data.Text (Text,stripSuffix, pack, unpack, append,take, takeEnd)
-- Vector
import Data.Vector (toList)
-- List
import Data.List (groupBy)


-- Test command:  stack exec -- csvdb ./misc/test.csv 20 ./misc/testo.csv
-- or, if you want to test with non-default csv options: 
-- stack exec -- csvdb ./misc/test_options.csv 20 ./misc/testo.csv

main :: IO ()
main = do
    --setEnv "NLS_LANG" "Greek_Greece.UTF8"  

    {-args <- getArgs
    let fi = args!!0
        n = args!!1
        fo = args!!2-}
    let  fi = "./test/data/test_options.csv"
         n = "20"
         fo = "./test/data/testo.csv"

    --csv <- C.readCSVFile fi
    
    --csv <- C.readCSV fi
    let myoptions = C.CSVOptions { C.delimiter = ';', C.hasHeader = C.Yes}
    csv <- C.readCSVwithOptions myoptions fi
    
    
    -- debug
    {-- 
    C.writeCSV fo csv
    C.printCSV csv
    --}
    
    --let newcsv = selectNrows (read n) csv
    let rtmdata = T.createRTableMData ( "TestTable", 
                                        [   ("Name", T.Varchar),
                                            ("MyDate", T.Date "DD/MM/YYYY"), 
                                            ("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("Number", T.Integer), 
                                            ("DNumber", T.Double)
                                        ]
                                      ) 
                                        []  -- primary key
                                        []  -- list of unique keys
        -- *** test CSV to RTable conversion                                        
        rtab = C.toRTable rtmdata csv
        rtabNew = T.limit (read n) rtab
        csvNew = C.fromRTable rtmdata rtabNew   

        -- *** test RFilter & RProjection operation
        rtabNew2 =  let
                       myfilter = T.f (\t -> t!"Name" == T.RText {T.rtext = "Karagiannidis"})
                       myprojection = T.p ["Name","MyTime","Number"]
                       myfilter2 = T.f (\t -> t!"Number" > T.RInt {T.rint = 30 })
                       myprojection2 = T.p ["Name","Number"]
                    in myprojection2.myfilter2.myprojection.myfilter $ rtab     -- ***** THIS IS AN ETL PIPELINE IMPLEMENTED AS FUNCTION COMPOSITION !!!! *****                    
        rtmdata2 = T.createRTableMData ( "TestTable2", 
                                        [   ("Name", T.Varchar),
                                            --("MyDate", T.Date "DD/MM/YYYY"), 
                                           -- ("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("Number", T.Integer) 
                                            --("DNumber", T.Double)
                                        ]
                                      ) 
                                        []  -- primary key
                                        []  -- list of unique keys
        csvNew2 = C.fromRTable rtmdata2 rtabNew2  

        -- Test Julius
        rtabNew2_J = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlR $
                    ROpStart
                    :.  (Filter (From $ Tab rtab) $ FilterBy (\t -> t!"Name" == T.RText {T.rtext = "Karagiannidis"}))
                    :. (Select ["Name","MyTime","Number"] $ From Previous)
                    :. (Filter (From Previous) $ FilterBy (\t -> t!"Number" > T.RInt {T.rint = 30 }))
                    :.  (Select ["Name","Number"] $ From Previous)
                )

{-        rtabNew2_J = E.etl $ evalJulius $
                (EtlR $ 
                       (Select ["Name","Number"] $ From Previous)
                    :. (Filter (From Previous) $ FilterBy (\t -> t!"Number" > T.RInt {T.rint = 30 }))
                    :. (Select ["Name","MyTime","Number"] $ From Previous)
                    :. (Filter (From $ Tab rtab) $ FilterBy (\t -> t!"Name" == T.RText {T.rtext = "Karagiannidis"}) )
                    :. ROpEmpty)
            :-> EtlMapEmpty
-}
        csvNew2_J = C.fromRTable rtmdata2 rtabNew2_J  


    --print / write to file
    C.writeCSV fo csvNew
    C.printCSV csvNew      
    T.printRTable rtabNew  
    let foName2 = (fromJust (stripSuffix ".csv"  (pack fo))) `mappend` "_t2.csv"
    C.writeCSV (unpack foName2) csvNew2            
    C.printCSV csvNew2
    let foName2_J = (fromJust (stripSuffix ".csv"  (pack fo))) `mappend` "_t2_J.csv"
    C.writeCSV (unpack foName2_J) csvNew2_J
    C.printCSV csvNew2_J
    T.printRTable rtabNew2_J

    
    -- *** test Column Mapping
    -- create a new column holding the doubled value from the source column
    let cmap1 = E.RMap1x1 {E.srcCol = "Number", E.removeSrcCol = E.No, E.trgCol = "NewNumber", E.transform1x1 = \x -> 2*x, E.srcRTupleFilter = \_ -> True}
        rtabNew3 = E.runCM cmap1 rtabNew2
        rtmdata3 = T.createRTableMData ( "TestTable3", 
                                         [   ("Name", T.Varchar)
                                            ,("Number", T.Integer)
                                            ,("NewNumber", T.Integer) 
                                         ]
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys

    -- Test Julius
        rtabNew3_J = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlC $ 
                        Source ["Number"] $
                        Target ["NewNumber"] $
                        By (\[x] -> [2*x]) (On $ Tab rtabNew2)
                        DontRemoveSrc $
                        FilterBy (\_ -> True) )

{-        rtabNew3_J = E.etl $ evalJulius $
                    (EtlC $ 
                        Source ["Number"] $
                        Target ["NewNumber"] $
                        By (\[x] -> [2*x]) (On $ Tab rtabNew2)
                        DontRemoveSrc $
                        FilterBy (\_ -> True) )
                :-> EtlMapEmpty
-}
        
    writeResult fo "_t3.csv" rtmdata3 rtabNew3 
    writeResult fo "_t3_J.csv" rtmdata3 rtabNew3_J 
    T.printRTable rtabNew3_J

    --     foName3 = (fromJust (stripSuffix ".csv"  (pack fo))) `mappend` "_t3.csv"
    --     csvNew3 = C.fromRTable rtmdata3 rtabNew3
    -- C.writeCSV (unpack foName3) csvNew3            
    -- C.printCSV csvNew3

    -- *** test inner join operation
    let rtabNew4 = T.iJ (\t1 t2 -> t1!"Number" == t2!"Number") rtabNew3 rtab 
        rtmdata4 = T.createRTableMData ( "TestTable4", 
                                         [  ("Name", T.Varchar),
                                            ("MyDate", T.Date "DD/MM/YYYY"), 
                                            ("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("Number", T.Integer), 
                                            ("DNumber", T.Double),
                                            ("NewNumber", T.Integer)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys
    -- Test Julius
        rtabNew4_J = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlR $
                    ROpStart
                    :.  (Join (TabL rtabNew3) (Tab rtab) $ JoinOn (\t1 t2 -> t1!"Number" == t2!"Number"))
                )

{-        rtabNew4_J = E.etl $ evalJulius $
                (EtlR $ 
                        (Join (TabL rtabNew3) (Tab rtab) $ JoinOn (\t1 t2 -> t1!"Number" == t2!"Number"))
                    :.  ROpEmpty)
            :-> EtlMapEmpty
-}
        
    writeResult fo "_t4.csv" rtmdata4 rtabNew4
    writeResult fo "_t4_J.csv" rtmdata4 rtabNew4_J
    putStrLn "TEST INNER JOIN"
    T.printRTable rtabNew4_J  

    -- *** Test union, interesection, diff
    let -- change the value in column NewNumber
        cmap2 = E.RMap1x1 {E.srcCol = "NewNumber", E.removeSrcCol = E.No, E.trgCol = "NewNumber", E.transform1x1 = \x -> x + 100, E.srcRTupleFilter = \_ -> True}
        -- now union the two rtables
        rtabNew5 = rtabNew4 `T.u` (E.runCM cmap2 rtabNew4)
        rtmdata5 = T.createRTableMData ( "TestTable5", 
                                         [  ("Name", T.Varchar),
                                            ("MyDate", T.Date "DD/MM/YYYY"), 
                                            ("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("Number", T.Integer), 
                                            ("DNumber", T.Double),
                                            ("NewNumber", T.Integer)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys

        rtabNew6 = rtabNew5 `T.i` rtabNew4
        rtabNew7 =  rtabNew5 `T.d` rtabNew6

    -- Test Julius
        rtabNew7_J = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlC $ 
                    Source ["NewNumber"] $
                    Target ["NewNumber"] $
                    By (\[x] -> [x + 100]) (On $ Tab rtabNew4)
                    DontRemoveSrc $
                    FilterBy (\_ -> True))                
            :-> (EtlR $  -- rtabNew5
                    ROpStart
                    :.  (Union (TabL rtabNew4) (Previous))
                )
            :-> (EtlR $ -- rtabNew6
                    ROpStart
                    :.  (Intersect (TabL rtabNew4) (Previous))
                )
            :-> (EtlR $
                    ROpStart
                    :.  (Minus 
                            (TabL $ -- this is the rtabNew5 repeated
                                E.etl $ evalJulius $
                                            EtlMapStart
                                            :-> (EtlC $ 
                                                    Source ["NewNumber"] $
                                                    Target ["NewNumber"] $
                                                    By (\[x] -> [x + 100]) (On $ Tab rtabNew4)
                                                    DontRemoveSrc $
                                                    FilterBy (\_ -> True))                
                                            :-> (EtlR $  -- rtabNew5
                                                    ROpStart
                                                    :.  (Union (TabL rtabNew4) (Previous))
                                                )                                
                            ) 
                            (Previous))
                )


        rtabNew7_J2 = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlR $  -- rtabNew5
                    ROpStart
                    :.  (Minus (TabL rtabNew5) (Tab rtabNew6))
                )


        rtabNew7_J3 = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlR $  -- rtabNew5
                    ROpStart
                    :.  (MinusP (Tab rtabNew5) (TabL rtabNew6))
                )

-- test Intermediate named results
        rtabNew7a_J = E.etl $ evalJulius $ etlXpression

        etlXpression = 
            EtlMapStart
            :-> (EtlC $ 
                    Source ["NewNumber"] $
                    Target ["NewNumber"] $
                    By (\[x] -> [x + 100]) (On $ Tab rtabNew4)
                    DontRemoveSrc $
                    FilterBy (\_ -> True))                
            :=> NamedResult "rtabNew5" (EtlR $  -- rtabNew5
                    ROpStart
                    :.  (Union (TabL rtabNew4) (Previous))
                )
            :-> (EtlR $ -- rtabNew6
                    ROpStart
                    :.  (Intersect (TabL rtabNew4) (Previous))
                )
            :-> (EtlR $
                    ROpStart
                    :.  (Minus 
                            (TabL $ -- this is the rtabNew5 repeated
                                juliusToRTable $ takeNamedResult "rtabNew5" etlXpression    ---  THIS IS THE POINT TO BE TESTED!!!
                            ) 
                            (Previous))
                )

    writeResult fo "_t7a_J.csv" rtmdata5 rtabNew7a_J
    T.printRTable rtabNew7a_J


{-        rtabNew5_J = E.etl $ evalJulius $
                (EtlR $  -- rtabNew5
                        (Union (TabL rtabNew4) (Previous))
                    :.  ROpEmpty)
            :-> (EtlC $ 
                    Source ["NewNumber"] $
                    Target ["NewNumber"] $
                    By (\[x] -> [x + 100]) (On $ Tab rtabNew4)
                    DontRemoveSrc $
                    FilterBy (\_ -> True))                                
            :-> EtlMapEmpty

        rtabNew6_J = E.etl $ evalJulius $
                (EtlR $ -- rtabNew6
                        (Intersect (TabL rtabNew4) (Tab rtabNew5))
                    :.  ROpEmpty)
            :-> EtlMapEmpty

        -- Build rtabNew7 with a single ETL Mapping
        rtabNew7_J = E.etl $ evalJulius $
                (EtlR $
                        (Minus (TabL rtabNew4) (Previous))
                    :.  ROpEmpty)
            :-> (EtlR $ -- rtabNew6
                        (Intersect (TabL rtabNew4) (Previous))
                    :.  ROpEmpty)
            :-> (EtlR $  -- rtabNew5
                        (Union (TabL rtabNew4) (Previous))
                    :.  ROpEmpty)
            :-> (EtlC $ 
                    Source ["NewNumber"] $
                    Target ["NewNumber"] $
                    By (\[x] -> [x + 100]) (On $ Tab rtabNew4)
                    DontRemoveSrc $
                    FilterBy (\_ -> True))                
            :-> EtlMapEmpty
-}

    ---- DEBUG
    -- putStrLn   "------rtabNew3-------" 
    -- print rtabNew3
    -- putStrLn   "------rtab-------" 
    -- print rtab
    -- putStrLn   "------rtabNew4-------"     
    -- print rtabNew4
    -- putStrLn   "------rtmdata5-------" 
    -- print rtmdata5
    -- putStrLn   "------rtabNew5-------" 
    -- print rtabNew5
    ----

    writeResult fo "_t5.csv" rtmdata5 rtabNew5
    --writeResult fo "_t5_J.csv" rtmdata5 rtabNew5_J
    writeResult fo "_t6.csv" rtmdata5 rtabNew6
    --writeResult fo "_t6_J.csv" rtmdata5 rtabNew6_J
    writeResult fo "_t7.csv" rtmdata5 rtabNew7
    writeResult fo "_t7_J.csv" rtmdata5 rtabNew7_J
    writeResult fo "_t7_J2.csv" rtmdata5 rtabNew7_J2
    writeResult fo "_t7_J3.csv" rtmdata5 rtabNew7_J3

    -- *** Change existing column name 
    let -- change the value in column NewNumber
        cmap3 = E.RMap1x1 {E.srcCol = "NewNumber", E.removeSrcCol = E.Yes, E.trgCol = "NewNewNumber", E.transform1x1 = \x -> x, E.srcRTupleFilter = \_ -> True}
        rtabNew8 = E.runCM cmap3 rtabNew5
        rtmdata8 = T.createRTableMData ( "TestTable8", 
                                         [  ("Name", T.Varchar),
                                            ("MyDate", T.Date "DD/MM/YYYY"), 
                                            ("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("Number", T.Integer), 
                                            ("DNumber", T.Double),
                                            ("NewNewNumber", T.Integer)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys

    -- Test Julius
        rtabNew8_J = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlC $
                    Source ["NewNumber"] $
                    Target ["NewNewNumber"] $
                    By (\[x] -> [x]) (On $ Tab rtabNew5)
                    RemoveSrc $
                    FilterBy (\_ -> True)
                )


    writeResult fo "_t8.csv" rtmdata8 rtabNew8                                       
    writeResult fo "_t8_J.csv" rtmdata8 rtabNew8_J
    T.printRTable rtabNew8_J

    -- Test a RMapNx1 column mapping
    let 
        cmap4 = E.RMapNx1 {E.srcColGrp = ["Name","MyTime"], E.removeSrcCol = E.Yes, E.trgCol = "New_Nx1_Col", E.transformNx1 = (\[n, T.RTime{T.rtime = t}] -> n `rdtappend` (T.RText "<---->") `rdtappend` (T.rTimestampToRText T.stdTimestampFormat t)), E.srcRTupleFilter = \_ -> True}
        rtabNew9 = E.runCM cmap4 rtabNew8
        rtmdata9 = T.createRTableMData ( "TestTable9", 
                                         [ -- ("Name", T.Varchar),
                                            ("New_Nx1_Col", T.Varchar),
                                            ("MyDate", T.Date "DD/MM/YYYY"), 
                                           -- ("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("Number", T.Integer), 
                                            ("DNumber", T.Double),
                                            ("NewNewNumber", T.Integer)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys

    -- Test Julius
        rtabNew9_J = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlC $
                    Source ["Name","MyTime"] $
                    Target ["New_Nx1_Col"] $
                    By (\[n, T.RTime{T.rtime = t}] -> [n `rdtappend` (T.RText "<---->") `rdtappend` (T.rTimestampToRText T.stdTimestampFormat t)]) (On $ Tab rtabNew8)
                    RemoveSrc $
                    FilterBy (\_ -> True)
                )

    writeResult fo "_t9.csv" rtmdata9 rtabNew9    
    writeResult fo "_t9_J.csv" rtmdata9 rtabNew9_J    
    T.printRTable rtabNew9_J    

 -- Test a RMap1xN column mapping
    let 
        cmap5 = E.RMap1xN {E.srcCol = "New_Nx1_Col", E.removeSrcCol = E.No, E.trgColGrp = ["1xN_A","1xN_B","1xN_C"], E.transform1xN = (\(T.RText txt) -> [T.RText (Data.Text.take 5 txt), T.RText "<-|-|-|->", T.RText (Data.Text.takeEnd 10 txt)]), E.srcRTupleFilter = \_ -> True}
        rtabNew10 = E.runCM cmap5 rtabNew9
        rtmdata10 = T.createRTableMData ( "TestTable10", 
                                         [ -- ("Name", T.Varchar),
                                            ("New_Nx1_Col", T.Varchar)
                                            ,("MyDate", T.Date "DD/MM/YYYY") 
                                           -- ,("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS")
                                            ,("Number", T.Integer)
                                            ,("DNumber", T.Double)
                                            ,("NewNewNumber", T.Integer)
                                            ,("1xN_A", T.Varchar)
                                            ,("1xN_B", T.Varchar)
                                            ,("1xN_C", T.Varchar)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys

    -- Test Julius
        rtabNew10_J = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlC $
                    Source ["New_Nx1_Col"] $
                    Target ["1xN_A","1xN_B","1xN_C"] $
                    By (\[(T.RText txt)] -> [T.RText (Data.Text.take 5 txt), T.RText "<-|-|-|->", T.RText (Data.Text.takeEnd 10 txt)]) (On $ Tab rtabNew9)
                    DontRemoveSrc $
                    FilterBy (\_ -> True)
                )

    writeResult fo "_t10.csv" rtmdata10 rtabNew10                                       
    writeResult fo "_t10_J.csv" rtmdata10 rtabNew10_J 
    T.printRTable rtabNew10_J                                

-- Test a RMapNxM column mapping
    let 
        cmap6 = E.RMapNxM {E.srcColGrp = ["1xN_A","1xN_B","1xN_C"], E.removeSrcCol = E.Yes, E.trgColGrp = ["ColNew1","ColNew2"], E.transformNxM = transformation, E.srcRTupleFilter = \_ -> True}
                where
                    transformation [T.RText t1, T.RText t2, T.RText t3] = [T.RText (t1 `append` t3), T.RText t2]
        rtabNew11 = E.runCM cmap6 rtabNew10
        rtmdata11 = T.createRTableMData ( "TestTable11", 
                                         [ -- ("Name", T.Varchar),
                                            ("New_Nx1_Col", T.Varchar)
                                            ,("MyDate", T.Date "DD/MM/YYYY") 
                                           -- ,("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS")
                                            ,("Number", T.Integer)
                                            ,("DNumber", T.Double)
                                            ,("NewNewNumber", T.Integer)
                                            ,("ColNew1", T.Varchar)
                                            ,("ColNew2", T.Varchar)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys

    -- Test Julius
        rtabNew11_J = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlC $
                    Source ["1xN_A","1xN_B","1xN_C"] $
                    Target ["ColNew1","ColNew2"] $
                    By transformation (On $ Tab rtabNew10)
                    RemoveSrc $
                    FilterBy (\_ -> True)
                )
            where
                transformation [T.RText t1, T.RText t2, T.RText t3] = [T.RText (t1 `append` t3), T.RText t2]

    writeResult fo "_t11.csv" rtmdata11 rtabNew11                                       
    writeResult fo "_t11_J.csv" rtmdata11 rtabNew11_J 
    T.printRTable rtabNew11_J                                     

-- Test removeColumn operation
    let 
        rtabNew12 = T.removeColumn "NewNewNumber" (T.removeColumn "MyDate" rtabNew11)        
        rtmdata12 = T.createRTableMData ( "TestTable12", 
                                         [ -- ("Name", T.Varchar),
                                            ("New_Nx1_Col", T.Varchar)
                                      --      ,("MyDate", T.Date "DD/MM/YYYY") 
                                           -- ,("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS")
                                            ,("Number", T.Integer)
                                            ,("DNumber", T.Double)
                                      --      ,("NewNewNumber", T.Integer)
                                            ,("ColNew1", T.Varchar)
                                            ,("ColNew2", T.Varchar)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys

    -- Test Julius
        rtabNew12_J = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlR $
                    ROpStart
                    :.  (GenUnaryOp (On $ Tab rtabNew11) (ByUnaryOp myfunc))
                )
            where
                myfunc = (T.removeColumn "NewNewNumber") . (T.removeColumn "MyDate")


    writeResult fo "_t12.csv" rtmdata12 rtabNew12 -- this calls internally C.fromRTable and thus it cannot actually test the column removal (because the column removal is hidden
                                                  -- by the RTable metadata). An explicit print of the new RTable is required in order to check correctness.

    writeResult fo "_t12_J.csv" rtmdata12 rtabNew12_J                                                  
    
    print rtabNew12
    print rtabNew12_J
    T.printRTable rtabNew12_J

--  Test combined Roperations
    let
        myfilter = T.f (\t -> t!"Name" == T.RText {T.rtext = "Karagiannidis"})
        myprojection = T.p ["Name","MyTime","Number", "ColNew2"]
        myjoin = T.iJ (\t1 t2 -> t1!"Number" == t2!"Number") rtabNew12  -- !!! Check that the other table participating in the join is embedded in the myjoin operation
        rcombined = myprojection . myjoin . myfilter 
        rtabNew13 = T.rComb rcombined rtab
        rtmdata13 = T.createRTableMData ( "TestTable13", 
                                [   ("Name", T.Varchar)
                                    --,("MyDate", T.Date "DD/MM/YYYY")
                                    ,("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS")
                                    ,("Number", T.Integer) 
                                    --,("DNumber", T.Double)
                                    ,("ColNew2", T.Varchar)
                                ]
                              ) 
                                []  -- primary key
                                []  -- list of unique keys

    -- Test Julius
        rtabNew13_J = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlR $
                    ROpStart
                    :.  (Filter (From $ Tab rtab) $ FilterBy (\t -> t!"Name" == T.RText {T.rtext = "Karagiannidis"}))
                    :.  (Join (TabL rtabNew12) (Previous) $ JoinOn (\t1 t2 -> t1!"Number" == t2!"Number"))
                    :.  (Select ["Name","MyTime","Number", "ColNew2"] $ From Previous)
                )


    writeResult fo "_t13.csv" rtmdata13 rtabNew13
    writeResult fo "_t13_J.csv" rtmdata13 rtabNew13_J
    
    print rtabNew13
    print rtabNew13_J
    T.printRTable rtabNew13_J

-- Test Left Outer Join
    let rtabNew14 = T.lJ (\t1 t2 -> t1!"Number" == t2!"Number") rtab rtabNew3
        rtmdata14 = T.createRTableMData ( "TestTable14", 
                                         [  ("Name", T.Varchar),
                                            ("MyDate", T.Date "DD/MM/YYYY"), 
                                            ("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("Number", T.Integer), 
                                            ("DNumber", T.Double),
                                            ("NewNumber", T.Integer)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys

-- debug left join 
    -- the first part is the join
    let rtabNew14_dbg_1part = T.iJ (\t1 t2 -> t1!"Number" == t2!"Number") rtab rtabNew3  -- these are the rows of the left tab (enhanced with new columns) that satisfy the join
        -- project only left tab's columns
        rtabNew14_dbg_1part_proj = T.p (T.getColumnNamesfromRTab rtab) rtabNew14_dbg_1part


        -- enhance the left tab with the new columns with NULL values
--        left_tab_enhanced = T.iJ (\t1 t2 -> True) rtab (T.createSingletonRTable $ T.createNullRTuple ["Name","Number","NewNumber"])
        
        -- the second part are the rows from the preserving table that dont join
        rtabNew14_dbg_2part_a = 
            let
                leftTab = rtab --T.nvlRTable "DNumber" (T.RText "x") rtab  -- need to eliminate the Null values because Null == Null gives False.
                rightTab = rtabNew14_dbg_1part_proj --T.nvlRTable "DNumber" (T.RText "x") rtabNew14_dbg_1part_proj
            in  T.d leftTab rightTab --diffTab = T.d leftTab rightTab  -- get the  rows from left tab that dont join
        --    in  -- remove "x" and turn it back to Null
        --        T.decodeRTable "DNumber" (T.RText "x") T.Null T.Null T.Ignore diffTab

        -- enhance this with null columns from the non-preserving table
        rtabNew14_dbg_2part = T.iJ (\t1 t2 -> True) rtabNew14_dbg_2part_a (T.createSingletonRTable $ T.createNullRTuple $ (T.getColumnNamesfromRTab rtabNew3))

        -- finally, union the two parts
        rtabNew14_dbg = T.u rtabNew14_dbg_1part rtabNew14_dbg_2part

    -- Test Julius
        rtabNew14_J = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlR $
                    ROpStart
                    :.  (LJoin (TabL rtab) (Tab rtabNew3) $ JoinOn (\t1 t2 -> t1!"Number" == t2!"Number"))
                )

        
    writeResult fo "_t14.csv" rtmdata14 rtabNew14
    writeResult fo "_t14_dbg_1part.csv" rtmdata14 rtabNew14_dbg_1part
    writeResult fo "_t14_dbg_1part_proj.csv" rtmdata rtabNew14_dbg_1part_proj
    writeResult fo "_t14_dbg_dbg_2part_a.csv" rtmdata  rtabNew14_dbg_2part_a
    --writeResult fo "_t14_left_tab_enhanced.csv" rtmdata14 left_tab_enhanced
    writeResult fo "_t14_dbg_2part.csv" rtmdata14 rtabNew14_dbg_2part
    writeResult fo "_t14_dbg.csv" rtmdata14 rtabNew14_dbg
    writeResult fo "_t14_J.csv" rtmdata14 rtabNew14_J
    putStrLn "Test LEFT JOIN"
    T.printRTable rtabNew14_J

-- Test Right Outer Join
    let rtabNew15 = T.rJ (\t1 t2 -> t1!"Number" == t2!"Number") rtab rtabNew3
        rtmdata15 = T.createRTableMData ( "TestTable15", 
                                         [  ("Name", T.Varchar),
                                            ("MyDate", T.Date "DD/MM/YYYY"), 
                                            ("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("Number", T.Integer), 
                                            ("DNumber", T.Double),
                                            ("NewNumber", T.Integer)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys

    -- Test Julius
        rtabNew15_J = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlR $
                    ROpStart
                    :.  (RJoin (TabL rtab) (Tab rtabNew3) $ JoinOn (\t1 t2 -> t1!"Number" == t2!"Number"))
                )
        
    writeResult fo "_t15.csv" rtmdata15 rtabNew15
    writeResult fo "_t15_J.csv" rtmdata15 rtabNew15_J
    putStrLn "Test RIGHT JOIN"
    T.printRTable rtabNew15_J

-- Test Full Outer Join
    let rtabNew15fo = T.foJ (\t1 t2 -> t1!"Number" == t2!"Number") rtab rtabNew3
        rtabNew15fo2 = T.foJ (\t1 t2 -> t1!"Number" == t2!"Number") rtabNew3 rtab 
        rtmdata15 = T.createRTableMData ( "TestTable15", 
                                         [  ("Name", T.Varchar),
                                            ("MyDate", T.Date "DD/MM/YYYY"), 
                                            ("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("Number", T.Integer), 
                                            ("DNumber", T.Double),
                                            ("NewNumber", T.Integer)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys

    -- Test Julius
        rtabNew15fo_J = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlR $
                    ROpStart
                    :.  (FOJoin (TabL rtab) (Tab rtabNew3) $ JoinOn (\t1 t2 -> t1!"Number" == t2!"Number"))
                )
        
        rtabNew15fo2_J = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlR $
                    ROpStart
                    :.  (FOJoin (TabL rtabNew3) (Tab rtab) $ JoinOn (\t1 t2 -> t1!"Number" == t2!"Number"))
                )


    writeResult fo "_t15_fo.csv" rtmdata15 rtabNew15fo
    writeResult fo "_t15_fo2.csv" rtmdata15 rtabNew15fo2
    writeResult fo "_t15_fo_J.csv" rtmdata15 rtabNew15fo_J
    writeResult fo "_t15_fo2_J.csv" rtmdata15 rtabNew15fo2_J
    putStrLn "Test FULL OUTER JOIN"
    T.printRTable rtabNew15fo2_J
        



-- Test Aggregation
    -- 
    let rtabNew16 = T.rAgg [T.raggSum "Number" "SumNumber" 
                            , T.raggCount "Number" "CountNumber"
                            , T.raggAvg "Number" "AvgNumber" 
                            , T.raggSum "Name" "SumName"
                            ,T.raggMax "DNumber" "maxDNumber"
                            ,T.raggMax "Number" "maxNumber"
                            ,T.raggMax "Name" "maxName"
                            ,T.raggMin "DNumber" "minDNumber"
                            ,T.raggMin "Number" "minNumber"
                            ,T.raggMin "Name" "minName"
                            ,T.raggAvg "Name" "AvgName"
                         ] rtabNew
        rtmdata16 = T.createRTableMData ( "TestTable16", 
                                         [  --("Name", T.Varchar),
                                            --("MyDate", T.Date "DD/MM/YYYY"), 
                                            --("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("SumNumber", T.Integer) 
                                            ,("CountNumber", T.Integer)
                                            ,("AvgNumber", T.Double)
                                            ,("SumName", T.Integer) 
                                            ,("maxDNumber", T.Double)
                                            ,("maxNumber", T.Integer)
                                            ,("maxName", T.Varchar)
                                            ,("minDNumber", T.Double)
                                            ,("minNumber", T.Integer)
                                            ,("minName", T.Varchar)
                                            ,("AvgName", T.Double)
                                            --,("NewNumber", T.Integer)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys
        
    
    -- Test Julius
        rtabNew16_J = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlR $
                    ROpStart
                    :.  (Agg $ 
                            AggOn [ Sum "Number" $ As "SumNumber"
                                    ,Count "Number" $ As "CountNumber"
                                    ,Avg "Number" $ As "AvgNumber" 
                                    ,Sum "Name" $ As "SumName"
                                    ,Max "DNumber" $ As "maxDNumber"
                                    ,Max "Number" $ As "maxNumber"
                                    ,Max "Name" $ As "maxName"
                                    ,Min "DNumber" $ As "minDNumber"
                                    ,Min "Number" $ As "minNumber"
                                    ,Min "Name" $ As "minName"
                                    ,Avg "Name" $ As "AvgName"] $ From $ Tab rtabNew)
                )

    writeResult fo "_t16.csv" rtmdata16 rtabNew16
    writeResult fo "_t16_J.csv" rtmdata16 rtabNew16_J

    T.printRTable rtabNew16_J

-- Test GroupBy
    -- 
    let rtabNew17 = T.rG    (\t1 t2 ->  t1!"Name" == t2!"Name" && t1!"MyTime" == t2!"MyTime")  
                            [   T.raggSum "Number" "SumNumber" 
                                , T.raggCount "Name" "CountName"
                                -- , T.raggAvg "Number" "AvgNumber" 
                                , T.raggSum "Name" "SumName"
                                , T.raggCount "DNumber" "CountDNumber"
                                , T.raggMax "DNumber" "maxDNumber"
                                -- ,T.raggMax "Number" "maxNumber"
                                , T.raggMax "Name" "maxName"
                                -- ,T.raggMin "DNumber" "minDNumber"
                                -- ,T.raggMin "Number" "minNumber"
                                -- ,T.raggMin "Name" "minName"
                                -- ,T.raggAvg "Name" "AvgName"
                             ] 
                             ["Name", "MyTime"]
                             rtabNew

        -- debugging
--        rtupList = toList rtabNew 
--        listOfRTupGroupLists = Data.List.groupBy (\t1 t2 ->  t1!"Name" == t2!"Name") rtupList

        rtmdata17 = T.createRTableMData ( "TestTable17", 
                                         [  ("Name", T.Varchar)
                                            --,("MyDate", T.Date "DD/MM/YYYY"), 
                                            ,("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS")
                                            ,("SumNumber", T.Integer) 
                                            ,("CountName", T.Integer)
                                            -- ,("AvgNumber", T.Double)
                                            ,("SumName", T.Integer) 
                                            ,("CountDNumber", T.Integer)
                                            ,("maxDNumber", T.Double)
                                            -- ,("maxNumber", T.Integer)
                                            ,("maxName", T.Varchar)
                                            -- ,("minDNumber", T.Double)
                                            -- ,("minNumber", T.Integer)
                                            -- ,("minName", T.Varchar)
                                            -- ,("AvgName", T.Double)
                                            --,("NewNumber", T.Integer)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys

    -- Test Julius
        rtabNew17_J = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlR $
                    ROpStart
                    :.  (GroupBy ["Name", "MyTime"] 
                            (AggOn [ Sum "Number" $ As "SumNumber"
                                    ,Count "Name" $ As "CountName"
                                    --,Avg "Number" $ As "AvgNumber" 
                                    ,Sum "Name" $ As "SumName"
                                    ,Count "DNumber" $ As "CountDNumber"
                                    ,Max "DNumber" $ As "maxDNumber"
                                    --,Max "Number" $ As "maxNumber"
                                    ,Max "Name" $ As "maxName"]  $ From $ Tab rtabNew)
                            $ GroupOn (\t1 t2 ->  t1!"Name" == t2!"Name" && t1!"MyTime" == t2!"MyTime"))
                )

        rtabNew17_J2 = E.etl $ evalJulius $
            EtlMapStart
            :-> (EtlR $
                    ROpStart
                    :.  (GroupBy ["Name", "MyTime"] 
                            (AggOn [] {-[ Sum "Number" $ As "SumNumber"
                                    ,Count "Name" $ As "CountName"
                                    --,Avg "Number" $ As "AvgNumber" 
                                    ,Sum "Name" $ As "SumName"
                                    ,Count "DNumber" $ As "CountDNumber"
                                    ,Max "DNumber" $ As "maxDNumber"
                                    --,Max "Number" $ As "maxNumber"
                                    ,Max "Name" $ As "maxName"]-}  $ From $ Tab rtabNew)
                            $ GroupOn (\t1 t2 ->  t1!"Name" == t2!"Name" && t1!"MyTime" == t2!"MyTime"))
                )

    
  --  print listOfRTupGroupLists
    writeResult fo "_t17.csv" rtmdata17 rtabNew17
    writeResult fo "_t17_J.csv" rtmdata17 rtabNew17_J

    putStrLn "#### TEST GROUP BY ###"
    T.printRTable rtabNew 
    T.printRTable rtabNew17_J

    -- test groupNoAgg function
    putStrLn "GroupBy with [] AggOp list"
    T.printRTable rtabNew17_J2
    putStrLn "Test groupNoAgg function"
    T.printRTable $
        groupNoAgg  (\t1 t2 -> t1!"Name" == t2!"Name" && t1!"MyTime" == t2!"MyTime")
                    ["Name", "MyTime"]
                    rtabNew

    -- test groupNoAggList function                    
    putStrLn "Test groupNoAggList function"
    mapM_ (T.printRTable) $
        groupNoAggList  (\t1 t2 -> t1!"Name" == t2!"Name" && t1!"MyTime" == t2!"MyTime")
                    ["Name", "MyTime"]
                    rtabNew


    putStrLn "#### TEST CUSTOM Aggregation ###"
    putStrLn "Implement a custom listagg()"

    printRTable $ juliusToRTable $
        EtlMapStart
        :-> (EtlR $
                ROpStart
                :. (Select ["Name"] (From $ Tab rtabNew))
            )

    rtabNew18 <- runJulius $
            EtlMapStart
            :-> (EtlR $
                    ROpStart
                    :.  (GroupBy ["Name"] -- , "MyTime"] 
                            (AggOn [    {-Sum "Number" $ As "SumNumber"
                                        ,Count "Name" $ As "CountName"
                                        --,Avg "Number" $ As "AvgNumber" 
                                        ,Sum "Name" $ As "SumName"
                                        ,Count "DNumber" $ As "CountDNumber"
                                        ,Max "DNumber" $ As "maxDNumber"
                                        --,Max "Number" $ As "maxNumber"
                                        ,Max "Name" $ As "maxName"
                                        ,-}
                                        Count "Name" $ As "CountName"
                                        ,GenAgg "Name" (As "ListAggName") $ AggBy $ myListAgg (pack ";")
                                    ]  $ From $ Tab rtabNew)
                            $ GroupOn (\t1 t2 ->  t1!"Name" == t2!"Name" )) -- && t1!"MyTime" == t2!"MyTime"))
                )

    -- T.printRTable rtabNew18
    T.printfRTable (genRTupleFormat [ "Name", "ListAggName", "CountName"] genDefaultColFormatMap) $ rtabNew18


    where
        myListAgg :: Text -> AggFunction
        myListAgg delimiter col rtab = 
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

    --print csvNew2
    --return ()
    
    --C.writeCSVFile fo newcsv 
    --C.printCSVFile newcsv

writeResult ::
    String   -- File name
    -> Text  -- new file suffix
    -> T.RTableMData 
    -> T.RTable
    -> IO()
writeResult fname sfx md rt = do
    let foNameNew = (fromJust (stripSuffix ".csv"  (pack fname))) `mappend` sfx
        csvNew = C.fromRTable md rt
    C.writeCSV (unpack foNameNew) csvNew            
    C.printCSV csvNew


