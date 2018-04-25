{-|
Module      : ETL
Description : Implements ETL operations over RTables.
Copyright   : (c) Nikos Karagiannidis, 2017
                  
License     : GPL-3
Maintainer  : nkarag@gmail.com
Stability   : experimental
Portability : POSIX

Here is a longer description of this module, containing some
commentary with @some markup@.
-}

{-# LANGUAGE OverloadedStrings #-}
-- :set -XOverloadedStrings
--{-# LANGUAGE OverloadedRecordFields #-}
--{-# LANGUAGE  DuplicateRecordFields #-}

module Etl.Core
    (
        RColMapping (..)
        ,ColXForm
        ,createColMapping
        --,runColMapping
        ,runCM
        ,YesNo (..)
        --,RTabMapping
        --,runTabMapping
        --,t
        ,ETLOperation (..)
        ,etlOpU
        ,etlOpB
        ,ETLMapping (..)
        --,runETLmapping
        ,etl
        ,etlRes
        ,rtabToETLMapping
        ,createLeafETLMapLD
        ,createLeafBinETLMapLD
        ,connectETLMapLD
        ,rdtappend 
        ,addSurrogateKey    
        ,addSurrogateKeyJ    
        ,appendRTable   
        ,appendRTableJ
    ) where 

-- Data.RTable
import RTable.Core

-- Text
import Data.Text as T

-- HashMap                          -- https://hackage.haskell.org/package/unordered-containers-0.2.7.2/docs/Data-HashMap-Strict.html
import Data.HashMap.Strict as HM

-- Data.List
import Data.List (notElem, map, zip)

-- Data.Vector
import Data.Vector as V

data YesNo = Yes | No deriving (Eq, Show)

-- | RColMapping: This is the basic data type to define the column-to-column mapping from a source RTable to a target RTable.
--   Essentially, an RColMapping represents the column-level transformations of an Rtuple that will yield a target RTuple. 
--
--   A mapping is simply a triple of the form ( Source-Column(s), Target-Column(s), Transformation, RTuple-Filter), where we define the source columns
--   over which a transformation (i.e. a function) will be applied in order to yield the target columns. Also, an RPredicate (i.e. a filter) might be applied on the source RTuple.
--   Remember that an RTuple is essentially a mapping between a key (the Column Name) and a value (the RDataType value). So the various RColMapping
--   data constructors below simply describe the possible modifications of an RTuple orginating from its own columns.
--
--   So, we can have the following mapping types:
--          a) single-source column to single-target column mapping (1 to 1), 
--                  the source column will be removed or not based on the removeSrcCol flag (dublicate column names are not allowed in an RTuple)
--          b) multiple-source columns to single-target column mapping (N to 1),
--                  The N columns will be merged  to the single target column based on the transformation.
--                  The N columns will be removed from the RTuple or not based on the removeSrcCol flag (dublicate column names are not allowed in an RTuple)
--          c) single-source column to multiple-target columns mapping  (1 to M)
--                  the source column will be "expanded" to M target columns based ont he transformation.
--                  the source column will be removed or not based on the removeSrcCol flag (dublicate column names are not allowed in an RTuple)                  
--          d) multiple-source column to multiple target columns mapping (N to M)
--                  The N source columns will be mapped to M target columns based on the transformation.
--                  The N columns will be removed from the RTuple or not based on the removeSrcCol flag (dublicate column names are not allow in an RTuple)
--
--   Some examples of mapping are the following:
--   @
--      ("Start_Date", No, "StartDate", \t -> True)  --  copy the source value to target and dont remove the source column, so the target RTuple will have both columns "Start_Date" and "StartDate"
--                                           --  with the exactly the same value)
--
--      (["Amount", "Discount"], Yes, "FinalAmount", (\[a, d] -> a * d) ) -- "FinalAmount" is a derived column based on a function applied to the two source columns. 
--                                                                        --  In the final RTuple we remove the two source columns.
--   @
--
--  An RColMapping can be applied with the runCM (runColMapping) operator
--
data RColMapping = 
        ColMapEmpty
    |   RMap1x1 { srcCol :: ColumnName,         removeSrcCol :: YesNo,  trgCol :: ColumnName,       transform1x1 :: RDataType    -> RDataType,   srcRTupleFilter:: RPredicate   }   -- ^ single-source column to single-target column mapping (1 to 1).
    |   RMapNx1 { srcColGrp :: [ColumnName],    removeSrcCol :: YesNo,  trgCol :: ColumnName,       transformNx1 :: [RDataType]  -> RDataType,   srcRTupleFilter:: RPredicate   }      -- ^ multiple-source columns to single-target column mapping (N to 1)
    |   RMap1xN { srcCol :: ColumnName,         removeSrcCol :: YesNo,  trgColGrp :: [ColumnName],  transform1xN :: RDataType    -> [RDataType], srcRTupleFilter:: RPredicate }    -- ^ single-source column to multiple-target columns mapping (1 to N)
    |   RMapNxM { srcColGrp :: [ColumnName],    removeSrcCol :: YesNo,  trgColGrp :: [ColumnName],  transformNxM :: [RDataType]  -> [RDataType], srcRTupleFilter:: RPredicate }    -- ^ multiple-source column to multiple target columns mapping (N to M)                                                 

type ColXForm = [RDataType]  -> [RDataType]

-- | Constructs an RColMapping.
-- This is the suggested method for creating a column mapping and not by calling the data constructors directly.
createColMapping :: 
       [ColumnName]  -- ^ List of source column names
    -> [ColumnName]  -- ^ List of target column names
    -> ColXForm      -- ^ Column Transformation function
    -> YesNo         -- ^ Remove source column option
    -> RPredicate    -- ^ Filtering predicate
    -> RColMapping   -- ^ Output Column Mapping
createColMapping (src:[]) (trg:[]) xForm remove fPred = RMap1x1 {srcCol = src, removeSrcCol = remove, trgCol = trg, transform1x1 = \x -> unlist $ xForm (x:[]), srcRTupleFilter = fPred}
                                                            where unlist :: [a] -> a
                                                                  unlist (x:[]) = x  -- since this is a 1x1 col mapping, we are sure that xForm will return a single element list
createColMapping srcCols (trg:[]) xForm remove fPred =  RMapNx1 {srcColGrp = srcCols, removeSrcCol = remove, trgCol = trg, transformNx1 = \x -> unlist $ xForm (x), srcRTupleFilter = fPred}                                                                
                                                            where unlist :: [a] -> a
                                                                  unlist (x:[]) = x  -- since this is a Nx1 col mapping, we are sure that xForm will return a single element list
createColMapping (src:[]) trgCols xForm remove fPred =  RMap1xN {srcCol = src, removeSrcCol = remove, trgColGrp = trgCols, transform1xN = \x -> xForm (x:[]), srcRTupleFilter = fPred}                                                                  
createColMapping srcCols trgCols xForm remove fPred =  RMapNxM {srcColGrp = srcCols, removeSrcCol = remove, trgColGrp = trgCols, transformNxM = xForm, srcRTupleFilter = fPred}


-- | runCM operator executes an RColMapping
runCM = runColMapping

-- | Apply an RColMapping to a source RTable and produce a new RTable.
runColMapping :: RColMapping -> RTable -> RTable
runColMapping ColMapEmpty rtabS = rtabS
runColMapping rmap rtabS = 
    if isRTabEmpty rtabS
        then emptyRTable
        else 
            case rmap of 
                RMap1x1 {srcCol = src, trgCol = trg, removeSrcCol = rmvFlag, transform1x1 = xform, srcRTupleFilter = pred} -> do  -- an RTable is a Monad just like a list is a Monad, representing a non-deterministic value
                        srcRtuple <- f pred rtabS                                                                        
                        let 
                            -- 1. get original column value 
                            srcValue = getRTupColValue src srcRtuple
                            -- srcValue = HM.lookupDefault    Null -- return Null if value cannot be found based on column name 
                            --                                src   -- column name to look for (source) - i.e., the key in the HashMap
                            --                                srcRtuple  -- source RTuple (i.e., a HashMap ColumnName RDataType)
                            
                            -- 2. apply transformation to retrieve new column value
                            trgValue = xform srcValue                                         
                            
                            -- 3. remove the original ColumnName, Value mapping from the RTuple
                            rtupleTemp = 
                                case rmvFlag of
                                    Yes -> HM.delete src srcRtuple
                                    No  -> srcRtuple
                            
                            -- 4. insert new (ColumnName, Value) pair and thus create the target RTuple
                            trgRtuple = HM.insert trg trgValue rtupleTemp
                        
                        -- return new RTable
                        return trgRtuple

                RMapNx1 {srcColGrp = srcL, trgCol = trg, removeSrcCol = rmvFlag, transformNx1 = xform, srcRTupleFilter = pred} -> do  -- an RTable is a Monad just like a list is a Monad, representing a non-deterministic value
                        srcRtuple <- f pred rtabS                                                                        
                        let 
                            -- 1. get original column value (in this case it is a list of values)
                            srcValueL = Data.List.map ( \src ->  getRTupColValue src srcRtuple

                                            -- \src -> HM.lookupDefault       Null -- return Null if value cannot be found based on column name 
                                            --                                 src   -- column name to look for (source) - i.e., the key in the HashMap
                                            --                                 srcRtuple  -- source RTuple (i.e., a HashMap ColumnName RDataType)
                                            ) srcL
                            
                            -- 2. apply transformation to retrieve new column value
                            trgValue = xform srcValueL                                         
                            
                            -- 3. remove the original (ColumnName, Value) mappings from the RTuple (i.e., remove ColumnNames mentioned in the RColMapping from source RTuple)
                            rtupleTemp = 
                                case rmvFlag of
                                    Yes -> HM.filterWithKey (\colName _ -> Data.List.notElem colName srcL) srcRtuple
                                    No  -> srcRtuple
                            
                            -- 4. insert new ColumnName, Value mapping as the target RTuple must be
                            trgRtuple = HM.insert trg trgValue rtupleTemp
                        -- return new RTable
                        return trgRtuple

                RMap1xN {srcCol = src, trgColGrp = trgL, removeSrcCol = rmvFlag, transform1xN = xform, srcRTupleFilter = pred} -> do  -- an RTable is a Monad just like a list is a Monad, representing a non-deterministic value
                        srcRtuple <- f pred rtabS                                                                        
                        let 
                            -- 1. get original column value 
                            srcValue = getRTupColValue src srcRtuple

                            -- srcValue = HM.lookupDefault    Null -- return Null if value cannot be found based on column name 
                            --                                src   -- column name to look for (source) - i.e., the key in the HashMap
                            --                                srcRtuple  -- source RTuple (i.e., a HashMap ColumnName RDataType)

                            -- 2. apply transformation to retrieve new column value list
                            trgValueL = xform srcValue                                         

                            -- 3. remove the original ColumnName, Value mapping from the RTuple
                            rtupleTemp = 
                                case rmvFlag of
                                    Yes -> HM.delete src srcRtuple
                                    No  -> srcRtuple

                            -- 4. insert new (ColumnName, Value) pairs to the target RTuple
                            tempL = Data.List.zip trgL trgValueL
                            trgRtuple = HM.union (HM.fromList tempL) rtupleTemp  -- implement as a hashmap union between new (columnName,value) pairs and source tuple
                                
                        -- return new RTable
                        return trgRtuple

                RMapNxM {srcColGrp = srcL, trgColGrp = trgL, removeSrcCol = rmvFlag, transformNxM = xform, srcRTupleFilter = pred} -> do  -- an RTable is a Monad just like a list is a Monad, representing a non-deterministic value
                        srcRtuple <- f pred rtabS                                                                        
                        let 
                            -- 1. get original column value (in this case it is a list of values)
                            srcValueL = Data.List.map (  \src ->  getRTupColValue src srcRtuple

                                            -- \src -> HM.lookupDefault       Null -- return Null if value cannot be found based on column name 
                                            --                                 src   -- column name to look for (source) - i.e., the key in the HashMap
                                            --                                 srcRtuple  -- source RTuple (i.e., a HashMap ColumnName RDataType)
                                            ) srcL
                            
                            -- 2. apply transformation to retrieve new column value
                            trgValueL = xform srcValueL                                         

                            -- 3. remove the original ColumnName, Value mapping from the RTuple
                            rtupleTemp = 
                                case rmvFlag of
                                    Yes -> HM.filterWithKey (\colName _ -> Data.List.notElem colName srcL) srcRtuple
                                    No  -> srcRtuple

                            -- 4. insert new (ColumnName, Value) pairs to the target RTuple
                            tempL = Data.List.zip trgL trgValueL
                            trgRtuple = HM.union (HM.fromList tempL) rtupleTemp  -- implement as a hashmap union between new (columnName,value) pairs and source tuple
                                
                        -- return new RTable
                        return trgRtuple

-- | An ETL operation applied to an RTable can be either an ROperation (a relational agebra operation like join, filter etc.) defined in 'RTable' module,
--   or an RColMapping applied to an RTable
data ETLOperation =  
            ETLrOp { rop  :: ROperation   } 
        |   ETLcOp { cmap :: RColMapping } 
                


-- | executes a Unary ETL Operation
etlOpU = runUnaryETLOperation

-- | executes an ETL Operation
runUnaryETLOperation ::
    ETLOperation
    -> RTable  -- ^ input RTable
    -> RTable  -- ^ output RTable
runUnaryETLOperation op inpRtab = 
    case op of 
        ETLrOp { rop  = relOp  } -> ropU relOp inpRtab
        ETLcOp { cmap = colMap } -> runCM colMap inpRtab

-- | executes a Binary ETL Operation
etlOpB = runBinaryETLOperation

-- | executes an ETL Operation
runBinaryETLOperation ::
    ETLOperation
    -> RTable  -- ^ input RTable1
    -> RTable  -- ^ input RTable2    
    -> RTable  -- ^ output RTable
runBinaryETLOperation ETLrOp {rop  = relOp} inpT1 inpT2 = ropB relOp inpT1 inpT2


-- | ETLmapping : it is the equivalent of a mapping in an ETL tool and consists of a series of ETLOperations that are applied, one-by-one,
--   to some initial input RTable, but if binary ETLOperations are included in the ETLMapping, then there will be more than one input RTables that
--   the ETLOperations of the ETLMapping will be applied to. When we apply (i.e., run) an ETLOperation of the ETLMapping we get a new RTable,
--   which is then inputed to the next ETLOperation, until we finally run all ETLOperations. The purpose of the execution of an ETLMapping is     
--   to produce a single new RTable as the result of the execution of all the ETLOperations of the ETLMapping.
--   In terms of database operations an ETLMapping is the equivalent of an CREATE AS SELECT (CTAS) operation in an RDBMS. This means that
--   anything that can be done in the SELECT part (i.e., column projection, row filtering, grouping and join operations, etc.)
--   in order to produce a new table, can be included in an ETLMapping.
--
--   An ETLMapping is executed with the etl (runETLmapping) operator
--
--   Implementation: 
--   An ETLMapping is implemented as a binary tree where the node represents the ETLOperation to be executed and the left branch is another 
--   ETLMapping, while the right branch is an RTable (that might be empty in the case of a Unary ETLOperation). 
--   Execution proceeds from bottom-left to top-right.
--   This is similar in concept to a left-deep join tree. In a Left-Deep ETLOperation tree the "pipe" of ETLOperations comes from 
--   the left branches always.
--   The leaf node is always an ETLMapping with an ETLMapEmpty in the left branch and an RTable  in the right branch (the initial RTable inputed
--   to the ETLMapping).
--   In this way, the result of the execution of each ETLOperation (which is an RTable) is passed on to the next ETLOperation. Here is an example:
--
-- @
--     A Left-Deep ETLOperation Tree
-- 
--                              final RTable result
--                                    / 
--                                 etlOp3 
--                              /       \ 
--                           etlOp2     rtab2
--                          /      \ 
-- A leaf-node -->    etlOp1    emptyRTab
--                    /       \
--              ETLMapEmpty   rtab1
--
-- @
--
--  You see that always on the left branch we have an ETLMapping data type (i.e., a left-deep ETLOperation tree). 
--  So how do we implement the following case?
--
-- @
--
--                     final RTable result
--                             / 
--  A leaf-node -->         etlOp1 
--                          /       \
--                         rtab1   rtab2
--
-- @
--
-- The answer is that we "model" the left RTable (rtab1 in our example) as an ETLMapping of the form:
--
-- @
--  ETLMapLD { etlOp = ETLcOp{cmap = ColMapEmpty}, tabL = ETLMapEmpty, tabR = rtab1 }
-- @
--
-- So we embed the rtab1 in a ETLMapping, which is a leaf (i.e., it has an empty prevMap), the rtab1 is in 
-- the right branch (tabR) and the ETLOperation is the EmptyColMapping, which returns its input RTable when executed.
-- We can use function 'rtabToETLMapping' for this job. So it becomes
-- @
-- A leaf-node -->    etlOp1    
--                    /     \
--   rtabToETLMapping rtab1  rtab2
-- @
--
-- In this manner, a leaf-node can also be implemented like this:
--
-- @
--                              final RTable result
--                                    / 
--                                 etlOp3 
--                              /       \ 
--                           etlOp2     rtab2
--                          /      \ 
-- A leaf-node -->    etlOp1    emptyRTab
--                    /     \
--   rtabToETLMapping rtab1  emptyRTable
-- @
--
data ETLMapping = 
        ETLMapEmpty -- ^ an empty node
    |   ETLMapLD    
        {           etlOp     :: ETLOperation -- ^ the ETLOperation to be executed    
                    ,tabL     :: ETLMapping   -- ^ the left-branch corresponding to the previous ETLOperation, which is input to this one.
                                              --       
                    ,tabR     :: RTable       -- ^ the right branch corresponds to another RTable (for binary ETL operations). 
                                               --  If this is a Unary ETLOperation then this field must be an empty RTable.
        } -- ^ a Left-Deep node
    |   ETLMapRD    
        {           etlOp     :: ETLOperation -- ^ the ETLOperation to be executed    
                    ,tabLrd     :: RTable       -- ^ the left-branch corresponds to another RTable (for binary ETL operations). 
                                              --   If this is a Unary ETLOperation then this field must be an empty RTable.                                                                                             
                    ,tabRrd     :: ETLMapping   -- ^ the right branch corresponding to the previous ETLOperation, which is input to this one.
        } -- ^ a Right-Deep node
    |   ETLMapBal    
        {           etlOp     :: ETLOperation -- ^ the ETLOperation to be executed    
                    ,tabLbal     :: ETLMapping   -- ^ the left-branch corresponding to the previous ETLOperation, which is input to this one. 
                                              --   If this is a Unary ETLOperation then this field might be an empty ETLMapping.                                                                                             
                    ,tabRbal     :: ETLMapping   -- ^ the right branch corresponding corresponding to the previous ETLOperation, which is input to this one.                                               --   If this is a Unary ETLOperation then this field might be an empty ETLMapping.
        } -- ^ a Balanced node


instance Eq ETLMapping where
    etlMap1 == etlMap2 = 
            (etl etlMap1) == (etl etlMap2)  -- two ETLMappings are equal if the RTables resulting from their execution are equal

-- | Creates a left-deep leaf ETL Mapping, of the following form:
-- @
--     A Left-Deep ETLOperation Tree
-- 
--                              final RTable result
--                                    / 
--                                 etlOp3 
--                              /       \ 
--                           etlOp2     rtab2
--                          /      \ 
-- A leaf-node -->    etlOp1    emptyRTab
--                    /       \
--              ETLMapEmpty   rtab1
--
-- @
--
createLeafETLMapLD ::
       ETLOperation -- ^ ETL operation of this ETL mapping
    -> RTable       -- ^ input RTable
    -> ETLMapping   -- ^ output ETLMapping
createLeafETLMapLD etlop rt = ETLMapLD { etlOp = etlop, tabL = ETLMapEmpty, tabR = rt}

-- | creates a Binary operation leaf node of the form:
-- @
--
-- A leaf-node -->    etlOp1    
--                    /     \
--   rtabToETLMapping rtab1  rtab2
-- @
-- 
createLeafBinETLMapLD ::
       ETLOperation -- ^ ETL operation of this ETL mapping
    -> RTable       -- ^ input RTable1
    -> RTable       -- ^ input RTable2
    -> ETLMapping   -- ^ output ETLMapping
createLeafBinETLMapLD etlop rt1 rt2 = ETLMapLD { etlOp = etlop, tabL = rtabToETLMapping rt1, tabR = rt2}

-- | Connects an ETL Mapping to a left-deep ETL Mapping tree, of the form
-- @
--     A Left-Deep ETLOperation Tree
-- 
--                              final RTable result
--                                    / 
--                                 etlOp3 
--                              /       \ 
--                           etlOp2     rtab2
--                          /      \ 
-- A leaf-node -->    etlOp1    emptyRTab
--                    /       \
--              ETLMapEmpty   rtab1
--
-- @
-- Example:
-- @
--   -- connect a Unary ETL mapping (etlOp2)
--
--                           etlOp2    
--                          /      \ 
--                       etlOp1    emptyRTab
--        
--   => connectETLMapLD etlOp2 emptyRTable prevMap
--
--   -- connect a Binary ETL Mapping (etlOp3)
--
--                                 etlOp3 
--                              /       \ 
--                           etlOp2     rtab2
--
--   => connectETLMapLD etlOp3 rtab2 prevMap
-- @
--
-- Note that the right branch (RTable) appears first in the list of input arguments of this function and 
-- the left branch (ETLMapping) appears second. This is strange, and one could thought that it is a mistake
-- (i.e., the left branch should appear first and the right branch second) since we are reading from left to right.
-- However this was a deliberate choice, so that we leave the left branch (which is the connection point with the
-- previous ETLMapping) as the last argument, and thus we can partially apply the argumenets and get a new function
-- with input parameter only the previous mapping. This is very helpfull in function composition
-- 
connectETLMapLD ::
        ETLOperation  -- ^ ETL operation of this ETL Mapping
     -> RTable        -- ^ Right RTable (right branch) (if this is a Unary ETL mapping this should be an emptyRTable) 
     -> ETLMapping    -- ^ Previous ETL mapping (left branch)        
     -> ETLMapping    -- ^ New ETL Mapping, which has added at the end the new node
connectETLMapLD etlop rt prevMap = ETLMapLD { etlOp = etlop, tabL = prevMap, tabR = rt}


-- | This operator executes an ETLmapping
etl = runETLmapping

-- | Executes an ETLMapping
runETLmapping ::
    ETLMapping  -- ^ input ETLMapping
    -> RTable   -- ^ output RTable
-- empty ETL mapping
runETLmapping ETLMapEmpty = emptyRTable
--  ETL mapping with an empty ETLOperation, which is just modelling an RTable
runETLmapping ETLMapLD { etlOp = ETLcOp{cmap = ColMapEmpty}, tabL = ETLMapEmpty, tabR = rtab } = rtab
-- leaf node --> unary ETLOperation on RTable
runETLmapping ETLMapLD { etlOp = runMe, tabL = ETLMapEmpty, tabR = rtab } = etlOpU runMe rtab {--   if (isRTabEmpty rtab) 
                                                                                                    then emptyRTable  
                                                                                                    else etlOpU runMe rtab    --}
runETLmapping ETLMapLD { etlOp = runMe, tabL = prevmap, tabR = rtab } =
        if (isRTabEmpty rtab)
        then let
                prevRtab = runETLmapping prevmap -- execute previous ETLMapping to get the resulting RTable
             in etlOpU runMe prevRtab
        else let   
                prevRtab = runETLmapping prevmap -- execute previous ETLMapping to get the resulting RTable
             in etlOpB runMe prevRtab rtab

-- | This operator executes an ETLMapping and returns the RTabResult  Writer Monad
-- that embedds apart from the resulting RTable, also the number of RTuples returned
etlRes ::
       ETLMapping  -- ^ input ETLMapping
    -> RTabResult   -- ^ output RTabResult
etlRes etlm = 
    let resultRtab = etl etlm
        returnedRtups = rtuplesRet $ V.length resultRtab
    in rtabResult (resultRtab, returnedRtups)

-- | Model an RTable as an ETLMapping which when executed will return the input RTable
rtabToETLMapping ::
       RTable
    -> ETLMapping 
rtabToETLMapping rt =   if (isRTabEmpty rt)
                        then ETLMapEmpty
                        else ETLMapLD { etlOp = ETLcOp {cmap = ColMapEmpty}, tabL = ETLMapEmpty, tabR = rt }

--
--   An ETLMapping is implemented as a series of ETLOperations conected with the :=> operator which is right associative
--  i.e., ETLOp3 :=> ETLOp2 :=> ETLOp1 RTable is  (ETLOp3 :=> (ETLOp2 :=> (ETLOp1 RTable))
--  infixr 9 :=> 
-- data ETLMapping = EmptyETLop RTable | ETLMapping :=> ETLOperation


{--
-- | Executes an ETL mapping 
--   Note htat the source RTables are "embedded" in the data constructors of the ETLMapping data type.
runETLmapping ::
    ETLMapping   -- ^ input ETLMapping
    -> RTable    -- output RTable
runETLmapping EmptyETLop rtab = rtab
runETLmapping etlMap :=> etlOp = runETLmapping etlMap 
    
    case etlOp of
        ETLrOp {rop = relOp} -> runROperation relOp
--}

-- Example of an ETLMapping((<T> TabColTransformation).(<F> RPredicate).(<T> TabTransformation) rtable1) `(<EJ> RJoinPredicate)` rtable2


-- ##################################################
-- *  Various useful RDataType Transformations 
-- *  and pre-cooked Column Mappings
-- ##################################################

-- | rtdappend : Concatenates two Text RDataTypes, in all other cases of RDataType it returns Null.
rdtappend :: 
    RDataType 
    -> RDataType
    -> RDataType
rdtappend (RText t1) (RText t2) = RText (t1 `T.append` t2)
rdtappend _ _ = Null


-- | Returns an ETL Operation that adds a surrogate key (SK) column to an RTable and
-- fills each row with a SK value.
addSurrogateKey :: Integral a =>    
       ColumnName    -- ^ The name of the surrogate key column
    -- -> Integer       -- ^ The initial value of the Surrogate Key will be the value of this parameter    
    -> a             -- ^ The initial value of the Surrogate Key will be the value of this parameter    
    -> ETLOperation  -- ^ Output ETL operation which encapsulates the add surrogate key column mapping
addSurrogateKey cname initVal  =   
    let combOp = RCombinedOp { rcombOp = updateSKvalue . (addColumn cname (RInt (fromIntegral initVal)))  }
            where
                -- updateSKvalue :: RTable -> RTable
                -- updateSKvalue rt = V.foldr' ( \rtup trgRtab ->  let 
                --                                                 newTuple = updateRTuple cname (rtup<!>cname + RInt 1) rtup
                --                                         in appendRTuple newTuple trgRtab 
                --                     ) emptyRTable rt
                updateSKvalue :: RTable -> RTable
                updateSKvalue rt =
                        let     indexedRTab = V.indexed rt -- this returns a: Vector (Int, RTuple) (pairs each RTuple with its index in the RTable)
                        in V.zipWith (\tupsrc (val, _) -> upsertRTuple cname (RInt (fromIntegral initVal) + RInt (fromIntegral val)) tupsrc ) rt indexedRTab
    in ETLrOp combOp 

-- | Returns an UnaryRTableOperation (RTable -> RTable) that adds a surrogate key (SK) column to an RTable and
-- fills each row with a SK value. It  primarily  is intended to be used within a Julius expression
addSurrogateKeyJ :: Integral a =>    
       ColumnName    -- ^ The name of the surrogate key column
    -- -> Integer       -- ^ The initial value of the Surrogate Key will be the value of this parameter    
    -> a             -- ^ The initial value of the Surrogate Key will be the value of this parameter    
    -> RTable        -- ^ Input RTable
    -> RTable        -- ^ Output RTable
addSurrogateKeyJ cname initVal  =   
    updateSKvalue . (addColumn cname (RInt (fromIntegral initVal))) 
            where
                -- updateSKvalue :: RTable -> RTable
                -- updateSKvalue rt = V.foldr' ( \rtup trgRtab ->  let 
                --                                                 newTuple = updateRTuple cname (rtup<!>cname + RInt 1) rtup
                --                                         in appendRTuple newTuple trgRtab 
                --                     ) emptyRTable rt
                updateSKvalue :: RTable -> RTable
                updateSKvalue rt =
                        let     indexedRTab = V.indexed rt -- this returns a: Vector (Int, RTuple) (pairs each RTuple with its index in the RTable)
                        in V.zipWith (\tupsrc (val, _) -> upsertRTuple cname (RInt (fromIntegral initVal) + RInt (fromIntegral val)) tupsrc ) rt indexedRTab


-- | Returns an ETL Operation that Appends an RTable to a target RTable 
appendRTable ::
        ETLOperation  -- ^ Output ETL Operation
appendRTable  = 
    let binOp = RBinOp { rbinOp = flip (V.++) } -- we need to flip the input parameters so that when we embed this operation in an ETL mapping
                                                -- then the delta table will be appended to the target table.
    in ETLrOp binOp


-- | Returns a BinaryRTableOperation (RTable -> RTable -> RTable) that Appends an RTable to a target RTable.
-- It is primarily intended to be used within a Julius expression. For example:
-- @
--   GenBinaryOp (TabL rtab1) (Tab $ rtab2) $ ByBinaryOp RTE.appendRTableJ
-- @
--
appendRTableJ ::
            RTable -- ^ Target RTable
        ->  RTable -- ^ Input RTable
        ->  RTable -- ^ Output RTable 
appendRTableJ  = (V.++) 


-- | Returns an ETL Operation that adds a surrogate key column to an RTable
-- The first argument is the initial value of the surrogate key. If Nothing is given, then
-- the initial value will be 0.    
-- addSurrogateKey_old :: Integral a =>    
--        Maybe a       -- ^ The initial value of the Surrogate Key will be the value of this parameter + 1
--     -> a            -- ^  Number of rows that the Surrogate Key will be assigned
--     -> ColumnName    -- ^ The name of the surrogate key column
--     -> ETLOperation  -- ^ Output ETL operation which encapsulates the add surrogate key column mapping
-- addSurrogateKey_old init 0 cname = 
--     let initVal = case init of 
--             Just x -> x
--             Nothing -> 0
--         cmap = RMap1x1 {   
--                                 srcCol = "", removeSrcCol = No   -- the source column can be any column in this mapping, even ""
--                                 ,trgCol = cname
--                                 ,transform1x1 = \_ -> RInt (fromIntegral initVal)
--                                 ,srcRTupleFilter = \_ -> True   
--                        }
--     in ETLcOp cmap
-- addSurrogateKey_old init numRows cname = 
--     let initVal = case init of 
--             Just x -> x
--             Nothing -> 0
--         cmap = RMap1x1 {   
--                                 srcCol = "", removeSrcCol = No   -- the source column can be any column in this mapping, even ""
--                                 ,trgCol = cname
--                                 ,transform1x1 = \_ -> RInt (fromIntegral initVal + 1)
--                                 ,srcRTupleFilter = \_ -> True   
--                        }
--     in addSurrogateKey_old (Just (initVal + 1)) (numRows - 1) cname



