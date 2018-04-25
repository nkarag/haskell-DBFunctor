{-|
Module      : CSVdb.Base
Description : Implements basic CSV data types and core processing functionality
Copyright   : (c) Nikos Karagiannidis, 2017
                  
License     : GPL-3
Maintainer  : nkarag@gmail.com
Stability   : experimental
Portability : POSIX

This offers functions for decoding a CSV file into an RTable and encoding an RTable into a CSV file.
After a CSV is decoded to an RTable, then all operations implemented on an RTable are applicable.



**** NOTE ****
Needs the following ghc option to compile from Cygwin (default is 100) :  
    stack build --ghc-options  -fsimpl-tick-factor=200

-}

{-# LANGUAGE OverloadedStrings #-}
-- :set -XOverloadedStrings
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE NoMonomorphismRestriction #-}


module RTable.Data.CSV
   (
        CSV 
        ,Row
        ,Column                 
        ,readCSV
        ,readCSVwithOptions
        ,readCSVFile
        ,writeCSV
        ,writeCSVFile
        ,printCSV
        ,printCSVFile
        ,copyCSV
        ,selectNrows
        ,csvToRTable
        ,rtableToCSV
    -- export the following only for debugging purposes via GHCI    
        --,csv2rtable        
        --,rtable2csv
        ,csvHeaderFromRtable
        ,projectByIndex
        ,headCSV
        ,tailCSV
        ,CSVOptions(..)
        ,YesNo (..)
    ) where
{--        ,addColumn
        ,dropColumn
        ,updateColumn
        ,filterRows
        ,appendRow
        ,deleteRow
        ,joinCSV  --}


--------------
--
--  NOTE:
--      issue command: stack list-dependencies
--      in order to see the exact versions of the packages the CSVdb depends on
--------------

import Debug.Trace

import RTable.Core

-- CSV-conduit
--import qualified Data.CSV.Conduit as CC           -- http://hackage.haskell.org/package/csv-conduit  ,  https://www.stackage.org/haddock/lts-6.27/csv-conduit-0.6.6/Data-CSV-Conduit.html

-- Cassava  (CSV parsing Library)
    --  https://github.com/hvr/cassava
    --  https://www.stackbuilders.com/tutorials/haskell/csv-encoding-decoding/  
    --  https://www.stackage.org/lts-7.15/package/cassava-0.4.5.1
    --  https://hackage.haskell.org/package/cassava-0.4.5.1/docs/Data-Csv.html
import qualified Data.Csv as CV


-- HashMap                          -- https://hackage.haskell.org/package/unordered-containers-0.2.7.2/docs/Data-HashMap-Strict.html
import qualified Data.HashMap.Strict as HM

-- Data.List
import Data.List (map)

-- ByteString
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as BS
import Data.ByteString.Char8 (pack,unpack) -- as BSW --(pack)
import Prelude hiding (putStr)
import  Data.ByteString.Lazy.Char8 (putStr)--as BLW

-- Text
import Data.Text as T
import Data.Text.Encoding (decodeUtf8, encodeUtf8, decodeUtf8', decodeUtf16LE)

-- Vector
import qualified Data.Vector as V 

-- Data.Maybe
import Data.Maybe (fromJust)

-- Data.Serialize (Cereal package)  
--                                  https://hackage.haskell.org/package/cereal
--                                  https://hackage.haskell.org/package/cereal-0.5.4.0/docs/Data-Serialize.html
--                                  http://stackoverflow.com/questions/2283119/how-to-convert-a-integer-to-a-bytestring-in-haskell
import Data.Serialize (decode, encode)

-- Typepable                        -- https://hackage.haskell.org/package/base-4.9.1.0/docs/Data-Typeable.html
                                    -- http://stackoverflow.com/questions/6600380/what-is-haskells-data-typeable
                                    -- http://alvinalexander.com/source-code/haskell/how-determine-type-object-haskell-program
import qualified Data.Typeable as TB --(typeOf, Typeable)

import Data.Either.Combinators (fromRight')

import Data.Char (ord)

import Text.Printf (printf)

{--  

-- Example code from: https://github.com/hvr/cassava  

{--
Sample CSV:

name,salary
John Doe,50000
Jane Doe,60000
    
--} 

data Person = Person
    { name   :: !String
    , salary :: !Int
    }

instance FromNamedRecord Person where
    parseNamedRecord r = Person <$> r .: "name" <*> r .: "salary"

main :: IO ()
main = do
    csvData <- BL.readFile "salaries.csv"
    case decodeByName csvData of
        Left err -> putStrLn err
        Right (_, v) -> V.forM_ v $ \ p ->
            putStrLn $ name p ++ " earns " ++ show (salary p) ++ " dollars"
--}

-- ##################################################
-- *  Data Types
-- ##################################################

-- | Definition of a CSV file
-- Treating CSV data as opaque byte strings 
-- (type Csv = Vector Record)
type CSV = V.Vector Row -- i.e., CV.Csv

-- | Definition of a CSV Row
-- Essentially a Row is just a Vector of ByteString
-- (type Record = Vector Field)
type Row = V.Vector Column -- i.e., CV.Record

-- | Definition of a CSV Record column
-- (type Field = ByteString)
type Column = CV.Field   

-- This typeclass instance is required by CV.decodeByName
--instance CV.FromNamedRecord  (V.Vector BS.ByteString)

-- ##################################################
-- *  IO operations
-- ##################################################

-- | readCSVFile: reads a CSV file and returns a lazy bytestring
readCSVFile ::
    FilePath  -- ^ the CSV file
    -> IO BL.ByteString  -- ^ the output CSV
readCSVFile f = BL.readFile f     


-- | readCSV: reads a CSV file and returns a CSV data type (Treating CSV data as opaque byte strings)
readCSV ::
    FilePath  -- ^ the CSV file
    -> IO CSV  -- ^ the output CSV type
readCSV f = do
    csvData <- BL.readFile f     
{-    csvDataBS <- BL.readFile f     
    let 
        --decodeUtf8' :: ByteString -> Either UnicodeException Text
        utf8text = case decodeUtf8' (BL.toStrict csvDataBS) of
            Left exc -> error $ "Error in decodeUtf8' the whole ByteString from Data.ByteString.Lazy.readFile: " ++ (show exc)
            Right t  -> t
        -- Note that I had to make sure to use encodeUtf8 on a literal of type Text rather than just using a ByteString literal directly to Cassava
        -- because The IsString instance for ByteStrings, which is what's used to convert the literal to a ByteString, truncates each Unicode code point
        -- see : https://stackoverflow.com/questions/26499831/parse-csv-tsv-file-in-haskell-unicode-characters
        csvData = encodeUtf8 utf8text  -- encodeUtf8 :: Text -> ByteString
-}
    let
        csvResult = -- fromRight' $ CV.decode CV.HasHeader csvData
            case CV.decode CV.HasHeader csvData of
                Left str -> error $ "Error in decoding CSV file " ++ f ++ ": " ++ str
                Right res -> res
        {--
        case CV.decode CV.HasHeader csvData of   --CV.decodeByName csvData of
                Left err -> let errbs = encode (err::String) -- BL.pack err  -- convert String to ByteString
                                record = V.singleton (errbs)
                                csv = V.singleton (record)
                            in csv
                Right csv -> csv            --Right (hdr, csv) -> csv
        --}        
    return csvResult

data YesNo = Yes | No

data CSVOptions = CSVOptions {
        delimiter :: Char
        ,hasHeader :: YesNo
}

-- | readCSVwithOptions: reads a CSV file based on input options (delimiter and header option) and returns a CSV data type (Treating CSV data as opaque byte strings)
readCSVwithOptions ::
        CSVOptions 
    ->  FilePath  -- ^ the CSV file
    ->  IO CSV  -- ^ the output CSV type
readCSVwithOptions opt f = do
    csvData <- BL.readFile f     
    let csvoptions = CV.defaultDecodeOptions {
                                            CV.decDelimiter = fromIntegral $ ord (delimiter opt)
                     }
        csvResult = case CV.decodeWith  csvoptions 

                                        (case (hasHeader opt) of 
                                            Yes -> CV.HasHeader
                                            No  -> CV.NoHeader)

                                        csvData  of
                                            Left str -> error $ "Error in decoding CSV file " ++ f ++ ": " ++ str
                                            Right res -> res

{-        csvResult = fromRight' $ 
                        CV.decodeWith   csvoptions 

                                        (case (hasHeader opt) of 
                                            Yes -> CV.HasHeader
                                            No  -> CV.NoHeader)

                                        csvData
-}
    return csvResult



-- | writeCSVFile: write a CSV (bytestring) to a newly created csv file
writeCSVFile ::
       FilePath  -- ^ the csv file to be created
    -> BL.ByteString       -- ^  input CSV
    -> IO()
writeCSVFile f csv =  BL.writeFile f csv


-- | writeCSV: write a CSV to a newly created csv file
writeCSV ::
       FilePath  -- ^ the csv file to be created
    -> CSV       -- ^  input CSV
    -> IO()
writeCSV f csv = do
    let csvBS = CV.encode (V.toList csv)
    BL.writeFile f csvBS


-- | printCSVFile: print input CSV on screen
printCSVFile ::
    BL.ByteString     -- ^ input CSV to be printed on screen
    -> IO()
printCSVFile csv = putStr csv


-- | printCSV: print input CSV on screen
printCSV ::
    CSV     -- ^ input CSV to be printed on screen
    -> IO()
printCSV csv = do
    -- convert each ByteString field to Text
    {--let csvText = V.map (\r -> V.map (decodeUtf32LE) r) csv
    let csvBS = CV.encode (V.toList csvText)--}
    let csvBS = CV.encode (V.toList csv)
    putStr csvBS

-- | copyCSV: copy input csv file to specified output csv file
copyCSV ::
      FilePath  -- ^ input csv file
    ->FilePath  -- ^ output csv file
    -> IO()
copyCSV fi fo = do
    csv <- readCSV fi
    writeCSV fo csv 

-- ##################################################
-- *  CSV to RTable integration
-- ##################################################

-- | csvToRTable: Creates an RTable from a CSV and a set of RTable Metadata.
-- The RTable metadata essentially defines the data type of each column so as to
-- call the appropriate data constructor of RDataType and turn the ByteString values of CSV to RDataTypes values of RTable
-- We assume that the order of the columns in the CSV is identical with the order of the columns in the RTable metadata
csvToRTable :: 
    RTableMData 
    -> CSV 
    -> RTable
csvToRTable m c = 
    V.map (row2RTuple m) c
    where
        row2RTuple :: RTableMData -> Row -> RTuple
        row2RTuple md row =             
            let 
                -- create a list of ColumnInfo. The order of the list correpsonds to the fixed column  order and it is identical to the CSV column order
                listOfColInfo = toListColumnInfo (rtuplemdata md) --Prelude.map (snd) $ (rtuplemdata md) -- HM.toList (rtuplemdata md)
                -- create a list of the form [(ColumnInfo, Column)]
                listOfColInfoColumn = Prelude.zip listOfColInfo (V.toList row)  
                -- create a list of ColumnNames
                listOfColNames   =  toListColumnName (rtuplemdata md) --Prelude.map (fst) $ (rtuplemdata md) --HM.toList (rtuplemdata md)
                -- create a list of RDataTypes
                listOfRDataTypes = Prelude.map (\(ci,co) -> column2RDataType ci co) $ listOfColInfoColumn
                    where
                        column2RDataType :: ColumnInfo -> Column -> RDataType
                        column2RDataType ci col =    
                            if col == BS.empty
                                then  -- this is an empty ByteString
                                    Null
                                else
                                    -- Data.ByteString.Char8.unpack :: ByteString -> [Char] 
                                    case (dtype ci) of
                                        Integer     -> RInt (val::Integer) -- (read (Data.ByteString.Char8.unpack col) :: Int)   --((read $ show val) :: Int)
                                        Varchar     -> RText $ if False then trace ("Creating RText for column " ++ (name ci)) $ (val::T.Text) else (val::T.Text)                                                                                                                        
                                        Date fmt    -> RDate {   rdate = (val::T.Text) {-decodeUtf8 col-} , dtformat = fmt } -- (val::T.Text)
                                                                 --getDateFormat (val::String)}
                                        Timestamp fmt -> RTime $ createRTimeStamp fmt (Data.ByteString.Char8.unpack col)  -- Data.ByteString.Char8.unpack :: ByteString -> [Char] 
                                        Double      -> RDouble (val::Double) --(read (Data.ByteString.Char8.unpack col) :: Double)  -- ((read $ show val) :: Double)
                                    where
                                    
                                        -- Use Data.Serialize for the decoding from ByteString to a known data type
                                        -- decode :: Serialize a => ByteString -> Either String a
                                        -- val = fromRight' (decode col) 
                                        {--
                                        val = case decode col of
                                                    Left  e  -> e -- you should throw an exception here!
                                                    Right v -> v
                                        --}
                                    
                                        -- use Data.Csv parsing capabilities in order to turn a Column (i.e. a Field, i.e., a ByteString)
                                        -- into a known data type.
                                        -- For this reason we are going to use : CV.parseField :: Field -> Parser a                                    
                                        --val = fromRight' $ CV.runParser $ CV.parseField col
                                        val = case CV.runParser $ CV.parseField col of
                                            Left str -> error $ "Error in parsing column " ++ (name ci) ++ ":" ++ str
                                            Right v -> v

                                        {--
                                        val = case CV.runParser $ CV.parseField col of 
                                                    Left  e  -> e -- you should throw an exception here!
                                                    Right v -> v
                                        --}
                                        {--
                                        getDateFormat :: String -> String
                                        getDateFormat _ =  "DD/MM/YYYY"-- parse and return date format                                    
                                        --}
            in HM.fromList $ Prelude.zip listOfColNames listOfRDataTypes


-- | rtableToCSV : Retunrs a CSV from an RTable
-- The first line of the CSV will be the header line, taken from the RTable metadata.
-- Note that the driver for creating the output CSV file is the input RTableMData descrbing the columns and RDataTypes of each RTuple.
-- This means, that if the RTableMData include a subset of the actual columns of the input RTable, then no eror will occure and the
-- output CSV will include only this subset.
-- In the same token, if in the RTableMData there is a column name that is not present in the input RTable, then an error will occur.
rtableToCSV ::
        RTableMData -- ^ input RTable metadata describing the RTable
        -> RTable   -- ^ input RTable
        -> CSV      -- ^ output CSV
rtableToCSV m t =       
    (createCSVHeader m) V.++ (V.map (rtuple2row m) t)
    where
        rtuple2row :: RTableMData -> RTuple -> Row 
        rtuple2row md rt =
            -- check that the RTuple is not empty. Otherwise the HM.! operator will cause an exception
            if not $ isRTupEmpty rt
            then
                let listOfColInfo = toListColumnInfo (rtuplemdata md)  --Prelude.map (snd) $ (rtuplemdata md) --HM.toList (rtuplemdata md)
                    
                    -- create a list of the form [(ColumnInfo, RDataType)] 
                        -- Prelude.zip listOfColInfo (Prelude.map (snd) $ HM.toList rt)  -- this code does NOT guarantee that HM.toList will return the same column order as [ColumnInfo]
                    listOfColInfoRDataType :: [ColumnInfo] -> RTuple -> [(ColumnInfo, RDataType)]  -- this code does guarantees that RDataTypes will be in the same column order as [ColumnInfo], i.e., the correct RDataType for the correct column
                    listOfColInfoRDataType (ci:[]) rtup = [(ci, rtup HM.!(name ci))]  -- rt HM.!(name ci) -> this returns the RDataType by column name
                    listOfColInfoRDataType (ci:colInfos) rtup = (ci, rtup HM.!(name ci)):listOfColInfoRDataType colInfos rtup
                    
                    listOfColumns = Prelude.map (\(ci,rdt) -> rDataType2Column ci rdt) $ listOfColInfoRDataType listOfColInfo rt
                        where
                            rDataType2Column :: ColumnInfo -> RDataType -> Column
                            rDataType2Column _ rdt = 
                                {--
                                -- encode :: Serialize a => a -> ByteString
                                case rdt of
                                    RInt i                          -> encode i
                                    RText t                         -> encodeUtf8 t -- encodeUtf8 :: Text -> ByteString
                                    RDate {rdate = d, dtformat = f} -> encode d 
                                    RDouble db                      -> encode db
                                --}
                                -- toField :: a -> Field (from Data.Csv)
                                case rdt of
                                    RInt i                          -> CV.toField i
                                    RText t                         -> CV.toField t 
                                    RDate {rdate = d, dtformat = f} -> CV.toField d 
                                    RDouble db                      -> CV.toField  ((printf "%.2f" db)::String)
                                    RTime { rtime = RTimestampVal {year = y, month = m, day = d, hours24 = h, minutes = mi, seconds = s} } ->  let  timeText = (digitToText d) `mappend` T.pack "/" `mappend` (digitToText m) `mappend` T.pack "/" `mappend` (digitToText y) `mappend` T.pack " " `mappend`  (digitToText h) `mappend` T.pack ":"  `mappend` (digitToText mi) `mappend` T.pack ":" `mappend` (digitToText s) 
                                                                                                                                                              -- T.pack . removeQuotes . T.unpack $ (showText d) `mappend` T.pack "/" `mappend` (showText m) `mappend` T.pack "/" `mappend` (showText y) `mappend` T.pack " " `mappend`  (showText h) `mappend` T.pack ":"  `mappend` (showText mi) `mappend` T.pack ":" `mappend` (showText s)
                                                                                                                                                                    -- removeQuotes $ (show d) ++ "/" ++ (show m) ++ "/" ++ (show y) ++ " " ++  (show h) ++ ":" ++ (show mi) ++ ":" ++ (show s)
                                                                                                                                                        where  digitToText :: Int -> T.Text
                                                                                                                                                               digitToText d =
                                                                                                                                                                    if d > 9 then showText d
                                                                                                                                                                    else "0" `mappend` (showText d)

                                                                                                                                                               showText :: Show a => a -> Text
                                                                                                                                                               showText = T.pack . show
                                                                                                                                                                                                                                                                                                                                  
                                                                                                                                                               -- removeQuotes ('"' : [] ) = ""
                                                                                                                                                               -- removeQuotes ('"' : xs ) = removeQuotes xs
                                                                                                                                                               -- removeQuotes ( x : xs )   = x:removeQuotes xs
                                                                                                                                                               -- removeQuotes _ = ""
                                                                                                                                                    --noQuotesText = fromJust $ T.stripSuffix "\"" (fromJust $ T.stripPrefix "\"" timeText)
                                                                                                                                                in CV.toField timeText --noQuotesText 
                                                                                                                                                -- CV.toField $ (show d) ++ "/" ++ (show m) ++ "/" ++ (show y) ++ " " ++  (show h) ++ ":" ++ (show mi) ++ ":" ++ (show s)
                                    Null                             ->   CV.toField (""::T.Text)                                                                                                             
                in V.fromList $ listOfColumns
            else V.empty::Row
        createCSVHeader :: RTableMData -> CSV
        createCSVHeader md =
            let listOfColNames = toListColumnName (rtuplemdata md) --Prelude.map (fst) $ (rtuplemdata md) --HM.toList (rtuplemdata md)
                listOfByteStrings = Prelude.map (\n -> CV.toField n) listOfColNames  
                headerRow = V.fromList listOfByteStrings
            in  V.singleton headerRow


-- In order to be able to decode a CSV bytestring into an RTuple,
-- we need to make Rtuple an instance of the FromNamedRecord typeclass and
-- implement the parseNamesRecord function. But this is not necessary, since there is already an instance for CV.FromNamedRecord (HM.HashMap a b), which is the same,
-- since an RTuple is a HashMap. 
--
--      type RTuple = HM.HashMap ColumnName RDataType
--      type ColumnName = String
--      data RDataType = 
--              RInt { rint :: Int }
--            | RChar { rchar :: Char }
--            | RText { rtext :: T.Text }
--            | RString {rstring :: [Char]}
--            | RDate { 
--                        rdate :: String
--                       ,dtformat :: String  -- ^ e.g., "DD/MM/YYYY"
--                    }
--            | RDouble { rdouble :: Double }
--            | RFloat  { rfloat :: Float }
--            | Null
--            deriving (Show, Eq)
--
--
--      parseNamedRecord :: NamedRecord -> Parser a
--      type NamedRecord = HashMap ByteString ByteString
--
--      Instance of class FromNamedRecord:
--      (Eq a, FromField a, FromField b, Hashable a) => FromNamedRecord (HashMap a b)
--
-- From this we understand that we need to make RDataType (which is "b" in HashMap a b) an instance of FormField ((CV.FromField RDataType)) by implementing parseField
-- where:
-- @
--              parseField :: Field -> Parser a
--              type Field = ByteString
-- @
{--instance CV.FromNamedRecord RTuple where
  parseNamedRecord r = do
        let listOfcolNames = map (fst) $ HM.toList r -- get the first element of each pair which is the name of the column (list of ByteStrings)
            listOfParserValues = map (\c -> r CV..: c) listOfcolNames   --  this retuns a list of the form [Parser RDataType]
            listOfValues = map (\v -> right (CV.runParser v)) listOfParserValues     -- this returns a list of the form [RDataType]
            rtup = createRtuple $ zip listOfcolNames listOfValues
        return rtup
--}


instance CV.FromField RDataType where
  parseField dt = do
        -- dt is a ByteString (i.e., a Field) representing some value that we have read from the CSV file (we dont know its type)
        -- we need to construct an RDataType from this value and then wrap it into a Parser Monad and return it
        --
        
        -- ### Note: the following line does not work ###
        -- 1. parse the input ByteString using Cassavas' parsing capabilities for known data types
--        val <-  CV.parseField dt    

        -- 1. We dont know the type of dt. OK lets wrap it into a generic type, that of Data.Typeable.TypeRep
        let valTypeRep = TB.typeOf dt

        -- 2. wrap this value into a RDataType
        let rdata =  createRDataType  valTypeRep --val

        -- wrap the RDataType into a Parser Monad and return it
        pure rdata


{--
    -- #### NOTE ###
    --  
    --  if the following does not work (val is always a String, then try to use Data.Serialize.decode instead in order to get the original value from a bytestring)

    -- get the value inside the Parser Monad (FromField has instances from all common haskell data types)
       let val = case CV.runParser (CV.parseField dt) of    -- runParser :: Parser a -> Either String a
            Left e ->  e
            Right v -> v 
--}
    -- Lets try to use Data.Serialize.decode to get the vlaue from the bytestring  (decode :: Serialize a => ByteString -> Either String a)
{--       let val = case decode dt of
                Left e -> e 
                Right v -> v
    
        -- wrap this value into a RDataType
       let rdata =  createRDataType val
        -- wrap the RDataType into a Parser Monad
       pure rdata
--}

-- In order to encode an input RTable into a CSV bytestring 
-- we need to make Rtuple an instance of the ToNamedRecord typeclass and
-- implement the toNamedRecord function. 
-- Where:
-- @
--              toNamedRecord :: a -> NamedRecord
--              type NamedRecord = HashMap ByteString ByteString
--
--              namedRecord :: [(ByteString, ByteString)] -> NamedRecord
--                  Construct a named record from a list of name-value ByteString pairs. Use .= to construct such a pair from a name and a value.
--
--              (.=) :: ToField a => ByteString -> a -> (ByteString, ByteString)
-- @
-- In our case, we dont need to do this because an RTuple is just a synonym for HM.HashMap ColumnName RDataType and the data type HashMap a b is
-- already an instance of ToNamedRecord.
--
-- Also we need to make RDataType an instance of ToField ((CV.ToField RDataType)) by implementing toField, so as to be able
-- to convert an RDataType into a ByteString
-- where:
-- @
--              toField :: a -> Field
--              type Field = ByteString
-- @
instance CV.ToField RDataType where
    toField rdata = case rdata of
            RInt i      -> encode (i::Integer)
            --RChar c     -> encode (c::Char)
            -- RText t     -> encode (t::String) 
            RText t     -> encodeUtf8 t -- encodeUtf8 :: Text -> ByteString
            --RString s   -> encode (s::String)
            --RFloat f    -> encode (f::Float)
            RDouble d   -> encode (d::Double)
            Null        -> encode (""::String)
            RDate d f   -> encodeUtf8 d -- encode (d::String)

{--instance CV.ToField RDataType where
    toField rdata = case rdata of
            (i::Int)    -> encode (i::Int)
            -- RText t     -> encode (t::String) 
            (t::T.Text)     -> encodeUtf8 t -- encodeUtf8 :: Text -> ByteString
            (d::Double)   -> encode (d::Double)
            Null        -> encode (""::String)
--}

-- | csv2rtable : turn a input CSV to an RTable.
-- The input CSV will be a ByteString. We assume that the first line is the CSV header,
-- including the Column Names. The RTable that will be created will have as column names the headers appearing
-- in the first line of the CSV.
-- Internally we use CV.decodeByName to achieve this decoding
-- where:
-- @
--      decodeByName
--        :: FromNamedRecord a     
--        => ByteString   
--        -> Either String (Header, Vector a)  
-- @
-- Essentially, decodeByName will return a  @Vector RTuples@
--
-- In order to be able to decode a CSV bytestring into an RTuple,
-- we need to make Rtuple an instance of the FromNamesRecrd typeclass and
-- implement the parseNamesRecord function. But this is not necessary, since there is already an instance for CV.FromNamedRecord (HM.HashMap a b), which is the same,
-- since an RTuple is a HashMap.
-- Also we need to make RDataType an instance of FormField ((CV.FromField RDataType)) by implementing parseField
-- where:
-- @
--              parseField :: Field -> Parser a
--              type Field = ByteString
-- @
-- See RTable module for these instance
csv2rtable :: 
       BL.ByteString  -- ^ input CSV (we asume that this CSV has a header in the 1st line)
    -> RTable         -- ^ output RTable
csv2rtable csv = 
    case CV.decodeByName csv of
        Left e -> emptyRTable
        Right (h, v) -> v


-- | rtable2csv: encode an RTable into a CSV bytestring
-- The first line of the CSV will be the header, which compirses of the column names.
--
-- Internally we use CV.encodeByName to achieve this decoding
-- where:
-- @
--      encodeByName :: ToNamedRecord a => Header -> [a] -> ByteString 
--          Efficiently serialize CSV records as a lazy ByteString. The header is written before any records and dictates the field order.
--
--      type Header = Vector Name
--      type Name = ByteString
-- @
--
-- In order to encode an input RTable into a CSV bytestring 
-- we need to make Rtuple an instance of the ToNamedRecord typeclass and
-- implement the toNamedRecord function. 
-- Where:
-- @
--              toNamedRecord :: a -> NamedRecord
--              type NamedRecord = HashMap ByteString ByteString
--
--              namedRecord :: [(ByteString, ByteString)] -> NamedRecord
--                  Construct a named record from a list of name-value ByteString pairs. Use .= to construct such a pair from a name and a value.
--
--              (.=) :: ToField a => ByteString -> a -> (ByteString, ByteString)
-- @
-- In our case, we dont need to do this because an RTuple is just a synonym for HM.HashMap ColumnName RDataType and the data type HashMap a b is
-- already an instance of ToNamedRecord.
--
-- Also we need to make RDataType an instance of ToField ((CV.ToField RDataType)) by implementing toField, so as to be able
-- to convert an RDataType into a ByteString
-- where:
-- @
--              toField :: a -> Field
--              type Field = ByteString
-- @
-- See 'RTable' module for these instance
rtable2csv ::
       RTable           -- ^ input RTable
    -> BL.ByteString    -- ^ Output ByteString
rtable2csv rtab = 
    CV.encodeByName (csvHeaderFromRtable rtab) (V.toList rtab)

-- | csvHeaderFromRtable: creates a Header (as defined in Data.Csv) from an RTable
--      type Header = Vector Name
--      type Name = ByteString
csvHeaderFromRtable ::
                 RTable
              -> CV.Header
csvHeaderFromRtable rtab = 
            let fstRTuple = V.head rtab  -- just get any tuple, e.g., the 1st one
                colList = HM.keys fstRTuple -- get a list of the columnNames ([ColumnName])
                colListPacked = Prelude.map (encode) colList  -- turn it into a list of ByteStrings ([ByteString])
                header = V.fromList colListPacked
            in header
-- ##################################################
-- *  Vector oprtations on CSV
-- ##################################################

-- | O(1) First row
headCSV :: CSV -> Row
headCSV = V.head

-- | O(1) Yield all but the first row without copying. The CSV may not be empty.
tailCSV :: CSV -> CSV
tailCSV = V.tail


-- ##################################################
-- *  DDL on CSV
-- ##################################################


-- ##################################################
-- *  DML on CSV
-- ##################################################


-- ##################################################
-- *  Filter, Join, Projection
-- ##################################################

-- | selectNrows: Returns  the first N rows from a CSV file
selectNrows::
       Int             -- ^ Number of rows to select
    -> BL.ByteString   -- ^ Input csv 
    -> BL.ByteString   -- ^ Output csv
selectNrows n csvi = 
    let rtabi = csv2rtable csvi
        rtabo = restrictNrows n rtabi
    in rtable2csv rtabo

-- | Column projection on an input CSV file where 
-- desired columns are defined by position (index)
-- in the CSV.
projectByIndex :: 
             [Int]  -- ^ input list of column indexes
          -> CSV    -- ^ input csv
          -> CSV    -- ^ output CSV
projectByIndex inds icsv = 
    V.foldr (prj) V.empty icsv
    where
        prj :: Row -> CSV -> CSV
        prj row acc = 
            let 
                -- construct new row including only projected columns
                newrow = V.fromList $ Data.List.map (\i -> row V.! i) inds
            in -- add new row in result vector
                V.snoc acc newrow
