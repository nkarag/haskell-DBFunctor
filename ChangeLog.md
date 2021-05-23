# Changelog for DBFunctor

### 0.1.0.0
 - Initial Version. Includes a full-working version of 
	 - Julius: A type-level Embedded Domain Specific (EDSL) Language for ETL
	 - all common Relational Algebra operations, 
	 - the ETL Mapping and other typical ETL constructs and operations
	 - operations applicable to all kinds of tabular data
	 - In-memory, database-less data processing.
	 
### 0.1.1.0
 - Includes various enhancements (most notable is DML operations support) and fixes 
	 - Issue #1: Implemented agg function string_agg (listagg in Oracle) and the corresponding Julius clause
	 - Issue #2: Implemented Julius Aggregate clauses: CountDist and CountStar
	 - Issue #6 DML Enhancements
	 	- Implement Update Julius Clause
	 	- Implement Insert Operation and corresponding Julius Clause (both single RTuple INSERT and INSERT INTO SELECT) 
	 	- Implement Merge/Upsert operation and corresponding Julius clause
	 	- Implement semi-join operation and corresponding Julius clause
	 	- Implement anti-join operation and corresponding Julius clause
	 	- Implement Delete operation and corresponding Julius clause
	 - Issue #5 : Add support for UTCTime
	 - Solve the CSV orphan instances problem by defining CSV with newtype
	 - Fix problem with order by. I have noticed the following bug:
```Haskell
								>>> let t1 = RDate {rdate = "01/12/1990", dtformat = "DD/MM/YYYY"}
								>>> let t2 = RDate {rdate = "1/12/1991", dtformat = "DD/MM/YYYY"}
								>>>	compare t1 t2
								>>>	EQ
```
Fix: 
- Redefine the RDataType Ord instance based on the compare function instead of the (<=) function.
- When comparing RDate types, convert them first to RTimeStamps and then compare these ones
- The previous point apply it also to the Eq instance for RDataType.

### 0.1.1.1
- bumped a version number (the least significant) in order to upload on hackage a new version with correct github links

### 0.1.2.0
- Removed (indirect) dependency to random < 1.2 via package MissingH
- Added support for UTCTime in RDataType 
