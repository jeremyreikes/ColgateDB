# ColgateDB
ColgateDB is multi-user transactional DBMS written in Java.  
It served a semester-long project for COSC 460, Database Management Systems, taught by Professor Michael Hay at Colgate University.  
ColgateDB borrows heavily from SimpleDB, which was created by Sam Madden at MIT.
Many thanks to Professor Hay for creating this invaluable tool for understanding Databases.

## Overview
ColgateDB is a minimalist Database Management System written in Java.  

The original Github repository we began with can be found [here](https://github.com/colgate-cosc460/colgatedb).  Most files are unimplemented but lay the groundwork for our own implementations.

ColgateDB contains a variety of classes that represent:
- fields
- tuples
- schemas
- logging
- iterators
- operators
- cataloging
- heap files
- access manager
- buffer manager
- lock manager
- transaction manager
- disk manager

I wrote or heavily modified the following files, amongst a few others.
### Buffer and Disk Management

- [BufferManagerImpl.java](https://github.com/jeremyreikes/ColgateDB/blob/master/BufferManagerImpl.java)
- [HeapFile.java](https://github.com/jeremyreikes/ColgateDB/blob/master/dbfile/HeapFile.java)
### Tuples

- [RecordId.java](https://github.com/jeremyreikes/ColgateDB/blob/master/tuple/RecordId.java)
- [Tuple.java](https://github.com/jeremyreikes/ColgateDB/blob/master/tuple/Tuple.java)
- [TupleDesc.java](https://github.com/jeremyreikes/ColgateDB/blob/master/tuple/TupleDesc.java)
### Transactions

- [LockManagerImpl.java](https://github.com/jeremyreikes/ColgateDB/blob/master/transactions/LockManagerImpl.java)
- [LockTableEntry.java](https://github.com/jeremyreikes/ColgateDB/blob/master/transactions/LockTableEntry.java)
### Paging

- [SimplePageId.java](https://github.com/jeremyreikes/ColgateDB/blob/master/page/SimplePageId.java)
- [SlottedPage.java](https://github.com/jeremyreikes/ColgateDB/blob/master/page/SlottedPage.java)
- [SlottedPageFormatter.java](https://github.com/jeremyreikes/ColgateDB/blob/master/page/SlottedPageFormatter.java)
### Operators

- [Filter.java](https://github.com/jeremyreikes/ColgateDB/blob/master/operators/Filter.java)
- [Insert.java](https://github.com/jeremyreikes/ColgateDB/blob/master/operators/Insert.java)
- [Join.java](https://github.com/jeremyreikes/ColgateDB/blob/master/operators/Join.java)
- [JoinPredicate.java](https://github.com/jeremyreikes/ColgateDB/blob/master/operators/JoinPredicate.java)
###

## Limitations
Although ColgateDB contains most basic features found in DBMS's, it has some limitations compared to more fully-featured databases.
- Contains only fixed length records
- Lacks a front end client for querying
- No indexing capabilities
