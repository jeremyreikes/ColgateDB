# ColgateDB
ColgateDB is multi-user transactional DBMS written in Java.

It served a semester-long project for COSC 460, Database Management Systems, taught by Professor Michael Hay at Colgate University.

ColgateDB borrows heavily from SimpleDB, which was created by Sam Madden at MIT.
Many thanks to Professor Hay for creating this invaluable tool for understanding Databases.

## Overview
ColgateDB is a minimalist Database Management System written in Java.  

The original Github repository can be found [here](https://github.com/colgate-cosc460/colgatedb).  Most files are unimplemented but provide a foundation to work from.

ColgateDB contains a variety of classes that represent:
- fields
- tuples
- schemas
- logging
- iterators
- operators
- cataloging
- heap files
- access management
- buffer management
- lock management
- transaction management
- disk management

I implemented the following files, amongst a few others:
### Buffer and Disk Management
- [BufferManagerImpl.java](https://github.com/jeremyreikes/ColgateDB/blob/master/BufferManagerImpl.java)
- [HeapFile.java](https://github.com/jeremyreikes/ColgateDB/blob/master/dbfile/HeapFile.java)
### Tuples/Records
- [RecordId.java](https://github.com/jeremyreikes/ColgateDB/blob/master/tuple/RecordId.java)
- [Tuple.java](https://github.com/jeremyreikes/ColgateDB/blob/master/tuple/Tuple.java)
- [TupleDesc.java](https://github.com/jeremyreikes/ColgateDB/blob/master/tuple/TupleDesc.java)
### Transactions and Locking
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
### Logging
As of 11/05/2019, my logging file is on an old hard drive.  I should be able to get to the hard drive the week of 11/16 and will add it then.

## Limitations
Although ColgateDB contains most basic features found in DBMS's, it has some limitations compared to more fully-featured databases.
- Contains only fixed length records
- Lacks a front end client for querying
- No indexing capabilities

## Conclusion
Throughout my Databases course, we worked on implementing topics while we studied them.  

When we learned about file storage like heap files and B+ trees, we implemented heap files and buffer management in ColgateDB.  

When we studied transaction management, we implemented a lock manager.  

When we learned about crash-recovery and database durability, we implemented write ahead logging.

The list goes on.

Although ColgateDB is just a barebones DBMS, it was a helpful tool for learning the ins and outs of modern database design.
