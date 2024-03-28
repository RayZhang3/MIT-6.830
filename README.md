# MIT 6.830
Course Schedule: http://db.lcs.mit.edu/6.5830/2021/assign.php
Recording Video: https://www.youtube.com/watch?v=F3XGUPll6Qs&list=PLfciLKR3SgqOxCy1TIXXyfTqKzX2enDjK
[Lab1](https://github.com/MIT-DB-Class/simple-db-hw-2021/blob/master/lab1.md)
[Lab2](https://github.com/MIT-DB-Class/simple-db-hw-2021/blob/master/lab2.md)
[Lab3](https://github.com/MIT-DB-Class/simple-db-hw-2021/blob/master/lab3.md)
[Lab4](https://github.com/MIT-DB-Class/simple-db-hw-2021/blob/master/lab4.md)
[Lab5](https://github.com/MIT-DB-Class/simple-db-hw-2021/blob/master/lab5.md)
[Lab6](https://github.com/MIT-DB-Class/simple-db-hw-2021/blob/master/lab6.md)

# SimpleDB
SimpleDB consists of:
1. Classes that represent fields, tuples, and tuple schemas;
2. Classes that apply predicates and conditions to tuples;
3. One or more access methods (e.g., heap files) that store relations on disk and provide a way to iterate through tuples of those relations;
4. A collection of operator classes (e.g., select, join, insert, delete, etc.) that process tuples;
5. A buffer pool that caches active tuples and pages in memory and handles concurrency control and transactions (neither of which you need to worry about for this lab); and,
6. A catalog that stores information about available tables and their schemas.
7. B+ Tree Indices
8. Queries are built up by chaining a set of operators together into a hand-built query plan
9. Query optimizer
    
In particular, SimpleDB does not have:
1. Data types except integers and fixed length strings.
2. Views.
3. a SQL front end or parser that allows you to type queries directly into SimpleDB
4. 
# Lab1
## Goals
1. Implement the classes to manage tuples, namely Tuple, TupleDesc. We have already implemented Field, IntField, StringField, and Type for you. Since you only need to support integer and (fixed length) string fields and fixed length tuples, these are straightforward.
2. Implement the Catalog (this should be very simple).
3. Implement the BufferPool constructor and the getPage() method.
4. Implement the access methods, HeapPage and HeapFile and associated ID classes.
5. Implement the operator SeqScan.
6. Pass the ScanTest system test,

# Lab2
## Goals
1. Implement the operators Filter and Join and verify that their corresponding tests work. The Javadoc comments for these operators contain details about how they should work. 
2. Implement IntegerAggregator and StringAggregator. Here, you will write the logic that actually computes an aggregate over a particular field across multiple groups in a sequence of input tuples. Use integer division for computing the average, since SimpleDB only supports integers. StringAggegator only needs to support the COUNT aggregate, since the other operations do not make sense for strings.
3. Implement the Aggregate operator. As with other operators, aggregates implement the OpIterator interface so that they can be placed in SimpleDB query plans. Note that the output of an Aggregate operator is an aggregate value of an entire group for each call to next(), and that the aggregate constructor takes the aggregation and grouping fields.
4. Implement the methods related to tuple insertion, deletion, and page eviction in BufferPool. You do not need to worry about transactions at this point.
5. Implement the Insert and Delete operators. Like all operators, Insert and Delete implement OpIterator, accepting a stream of tuples to insert or delete and outputting a single tuple with an integer field that indicates the number of tuples inserted or deleted. These operators will need to call the appropriate methods in BufferPool that actually modify the pages on disk. Check that the tests for inserting and deleting tuples work properly.

# Lab3
## Goals
1. Implement the methods in the TableStats class that allow it to estimate selectivities of filters and cost of scans, using histograms (skeleton provided for the IntHistogram class) or some other form of statistics of your devising.
2. Implement the methods in the JoinOptimizer class that allow it to estimate the cost and selectivities of joins.
3. Write the orderJoins method in JoinOptimizer. This method must produce an optimal ordering for a series of joins (likely using the Selinger algorithm), given statistics computed in the previous two steps.

# Lab4
## Goals
1. Assuming you are using page-level locking, you will need to complete the following:
	1. Modify getPage() to block and acquire the desired lock before returning a page.
	2. Implement unsafeReleasePage(). This method is primarily used for testing, and at the end of transactions.
	3. Implement holdsLock() so that logic in Exercise 2 can determine whether a page is already locked by a transaction.
2. Reading tuples off of pages during a SeqScan
3. Inserting and deleting tuples through BufferPool and HeapFile methods
4. think especially hard about acquiring and releasing locks in the following situations:
	1. Adding a new page to a HeapFile. 
	2. Looking for an empty slot into which you can insert tuples. 
	3. Most implementations scan pages looking for an empty slot, and will need a READ_ONLY lock to do this. Surprisingly, however, if a transaction t finds no free slot on a page p, t may immediately release the lock on p. Although this apparently contradicts the rules of two-phase locking, it is ok because t did not use any data from the page, such that a concurrent transaction t' which updated p cannot possibly effect the answer or outcome of t.
5. Implement the necessary logic for page eviction without evicting dirty pages in the evictPage method in BufferPool.
6. Implement the transactionComplete() method in BufferPool. 
	1. When you commit, you should flush dirty pages associated to the transaction to disk. When you abort, you should revert any changes made by the transaction by restoring the page to its on-disk state.
	2. Whether the transaction commits or aborts, you should also release any state the BufferPool keeps regarding the transaction, including releasing any locks that the transaction held.
7. Implement deadlock detection or prevention in src/simpledb/BufferPool.java. You have many design decisions for your deadlock handling system, but it is not necessary to do something highly sophisticated. 
	1. You should ensure that your code aborts transactions properly when a deadlock occurs, by throwing a TransactionAbortedException exception. This exception will be caught by the code executing the transaction (e.g., TransactionTest.java), which should call transactionComplete() to cleanup after the transaction. 
	2. You are not expected to automatically restart a transaction which fails due to a deadlock -- you can assume that higher level code will take care of this.
	
# Lab5
## Goals
In this lab you will implement a B+ tree index for efficient lookups and range scans. We supply you with all of the low-level code you will need to implement the tree structure. You will implement searching, splitting pages, redistributing tuples between pages, and merging pages. Features including Search, Insert, Delete and Transactions.

As described by the textbook and discussed in class, the internal nodes in B+ trees contain multiple entries, each consisting of a key value and a left and a right child pointer. Adjacent keys share a child pointer, so internal nodes containing m keys have m+1 child pointers. Leaf nodes can either contain data entries or pointers to data entries in other database files. For simplicity, we will implement a B+tree in which the leaf pages actually contain the data entries. Adjacent leaf pages are linked together with right and left sibling pointers, so range scans only require one initial search through the root and internal nodes to find the first leaf page. Subsequent leaf pages are found by following right (or left) sibling pointers.

You may remember that B+ trees can prevent phantom tuples from showing up between two consecutive range scans by using next-key locking. Since SimpleDB uses page-level, strict two-phase locking, protection against phantoms effectively comes for free if the B+ tree is implemented correctly. Thus, at this point you should also be able to pass BTreeNextKeyLockingTest.
# Lab6
## Goals
In this lab you will implement log-based rollback for aborts and log-based crash recovery. We supply you with the code that defines the log format and appends records to a log file at appropriate times during transactions. You will implement rollback and recovery using the contents of the log file.

The logging code we provide generates records intended for physical whole-page undo and redo. When a page is first read in, our code remembers the original content of the page as a before-image. When a transaction updates a page, the corresponding log record contains that remembered before-image as well as the content of the page after modification as an after-image. You'll use the before-image to roll back during aborts and to undo loser transactions during recovery, and the after-image to redo winners during recovery.

We are able to get away with doing whole-page physical UNDO (while ARIES must do logical UNDO) because we are doing page level locking and because we have no indices which may have a different structure at UNDO time than when the log was initially written. The reason page-level locking simplifies things is that if a transaction modified a page, it must have had an exclusive lock on it, which means no other transaction was concurrently modifying it, so we can UNDO changes to it by just overwriting the whole page.

Your BufferPool already implements abort by deleting dirty pages, and pretends to implement atomic commit by forcing dirty pages to disk only at commit time. Logging allows more flexible buffer management (STEAL and NO-FORCE), and our test code calls BufferPool.flushAllPages() at certain points in order to exercise that flexibility.
