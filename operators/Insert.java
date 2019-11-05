package colgatedb.operators;

import colgatedb.Catalog;
import colgatedb.Database;
import colgatedb.DbException;
import colgatedb.dbfile.DbFile;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.IntField;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;
import colgatedb.tuple.Type;

import java.util.NoSuchElementException;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are either wholly the creation of Michael Hay or are
 * a significant adaptation of code from the SimpleDB project.  A number of
 * substantive changes have been made to meet the pedagogical goals of the cosc460
 * course at Colgate.  If this file contains remnants from SimpleDB, we are
 * grateful for Sam's permission to use and adapt his materials.
 */

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {
    private int tableId;
    private DbIterator child;
    private Tuple curTuple;
    private int insertions;
    private TransactionId t;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        this.insertions = 0;
        this.curTuple = null;
        this.tableId = tableid;
        this.child = child;
        this.t = t;
        TupleDesc childTd = child.getTupleDesc();
        TupleDesc catalogTd = Database.getCatalog().getTupleDesc(tableId);
        if (!childTd.equals(catalogTd)) {
            throw new DbException("The TupleDesc of the child differs from the TupleDesc of the insertion table");
        }
    }

    /**
     * @return tuple desc of the insert operator should be a single INT named count
     */
    @Override
    public TupleDesc getTupleDesc() {
        String[] fields = new String[1];
        fields[0] = "count";
        Type[] intType = new Type[]{Type.INT_TYPE};
        return new TupleDesc(intType, fields);
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        throw new UnsupportedOperationException("implement me!");
    }

    /**
     *
     * @return true if this is the first time being called...  even if child is empty,
     *         this iterator still has one tuple to return (the tuple that says that zero
     *         records were inserted).
     */
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        throw new UnsupportedOperationException("implement me!");
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a single-field tuple containing the number of
     * inserted records. Inserts should be completed by calling the insertTuple
     * method on the appropriate DbFile (which can be looked up in the Catalog).
     * <p>
     * Note that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * @return A single-field tuple containing the number of inserted records.
     * @throws NoSuchElementException if called more than once
     */
    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public DbIterator[] getChildren() {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public void setChildren(DbIterator[] children) {
        throw new UnsupportedOperationException("implement me!");
    }
}

