package colgatedb.operators;

import colgatedb.DbException;
import colgatedb.dbfile.DbFileIterator;
import colgatedb.main.OperatorMain;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.tuple.Tuple;

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
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    Predicate p;
    DbIterator child;
    private boolean opened;
    private boolean alreadyHasNexted = false;
    private Tuple curTuple;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.p = p;
        this.child = child;
        this.setTupleDesc(child.getTupleDesc());
        opened = false;
    }

    public Predicate getPredicate() {
        return this.p;
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        opened = true;
    }

    @Override
    public void close() {
        child.close();
        opened = false;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (child == null) {
            return false;
        }
        if (alreadyHasNexted) {
            return true;
        }
        if (opened) {
            while (child.hasNext()) {
                //System.out.println("HERE");
                Tuple tempTuple = child.next();
                System.out.println(tempTuple.toString());
                if (p.filter(tempTuple)) {
                    curTuple = tempTuple;
                    alreadyHasNexted = true;
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (hasNext()) {
            alreadyHasNexted = false;
            return curTuple;
        }
        throw new NoSuchElementException();
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length != 1) {
            throw new DbException("Expected one child");
        }
        child = children[0];
    }
}
