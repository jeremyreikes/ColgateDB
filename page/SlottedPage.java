package colgatedb.page;

import colgatedb.tuple.RecordId;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;

import java.util.Arrays;
import java.util.Iterator;
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
 * SlottedPage stores a collection of fixed-length tuples, all having the same schema.
 * Upon insertion, a tuple is assigned to a slot.  The number of slots available depends on
 * the size of the page and the schema of the tuples.
 */
public class SlottedPage implements Page {

    private final PageId pid;
    private final TupleDesc td;
    private final int pageSize;
    // you will want to include additional instance variables here.
    private final Tuple[] tuples;
    // ------------------------------------------------
    // oldData fields:
    // these are used for logging and recovery -- you can ignore for now
    private final Byte oldDataLock = (byte) 0;
    byte[] oldData;
    // ------------------------------------------------

    /**
     * Constructs empty SlottedPage
     * @param pid  page id to assign to this page
     * @param td   the schema for tuples held on this page
     * @param pageSize the size of this page
     */
    public SlottedPage(PageId pid, TupleDesc td, int pageSize) {
        this.pid = pid;
        this.td = td;
        this.pageSize = pageSize;
        this.tuples = new Tuple[getNumSlots()];
         // write some code here to finish initializing this object
        setBeforeImage();  // used for logging, leave this line at end of constructor
    }

    /**
     * Constructs SlottedPage with its data initialized according to last parameter
     * @param pid  page id to assign to this page
     * @param td   the schema for tuples held on this page
     * @param pageSize the size of this page
     * @param data data with which to initialize page content
     */
    public SlottedPage(PageId pid, TupleDesc td, int pageSize, byte[] data) {
        this(pid, td, pageSize);
        setPageData(data);
        setBeforeImage();  // used for logging, leave this line at end of constructor
    }

    @Override
    public PageId getId() {
        return this.pid;
    }

    /**
     * @param slotno the slot number
     * @return true if this slot is used (i.e., is occupied by a Tuple).
     */
    public boolean isSlotUsed(int slotno) {
        return tuples[slotno] != null;
    }

    /**
     *
     * @param slotno the slot number
     * @return true if this slot is empty (i.e., is not occupied by a Tuple).
     */
    public boolean isSlotEmpty(int slotno) {
        return !isSlotUsed(slotno);
    }

    /**
     * @return the number of slots this page can hold.  Determined by
     * the page size and the schema (TupleDesc).
     */
    public int getNumSlots() {
        return (int)Math.floor((pageSize * 8) / (td.getSize() * 8 + 1));
    }

    /**
     * @return the number of slots on this page that are empty.
     */
    public int getNumEmptySlots() {
        int emptySlots = 0;
        for (int i = 0; i < getNumSlots(); i++) {
            if (tuples[i] == null) {
                emptySlots++;
            }
        }
        return emptySlots;
    }

    /**
     * @param slotno the slot of interest
     * @return returns the Tuple at given slot
     * @throws PageException if slot is empty
     */
    public Tuple getTuple(int slotno) {
        if (tuples[slotno] == null) {
            throw new PageException("Slot is empty.  Failed to get tuple.");
        }
        else {
            return tuples[slotno];
        }
    }

    /**
     * Adds the specified tuple to specific slot in page.
     * <p>
     * The tuple should be updated to reflect that it is now stored on this page
     * (hint: set its RecordId).
     *
     * @param slotno the slot into which this tuple should be inserted
     * @param t The tuple to add.
     * @throws PageException if the slot is full or TupleDesc of
     *                          passed tuple is a mismatch with TupleDesc of this page.
     */
    public void insertTuple(int slotno, Tuple t) {
        if (isSlotEmpty(slotno)) {
            if (t.getTupleDesc().equals(this.td)) {
                tuples[slotno] = t;
                t.setRecordId(new RecordId(pid, slotno));
            } else {
                throw new PageException("The scehma do not match.  Insertion failed.");
            }
        } else {
            throw new PageException("Slot is already being used.  Insertion failed");
        }
    }

    /**
     * Adds the specified tuple to the page into an available slot.
     * <p>
     * The tuple should be updated to reflect that it is now stored on this page
     * (hint: set its RecordId).
     *
     * @param t The tuple to add.
     * @throws PageException if the page is full (no empty slots) or TupleDesc of
     *                          passed tuple is a mismatch with TupleDesc of this page.
     */
    public void insertTuple(Tuple t) throws PageException {
        if (t.getTupleDesc().equals(this.td)) {
            for (int i = 0; i < getNumSlots(); i++) {
                if (isSlotEmpty(i)) {
                    tuples[i] = t;
                    t.setRecordId(new RecordId(pid, i));
                    return;
                }
            }
            throw new PageException("No empty spots.  Insertion failed");
        }
        else {
            throw new PageException("The schema do not match.  Insertion failed.");
        }
    }

    /**
     * Delete the specified tuple from the page; the tuple should be updated to reflect
     * that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws PageException if this tuple doesn't have a record id, is not on this page, or tuple
     *                          slot is already empty.
     */
    public void deleteTuple(Tuple t) throws PageException {
        if (t.getRecordId() == null) {
            throw new PageException("The tuple doesn't have a record id.  Deletion failed.");
        }
        if (pid != t.getRecordId().getPageId()) {
            throw new PageException("The tuple isn't on this page.  Deletion failed");
        }
        for (int i = 0; i < getNumSlots(); i++) {
            if (isSlotUsed(i)) {
                if (t.equals(tuples[i])) {
                    tuples[i] = null;
                    t.setRecordId(null);
                    return;
                }
            }
        }
        throw new PageException("The tuple isn't on this page.  Deletion failed.");
    }


    /**
     * Creates an iterator over the (non-empty) slots of the page.
     *
     * @return an iterator over all tuples on this page
     * (Note: calling remove on this iterator throws an UnsupportedOperationException)
     */
    public Iterator<Tuple> iterator() {
        return new MyIterator();
    }

    class MyIterator implements Iterator<Tuple> {

        private int currIdx;

        public MyIterator() {
            currIdx = 0;
        }

        @Override
        public boolean hasNext() {
            for (int i = currIdx; i < getNumSlots() ; i++) {
                if (isSlotUsed(i))
                    return true;
            }
            return false;
        }

        @Override
        public Tuple next() {
            if (!hasNext()) {   // always check!
                throw new NoSuchElementException();
            }
            while(isSlotEmpty(currIdx)){
                currIdx++;
            }
            Tuple nextTuple = tuples[currIdx];
            currIdx++;
            return nextTuple;
        }
    }



    @Override
    public byte[] getPageData() {
         return SlottedPageFormatter.pageToBytes(this, this.td, this.pageSize);
    }

    /**
     * Fill the contents of this according to the data stored in byte array.
     * @param data
     */
    private void setPageData(byte[] data) {
         SlottedPageFormatter.bytesToPage(data, this, this.td);
    }

    @Override
    public Page getBeforeImage() {
        byte[] oldDataRef;
        synchronized (oldDataLock) {
            oldDataRef = Arrays.copyOf(oldData, oldData.length);
        }
        return new SlottedPage(pid, td, pageSize, oldDataRef);
    }

    @Override
    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

}
