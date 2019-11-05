package colgatedb.dbfile;

import colgatedb.*;
import colgatedb.page.*;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;
import colgatedb.tuple.RecordId;

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
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with SlottedPage. The format of SlottedPages is described in the javadocs
 * for SlottedPage.
 *
 * @see SlottedPage
 */
public class HeapFile implements DbFile {

    private final SlottedPageMaker pageMaker;   // this should be initialized in constructor
    private final TupleDesc td;
    private final int pageSize;
    private final int tableid;
    private int numPages;

    /**
     * Creates a heap file.
     * @param td the schema for records stored in this heapfile
     * @param pageSize the size in bytes of pages stored on disk (needed for PageMaker)
     * @param tableid the unique id for this table (needed to create appropriate page ids)
     * @param numPages size of this heapfile (i.e., number of pages already stored on disk)
     */
    public HeapFile(TupleDesc td, int pageSize, int tableid, int numPages) {
        this.td = td;
        this.pageSize = pageSize;
        this.tableid = tableid;
        this.numPages = numPages;
        this.pageMaker = new SlottedPageMaker(td, pageSize);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return this.numPages;
    }

    @Override
    public int getId() {
        return this.tableid;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    @Override
    public void insertTuple(TransactionId tid, Tuple t) throws TransactionAbortedException {
        SlottedPage page = getFreePage();
        page.insertTuple(t);
        BufferManager bufferManager = Database.getBufferManager();
        bufferManager.unpinPage(page.getId(), true); // Since we've added a tuple, the page is dirty and must be unpinned
    }

    /**
     * @return the first page with a free slot or a newly created page if none exists
     */
    public SlottedPage getFreePage() {
        BufferManager bufferManager = Database.getBufferManager();
        for (int i = 0; i < numPages; i++) { // loop through pages, pinning them, seeing if they have space
            SimplePageId pid = new SimplePageId(tableid, i);
            SlottedPage page = (SlottedPage)bufferManager.pinPage(pid, pageMaker);
            if (page.getNumEmptySlots() > 0) {
                return page;
            }
            bufferManager.unpinPage(pid, false);
        }
        // If no page has a free slot, create a new page, allocate space for it, and increment the page count
        SimplePageId pid = new SimplePageId(tableid, numPages);
        bufferManager.allocatePage(pid);
        numPages++;
        SlottedPage page = (SlottedPage)bufferManager.pinPage(pid, pageMaker);
        return page;
    }


    @Override
    public void deleteTuple(TransactionId tid, Tuple t) throws TransactionAbortedException {
        BufferManager bufferManager = Database.getBufferManager();
        PageId pid = t.getRecordId().getPageId();
        SlottedPage page = (SlottedPage)bufferManager.pinPage(pid, pageMaker);
        page.deleteTuple(t);
        bufferManager.unpinPage(pid, true);
    }

    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    /**
     * @see DbFileIterator
     */
    private class HeapFileIterator implements DbFileIterator {
        private int curPage;
        private Iterator<Tuple> iterator;
        private TransactionId tid;
        private boolean opened = false;
        private Tuple curTuple;
        private boolean alreadyHasNexted = false;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        @Override
        public void open() throws TransactionAbortedException {
            if (!opened) {
                curPage = 0;
                BufferManager bufferManager = Database.getBufferManager();
                SimplePageId pid = new SimplePageId(tableid, curPage);
                SlottedPage page = (SlottedPage)bufferManager.pinPage(pid, pageMaker);
                iterator =  page.iterator(); // if i include Iterator<tuple> iterator i pass another test
                opened = true;
                bufferManager.unpinPage(pid, false);
            }
            else {
                throw new TransactionAbortedException();
            }
        }

        @Override
        public boolean hasNext() throws TransactionAbortedException {
            BufferManager bufferManager = Database.getBufferManager();
            if (iterator == null) {
                return false;
            }
            if (opened) {
                if (alreadyHasNexted) { // checks to see whether hasNext() has been called since the last call to next()
                    return true;
                }
                if (iterator.hasNext()) { // check page for next tuple
                    curTuple = iterator.next();
                    alreadyHasNexted = true;
                    return true;
                }
                curPage++;
                if (curPage < numPages) {
                    for (; curPage < numPages; curPage++){ // checks pages in order
                        SimplePageId pid = new SimplePageId(tableid, curPage);
                        SlottedPage page = (SlottedPage)bufferManager.pinPage(pid, pageMaker);
                        iterator =  page.iterator();
                        bufferManager.unpinPage((PageId)pid, false);
                        if (iterator.hasNext()) {
                            curTuple = iterator.next();
                            alreadyHasNexted = true;
                            return true;
                        }
                    }
                }
                return false;
            }
            return false;
        }

        @Override
        public Tuple next() throws TransactionAbortedException, NoSuchElementException {
            if (hasNext()) {
                alreadyHasNexted = false;
                return curTuple;
            }
            throw new NoSuchElementException();
        }


        @Override
        public void rewind() throws TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            if (opened) {
                opened = false;
                iterator = null;
            }
        }
    }

}
