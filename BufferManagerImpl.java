package colgatedb;

import colgatedb.dbfile.HeapFile;
import colgatedb.page.*;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;

import static org.junit.Assert.fail;

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
public class BufferManagerImpl implements BufferManager {

    private boolean allowEvictDirty = false;  // a flag indicating whether a dirty page is candidate for eviction
    private DiskManager dm;
    private int numPages;
    private HashMap<PageId, Frame> bufferPool;
    /**
     * Construct a new buffer manager.
     * @param numPages maximum size of the buffer pool
     * @param dm the disk manager to call to read/write pages
     */
    public BufferManagerImpl(int numPages, DiskManager dm) {
        this.dm = dm;
        this.numPages = numPages;
        this.bufferPool = new HashMap<PageId, Frame>(numPages);
    }


    @Override
    public synchronized Page pinPage(PageId pid, PageMaker pageMaker) {
        Frame frame = new Frame(null);
        if (bufferPool.containsKey(pid)) {
            frame = bufferPool.get(pid);
            frame.pinCount++;
            return frame.page;
        }
        else {
            // if the bufferPool is full, look for eligible pages to evict.  If there is one, evict it and
            // add the pinned page to the buffer pool
            if (bufferPool.size() == numPages) {
                evictPage();
                /*
                try {
                    evictPage();
                } catch (BufferManagerException bme) {
                    allocatePage(pid);
                    SlottedPage page = (SlottedPage)pinPage(pid, pageMaker);
                    numPages++;
                    return page;
                }
                */


            }
            Page page = dm.readPage(pid, pageMaker);
            frame.page = page;
            bufferPool.put(pid, frame);
            return frame.page;
        }

    }

    @Override
    public synchronized void unpinPage(PageId pid, boolean isDirty) {
        if (bufferPool.containsKey(pid)) {
            Frame frame = bufferPool.get(pid);
            /* im trying to address the if page is unused, make sure it is unpineed
            if (frame.page.getPageData() == null) {
                frame.pinCount = 0;
                return;
            }
            **/
            if (frame.pinCount > 0) {
                frame.isDirty = isDirty;
                frame.pinCount--;
                // If page has been dirtied but not flushed to disk, mark as so.
                if (isDirty) {
                    frame.dirtyButNotFlushed = true;
                }
            }
            else {
                throw new BufferManagerException("pinCount is already 0");
            }
        }
        else {
            throw new BufferManagerException("Page not in buffer pool");
        }

    }

    @Override
    public synchronized void flushPage(PageId pid) {
        if (bufferPool.containsKey(pid)) {
            Frame frame = bufferPool.get(pid);
            if (frame.isDirty || frame.dirtyButNotFlushed) {
                dm.writePage(frame.page);
                bufferPool.remove(pid);
            }
        }
    }

    @Override
    public synchronized void flushAllPages() {
        ArrayList<PageId> pidsToRemove = new ArrayList<>();
        for (HashMap.Entry<PageId, Frame> entry : bufferPool.entrySet()) {
            PageId pid = entry.getKey();
            Frame frame = entry.getValue();
            if (frame.isDirty || frame.dirtyButNotFlushed) {
                dm.writePage(frame.page);
                pidsToRemove.add(pid);
            }
        }
        for (int i = 0; i < pidsToRemove.size(); i++) {
            bufferPool.remove(pidsToRemove.get(i));
        }

    }

    @Override
    public synchronized void evictDirty(boolean allowEvictDirty) {
        this.allowEvictDirty = allowEvictDirty;
    }

    @Override
    public synchronized void allocatePage(PageId pid) {
        dm.allocatePage(pid);
    }

    @Override
    public synchronized boolean isDirty(PageId pid) {
        if (inBufferPool(pid)) {
            return bufferPool.get(pid).isDirty;
        }
        return false;
    }

    @Override
    public synchronized boolean inBufferPool(PageId pid) {
        return bufferPool.containsKey(pid);
    }

    @Override
    public synchronized Page getPage(PageId pid) {
        if (inBufferPool(pid)) {
            return bufferPool.get(pid).page;
        }
        else {
            throw new BufferManagerException("Not in the buffer pool");
        }
    }

    @Override
    public synchronized void discardPage(PageId pid) {
        if (inBufferPool(pid)) {
            bufferPool.remove(pid);
        }
    }


    /**
     * My evictPage() method uses a dead simple eviction strategy.  Simply put, it loops through each frame in
     * the buffer pool.  If it finds a page with a pinCount of 0 AND either allowEvictDirty is true or the frame
     * isn't dirty, it writes that page to disk, and then removes it from the bufferPool.  If it is not able to find
     * such a page, it will throw an exception and the eviction will fail.
     */
    public synchronized void evictPage() {
        for (HashMap.Entry<PageId, Frame> entry : bufferPool.entrySet()) {
            PageId pid = entry.getKey();
            Frame frame = entry.getValue();
            if (frame.pinCount == 0 && allowEvictDirty && frame.isDirty) {
                dm.writePage(frame.page);
                bufferPool.remove(pid);
                return;
            }
            else if (frame.pinCount == 0 && !frame.isDirty) {
                bufferPool.remove(pid);
                return;
            }
        }
        throw new BufferManagerException("No pages are eligible to be evicted.");
    }


    /**
     * A frame holds one page and maintains state about that page.  You are encouraged to use this
     * in your design of a BufferManager.  You may also make any warranted modifications.
     */
    private class Frame {
        private Page page;
        private int pinCount;
        public boolean isDirty;
        public boolean dirtyButNotFlushed = false;


        public Frame(Page page) {
            this.page = page;
            this.pinCount = 1;   // assumes Frame is created on first pin -- feel free to modify as you see fit
            this.isDirty = false;
        }
    }

}