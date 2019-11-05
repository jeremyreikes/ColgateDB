package colgatedb.transactions;

import colgatedb.page.PageId;
import org.junit.runner.Request;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
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
public class LockManagerImpl implements LockManager {

    private HashMap<PageId,LockTableEntry> lockTable;


    public LockManagerImpl() {
        this.lockTable = new HashMap<PageId, LockTableEntry>();
    }

    @Override
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        // If PID isn't already in lockTable, add it
        synchronized (lockTable){
            if (!lockTable.containsKey(pid)){
                lockTable.put(pid,  new LockTableEntry());
            }
        }

        // Add TID to list of requests - Front if exclusive, End if otherwise
        LockTableEntry pageLock = lockTable.get(pid);
        pageLock.addNewRequest(perm ,tid);

        // initiate locking mechanism
        long sleepTime = ThreadLocalRandom.current().nextInt(100, 200 + 1);
        long timeWaited = 0;
        long startTime = System.currentTimeMillis();

        boolean waiting = true;
        while (waiting) {
            synchronized (this) {
                // CAN ONLY ACQUIRE LOCK IF FIRST IN QUEUE
                if (pageLock.getFirstTid() == tid || pageLock.getFirstPerm() == 0 || pageLock.upgradeGranted(tid)) {
                    // IF LOCK ISN'T HELD, ACQUIRE LOCK
                    if (pageLock.getLockType() == null) {
                        pageLock.addNewLock(perm, tid);
                        pageLock.clearFirstRequest(tid, perm);
                        lockTable.replace(pid, pageLock);
                        waiting = false;
                    }

                    int curLockType = pageLock.getLockType().permLevel;
                    int requestingLockType = perm.permLevel;

                    // REQUESTS SHARED LOCK
                    if (requestingLockType == 0) {
                        // NO CURRENT REQUESTS AND LOCK ISN'T EXCLUSIVE - ACQUIRE
                        if (pageLock.isFirstRequest(tid) && curLockType != 1) {
                            pageLock.addNewLock(perm, tid);
                            pageLock.removeFirstRequest();
                            waiting = false;
                            this.notifyAll();
                        }
                    }
                    // REQUESTS EXCLUSIVE LOCK
                    if (requestingLockType == 1) {
                        // FIRST REQUEST AND LOCK ISN'T HELD - ACQUIRE
                        if (pageLock.isFirstRequest(tid) && pageLock.getLockType() == null) {
                            pageLock.addNewLock(perm, tid);
                            pageLock.removeFirstRequest();
                            lockTable.replace(pid, pageLock);
                            waiting = false;
                        }
                        // UPGRADE FROM SHARED TO EXCLUSIVE - ACQUIRE
                        else if (curLockType == 0 && pageLock.getLockHolders().contains(tid)) {
                            pageLock.setPermission(perm);
                            pageLock.clearFirstRequest(tid, perm);
                            waiting = false;
                        }
                    }
                    // WAIT IF UNABLE TO ACQUIRE LOCK
                    if (waiting) {
                        try {
                            this.wait(sleepTime);
                            long currentTime = System.currentTimeMillis();
                            timeWaited = currentTime - startTime; // Total amount of time transaction has been waiting for lock
                            if (timeWaited >= 1000) {
                                if (pageLock.getLockType() == null) {
                                    // do nothing
                                }
                                else { // ABORT TRANSACTION
                                    pageLock.clearFirstRequest(tid, perm);
                                    throw new TransactionAbortedException();
                                }
                            }
                        } catch (InterruptedException e) {};
                    }
                }
            }
        }
    }



    public synchronized boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        if (lockTable.containsKey(pid)){
            LockTableEntry pageLock = lockTable.get(pid);
            if (pageLock.getLockHolders().contains(tid)) {
                return pageLock.getLockType().permLevel >= perm.permLevel;
            }
            else {
                return false;
            }
        }
        return false;
    }

    @Override
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        LockTableEntry pageLock = lockTable.get(pid);
        if (pageLock == null) {
            throw new LockManagerException("This transaction does not hold a lock on this page");
        }
        if (pageLock.getLockType() == null) {
            throw new LockManagerException("This transaction does not hold a lock on this page");
        }
        if (pageLock.getLockHolders().contains(tid)) {
            pageLock.releaseLockForTid(tid);
            this.notifyAll();
        }
        else {
            throw new LockManagerException("This transaction does not hold a lock on this page");
        }
    }

    @Override
    public synchronized List<PageId> getPagesForTid(TransactionId tid) {
        List<PageId> pages = new ArrayList<PageId>();
        for (PageId pid : lockTable.keySet()) {
            LockTableEntry pageLock = lockTable.get(pid);
            if (pageLock.txHoldsLock(tid)) {
                pages.add(pid);
            }
        }
        return pages;
    }

    @Override
    public synchronized List<TransactionId> getTidsForPage(PageId pid) {
        LockTableEntry pageLock = lockTable.get(pid);
        List<TransactionId> tids = new ArrayList<TransactionId>(pageLock.getLockHolders());
        return tids;
    }

}
