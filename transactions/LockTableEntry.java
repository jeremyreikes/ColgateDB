package colgatedb.transactions;

import colgatedb.page.PageId;

import java.security.Permission;
import java.util.*;
import java.util.concurrent.locks.Lock;

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
 * Represents the state associated with the lock on a particular page.
 */
public class LockTableEntry {

    // some suggested private instance variables; feel free to modify
    private Permissions lockType;             // null if no one currently has a lock
    private Set<TransactionId> lockHolders;   // a set of txns currently holding a lock on this page
    private List<LockRequest> requests;       // a queue of outstanding requests

    public LockTableEntry() {
        lockType = null;
        lockHolders = new HashSet<>();
        requests = new LinkedList<>();
    }


    public synchronized Set<TransactionId> getLockHolders() {
        return this.lockHolders;
    }
    public synchronized Permissions getLockType() {
        return this.lockType;
    }


    public synchronized boolean txHoldsLock (TransactionId tid) {
        return lockHolders.contains(tid);
    }

    public synchronized void addNewLock (Permissions perm, TransactionId tid) {
        this.lockType = perm;
        lockHolders.add(tid);
    }
    public synchronized void setPermission (Permissions perm) {
        this.lockType = perm;
    }

    public synchronized void releaseLockForTid (TransactionId tid) {
        lockHolders.remove(tid);
        if (lockHolders.isEmpty()) { // If only lockholder, fully release lock on page
            this.lockType = null;
        }
    }

    // METHODS ON REQUESTS
    public synchronized List<LockRequest> getRequests() {
        return this.requests;
    }

    public synchronized void addNewRequest(Permissions perm, TransactionId tid){
        LockRequest newRequest = new LockRequest(tid, perm);
        if (perm.permLevel == 1) {
            int i = 0;
            if (requestsIsEmpty()) {
                requests.add(newRequest);
            }
            else {

                for (LockRequest req : requests) {
                    if (req.perm.permLevel == 0) {
                        requests.add(i, newRequest);
                        return; // just added this
                    }
                    i++;
                }
                requests.add(i, newRequest); // this should either be 0, newRequest or just newRequest

                /*
                for (Iterator<LockRequest> iterator = requests.iterator(); iterator.hasNext()) {
                    LockRequest temp = iterator.next();
                    if(temp.perm.permLevel == 0) {
                        iterator
                    }
                }
                */
            }

        }
        else {
            requests.add(newRequest);
        }
    }

    public synchronized TransactionId getFirstTid () {
        for (LockRequest req : requests) {
            return req.tid;
        }
        return null;
    }

    public synchronized boolean upgradeGranted (TransactionId tid) {
        if (this.lockType == null) {return false;}
        if (this.lockType.permLevel == 0) {
            for (LockRequest req : requests) {
                if (req.perm.permLevel == 1 && tid == req.tid) {
                    return true;
                }
            }
        }
        return false;

    }

    public synchronized int getFirstPerm () {
        for (LockRequest req : requests) {
            return req.perm.permLevel;
        }
        return -1;
    }

    public synchronized void clearFirstRequest (TransactionId tid, Permissions perm) {
        LockRequest firstRequest = new LockRequest(tid, perm);
        requests.remove(firstRequest);
    }

    public synchronized boolean isFirstRequest (TransactionId tid) {
        for (LockRequest req : requests) {
            return req.tid == tid;
        }
        return false;
    }

    public synchronized void removeFirstRequest () {
        requests.remove(0);
    }

    public synchronized boolean requestsIsEmpty () {
        return requests.size() == 0;
    }

    public synchronized void clearRequests () {
        requests = new ArrayList<LockRequest>();
    }
    /**
     * A class representing a single lock request.  Simply tracks the txn and the desired lock type.
     * Feel free to use this, modify it, or not use it at all.
     */
    private class LockRequest {
        public final TransactionId tid;
        public final Permissions perm;

        public LockRequest(TransactionId tid, Permissions perm) {
            this.tid = tid;
            this.perm = perm;
        }

        public boolean equals(Object o) {
            if (!(o instanceof LockRequest)) {
                return false;
            }
            LockRequest otherLockRequest = (LockRequest) o;
            return tid.equals(otherLockRequest.tid) && perm.equals(otherLockRequest.perm);
        }

        public String toString() {
            return "Request[" + tid + "," + perm + "]";
        }
    }
}
