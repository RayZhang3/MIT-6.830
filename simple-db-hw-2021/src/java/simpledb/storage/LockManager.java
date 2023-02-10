package simpledb.storage;
import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertFalse;

public class LockManager {
    public class PageLock{
        public PageId pid;
        public LockType lockType;
        public TransactionId tid;
        public enum LockType {
            SHARED_LOCK, EXCLUSIVE_LOCK
        }
        public PageLock() {

        }
        public PageLock(TransactionId tid, PageId pid, LockType lockType) {
            this.tid = tid;
            this.pid = pid;
            this.lockType = lockType;
        }
        public PageLock(TransactionId tid, PageId pid, Permissions perm) {
            this.tid = tid;
            this.pid = pid;
            if (perm == Permissions.READ_ONLY) {
                this.lockType = LockType.SHARED_LOCK;
            } else if (perm == Permissions.READ_WRITE) {
                this.lockType =  LockType.EXCLUSIVE_LOCK;
            }
        }

    }

    private Map<TransactionId, List<PageLock>> transactionLockTable;
    private Map<PageId, List<PageLock>> lockTable;
    LockManager () {
        transactionLockTable = new ConcurrentHashMap<>();
        lockTable = new ConcurrentHashMap<>();
    }

    // When transaction commit or abort, we need to flush all the dirty page to disk.
    // This function returns the page transaction can Read/Write
    // (holds the exclusive lock)
    public synchronized List<PageId> transactionRWPageIds(TransactionId tid) {
        if (!transactionLockTable.containsKey(tid)) {
            return null;
        }
        List<PageId> list = new ArrayList<>();
        Iterator<PageLock> it = transactionLockTable.get(tid).iterator();
        while (it.hasNext()) {
            PageLock pageLock = it.next();
            if (pageLock.lockType == PageLock.LockType.EXCLUSIVE_LOCK) {
                list.add(pageLock.pid);
            }
        }
        return list;
    }

    //Release specific lock by tid and pid
    //getPage with no tuples
    public synchronized boolean releaseLock(TransactionId tid, PageId pid) {
        if (!lockTable.containsKey(pid)) {
            throw new NoSuchElementException("The page has no locks");
        }
        List<PageLock> list = lockTable.get(pid);
        Iterator<PageLock> it = list.iterator();
        while(it.hasNext()) {
            PageLock pageLock = it.next();
            if (pageLock.tid.equals(tid)) {
                removeLock(tid, pageLock);
                it.remove();
                return true;
            }
        }
        return false;
    }
    // Release all the lock which specific transaction holds.
    public synchronized boolean releaseLock(TransactionId tid) {
        boolean releaseLock = false;
        if (!transactionLockTable.containsKey(tid)) {
            return releaseLock;
        } else {
            List<PageLock> list = transactionLockTable.get(tid);
            Iterator<PageLock> it = list.iterator();
            while (it.hasNext()) {
                PageLock pageLock = it.next();
                lockTable.get(pageLock.pid).remove(pageLock);
                it.remove();
                releaseLock = true;
            }
        }
        return releaseLock;
    }

    public synchronized boolean releaseLock(PageId pid) {
        if (!lockTable.containsKey(pid)) {
            return true;
        } else {
            List<PageLock> list = lockTable.get(pid);
            Iterator<PageLock> it = list.iterator();
            while (it.hasNext()) {
                PageLock pageLock = it.next();
                removeLock(pageLock.tid, pageLock);
                it.remove();
            }
        }
        return true;
    }

    // Search the Map<PageId, PageLock> by PageId
    // If the transaction holds no lock on this page, return false
    public synchronized boolean hasLock(TransactionId tid, PageId pid) {
        boolean hasLock = false;
        if (!lockTable.containsKey(pid)) {
            return false;
        }
        List<PageLock> list = lockTable.get(pid);
        Iterator<PageLock> it = list.iterator();
        while(it.hasNext()) {
            PageLock pageLock = it.next();
            if (pageLock.tid.equals(tid)) {
                return true;
            }
        }
        return false;
    }

    public synchronized PageLock getlock(TransactionId tid, PageId pid) {
        if (!lockTable.containsKey(pid)) {
            throw new NoSuchElementException("");
        }
        List<PageLock> list = lockTable.get(pid);
        Iterator<PageLock> it = list.iterator();
        while(it.hasNext()) {
            PageLock pageLock = it.next();
            if (pageLock.tid.equals(tid)) {
                return pageLock;
            }
        }
        throw new NoSuchElementException("The lock is not existed");
    }
    //NO STEAL: never evict dirty page locked by transaction
    // Check if the page is locked
    public synchronized boolean isPageLocked(PageId pid) {
        if (!this.lockTable.containsKey(pid)) {
            return false;
        }
        if (this.lockTable.get(pid).size() > 0) {
            return false;
        }
        return true;
    }

    public synchronized boolean grantLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        if (hasLock(tid, pid)) {
            PageLock pageLock = getlock(tid, pid);
            // If try to R&W, but the lockType is SHARED_LOCK
            if (perm == Permissions.READ_WRITE && pageLock.lockType == PageLock.LockType.SHARED_LOCK) {
                List<PageLock> list = lockTable.get(pid);
                //If it has the only one shared_lock. Upgrade Lock to EXCLUSIVE
                if (list.size() == 1) {
                    list.remove(pageLock);
                    removeLock(tid, pageLock);
                    PageLock newLock = new PageLock(tid, pid, PageLock.LockType.EXCLUSIVE_LOCK);
                    list.add(newLock);
                    addLock(tid, newLock);
                    return true;
                } else {
                    List<TransactionId> conflits = new LinkedList();
                    for (PageLock lock: list) {
                        conflits.add(lock.tid);
                    }
                    WaitForGraph waitForGraph = Database.getBufferPool().waitForGraph;
                    waitForGraph.upDateGraphCopy();
                    for (TransactionId tConflits: conflits) {
                        waitForGraph.insertGraphCopyEdge(tid, tConflits);
                    }
                    if (waitForGraph.hasCycle()) {
                        throw new TransactionAbortedException();
                    }

                    for (TransactionId tConflits: conflits) {
                        waitForGraph.insertEdge(tid, tConflits);
                    }
                    //
                    return false;
                }

            } else {
                return true;
            }
        }

        boolean isShared = false;
        boolean isExclusive = false;

        List<TransactionId> conflits = new LinkedList(); //

        // The page has no lock
        if (!lockTable.containsKey(pid)) {
            List<PageLock> list = new LinkedList<>();
            PageLock newLock = new PageLock(tid, pid, perm);
            list.add(newLock);
            lockTable.put(pid, list);
            addLock(tid, newLock);
            return true;
        } else { // The page has lock List (maybe empty)
            List<PageLock> list = lockTable.get(pid);
            if (list.size() == 0) {
                PageLock newLock = new PageLock(tid, pid, perm);
                list.add(newLock);
                addLock(tid, newLock);
                return true;
            }
            for (PageLock lock: list) {
                conflits.add(lock.tid);
                if (lock.lockType == PageLock.LockType.EXCLUSIVE_LOCK) {
                    isExclusive = true;
                }
                if (lock.lockType == PageLock.LockType.SHARED_LOCK) {
                    isShared = true;
                }
            }

            assertFalse(isExclusive && isShared);

            if (perm == Permissions.READ_ONLY && isShared) {
                PageLock pageLock = new PageLock(tid, pid, PageLock.LockType.SHARED_LOCK);
                list.add(pageLock);
                addLock(tid, pageLock);
                return true;
            } else {
                //
                WaitForGraph waitForGraph = Database.getBufferPool().waitForGraph;
                waitForGraph.upDateGraphCopy();
                for (TransactionId tConflits: conflits) {
                    waitForGraph.insertGraphCopyEdge(tid, tConflits);
                }
                if (waitForGraph.hasCycle()) {
                    throw new TransactionAbortedException();
                }
                //
                for (TransactionId tConflits: conflits) {
                    waitForGraph.insertEdge(tid, tConflits);
                }
                return false;
            }

        }
    }
    private synchronized void addLock(TransactionId tid, PageLock lock) {
        if (!transactionLockTable.containsKey(tid)) {
            List<PageLock> list= new LinkedList<>(){};
            list.add(lock);
            transactionLockTable.put(tid, list);
        } else {
            transactionLockTable.get(tid).add(lock);
        }
    }

    private synchronized void removeLock(TransactionId tid, PageLock lock) {
        if (!transactionLockTable.containsKey(tid)) {
            throw new NoSuchElementException("The transaction holds no lock");
        } else {
            boolean res = transactionLockTable.get(tid).remove(lock);
            if (!res) {
                throw new NoSuchElementException("The transaction holds no lock");
            }
        }
    }


}
