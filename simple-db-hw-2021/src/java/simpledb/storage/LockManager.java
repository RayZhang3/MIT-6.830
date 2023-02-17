package simpledb.storage;
import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LockManager {
    public class PageLock{
        public PageId pid;
        public LockType lockType;
        public TransactionId tid;
        public enum LockType {
            SHARED_LOCK, EXCLUSIVE_LOCK, WAITING;
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
    // only add the record with S or X lock.
    public synchronized List<PageId> transactionPageIds(TransactionId tid) {
        if (!transactionLockTable.containsKey(tid)) {
            return null;
        }
        List<PageId> list = new ArrayList<>();
        Iterator<PageLock> it = transactionLockTable.get(tid).iterator();
        while (it.hasNext()) {
            PageLock pageLock = it.next();
            if (pageLock.lockType == PageLock.LockType.EXCLUSIVE_LOCK ||
            pageLock.lockType == PageLock.LockType.SHARED_LOCK)
            list.add(pageLock.pid);
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
            if (pageLock.tid.equals(tid) && pageLock.lockType != PageLock.LockType.WAITING) {
                return true;
            }
        }
        return false;
    }

    // Search the Map<PageId, PageLock> by PageId
    // If the transaction holds any types of lock include waiting on this page, return this lock
    public synchronized PageLock hasRecord(TransactionId tid, PageId pid) {
        if (!lockTable.containsKey(pid)) {
            return null;
        }
        List<PageLock> list = lockTable.get(pid);
        Iterator<PageLock> it = list.iterator();
        while(it.hasNext()) {
            PageLock pageLock = it.next();
            if (pageLock.tid.equals(tid)) {
                return pageLock;
            }
        }
        return null;
    }


    public synchronized PageLock getlock(TransactionId tid, PageId pid) {
        if (!lockTable.containsKey(pid)) {
            throw new NoSuchElementException("");
        }
        List<PageLock> list = lockTable.get(pid);
        Iterator<PageLock> it = list.iterator();
        while(it.hasNext()) {
            PageLock pageLock = it.next();
            if (pageLock.tid.equals(tid) && pageLock.lockType!= PageLock.LockType.WAITING) {
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
        List<PageLock> list = this.lockTable.get(pid);
        if (list.size() > 0) {
            for (int i = 0; i < list.size(); i += 1) {
                PageLock pageLock = list.get(i);
                if (isShareLock(pageLock) || isExclusiveLock(pageLock)) {
                    return true;
                }
            }
        }
        return false;
    }
    private boolean isShareLock(PageLock pageLock) {
        return pageLock.lockType == PageLock.LockType.SHARED_LOCK;
    }
    private boolean isExclusiveLock(PageLock pageLock) {
        return pageLock.lockType == PageLock.LockType.EXCLUSIVE_LOCK;
    }
    private boolean isWait(PageLock pageLock) {
        return pageLock.lockType == PageLock.LockType.WAITING;
    }
    private boolean allShare(List<PageLock> pageLocks) {
        for (int i = 0; i < pageLocks.size(); i += 1) {
            if (!isShareLock(pageLocks.get(i))) {
                return false;
            }
        }
        return true;
    }
    private boolean allWait(List<PageLock> pageLocks) {
        for (int i = 0; i < pageLocks.size(); i += 1) {
            if (!isWait(pageLocks.get(i))) {
                return false;
            }
        }
        return true;
    }
    public synchronized PageLock addWaitForLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        assertTrue(lockTable.containsKey(pid));
        List<PageLock> list = lockTable.get(pid);
        List<TransactionId> conflits = new LinkedList();
        //If there are all share lock, we need to add each one
        if (allShare(list)) {
            for (PageLock lock : list) {
                conflits.add(lock.tid);
            }
        } else { // If not, just add the last one, because we only wait for it.
            conflits.add(list.get(list.size() - 1).tid);
        }
        List<List<TransactionId>> edges = new ArrayList<>(conflits.size());
        for (TransactionId conflitId : conflits) {
            edges.add(Arrays.asList(tid, conflitId));
        }
        WaitForGraph waitForGraph = Database.getBufferPool().getWaitForGraph();

        boolean isCyclic = waitForGraph.DetectCycleWithEdges(edges);

        /*
        List<Long> cycleTransaction = waitForGraph.DetectCycleWithEdges(edges);
        boolean isCyclic = (cycleTransaction != null);
        Database.getBufferPool().cycleTransaction = cycleTransaction;
         */

        // If detect cycle, throw an aborted exception.
        if (isCyclic) {
            throw new TransactionAbortedException();
        } else { // no cycle, add to wait-for graph
            waitForGraph.updateGraph(edges);
            PageLock wait = new PageLock(tid, pid, PageLock.LockType.WAITING);
            list.add(wait);
            addLock(tid, wait);
            return wait;
        }
    }
    public synchronized PageLock grantLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        PageLock record = hasRecord(tid, pid);
        if (record != null) {
            switch(record.lockType) {
                case EXCLUSIVE_LOCK: {
                    return record;
                }
                case SHARED_LOCK: {
                    if (perm == Permissions.READ_ONLY) {
                        return record;
                    } else { //perm == READ_WRITE, need to upgrade
                        List<PageLock> list = lockTable.get(pid);
                        //If it has the only one shared_lock. Upgrade Lock to EXCLUSIVE
                        if (list.size() == 1 && allShare(list)) {
                            removeLock(tid, record);
                            list.remove(0);
                            PageLock newLock = new PageLock(tid, pid, PageLock.LockType.EXCLUSIVE_LOCK);
                            list.add(0, newLock);
                            addLock(tid, newLock);
                            return newLock;
                        } else {
                           PageLock waitLock = addWaitForLock(tid, pid, perm);
                           return waitLock;
                        }
                    }
                }
                case WAITING: {
                    List<PageLock> list = lockTable.get(pid);
                    /*
                    if (allWait(list)) {
                        for (int i = 0; i < list.size(); i += 1) {
                            if (list.get(i).equals(record)) {
                                removeLock(tid, record);
                                list.remove(i);
                                PageLock newLock = new PageLock(tid, pid, perm);
                                list.add(0, newLock);
                                addLock(tid, newLock);
                                return newLock;
                            }
                        }
                    }
                     */
                    if (list.get(0).equals(record)) {
                        removeLock(tid, record);
                        list.remove(0);
                        PageLock newlock = new PageLock(tid, pid, perm);
                        list.add(newlock);
                        addLock(tid,newlock);
                        return newlock;
                    }
                    if (allWait(list)) {
                        PageLock pageLock = hasRecord(tid, pid);
                        for (int i = 0; i < list.size(); i += 1) {
                            if (list.get(i).equals(pageLock)) {
                                list.remove(pageLock);
                                removeLock(tid, pageLock);
                            }
                        }
                        PageLock newLock = new PageLock(tid, pid, perm);
                        list.add(0, newLock);
                        addLock(tid, newLock);
                        return newLock;
                    }
                    break;
                }
            }

        } else {
            // The page has no lock
            if (!lockTable.containsKey(pid) || lockTable.get(pid).size() == 0) {
                List<PageLock> list = new LinkedList<>();
                PageLock newLock = new PageLock(tid, pid, perm);
                list.add(newLock);
                lockTable.put(pid, list);
                addLock(tid, newLock);
                return newLock;
            }
            //The page has lock list
            List<PageLock> list = lockTable.get(pid);
            switch (perm) {
                case READ_ONLY : {
                    if (allShare(list)) {
                        PageLock pageLock = new PageLock(tid, pid, PageLock.LockType.SHARED_LOCK);
                        list.add(pageLock);
                        addLock(tid, pageLock);
                        return pageLock;
                    } else {
                        PageLock waitLock = addWaitForLock(tid, pid, perm);
                        return waitLock;
                    }
                }
                case READ_WRITE: {
                    PageLock waitLock = addWaitForLock(tid, pid, perm);
                    return waitLock;
                }
            }
        }
        return record;
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
