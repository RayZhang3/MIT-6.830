package simpledb.storage;
import simpledb.common.Permissions;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.locks.*;
enum LockType {
    NO_LOCK, SHARED_LOCK, EXCLUSIVE_LOCK,
}
public class LockManager {
    public class PageLock{
        public TransactionId tid;
        public LockType type;
        public PageLock() {

        }
        public PageLock(TransactionId tid, LockType lockType) {
            this.tid = tid;
            this.type = lockType;
        }
    }

    // lock of lockManager
    private final ReentrantLock globalLock = new ReentrantLock();
    // WaitFor graph
    private final WaitForGraph waitForGraph = new WaitForGraph();

    // Condition for each PageID
    private final Map<PageId, List<PageLock>> pidToLocks = new ConcurrentHashMap<>();
    private final Map<TransactionId, Set<PageId>> tidToPids = new ConcurrentHashMap<>();
    private final Map<PageId, TransactionId> writeLockWating = new ConcurrentHashMap<>();
    // Wait and notify
    private final Map<TransactionId, Condition> conditionsMap = new ConcurrentHashMap<>();


    public boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        // try to acquire the lock
        globalLock.lock();
        try {
            if (canAcquireLock(tid, pid, perm)) {
                acquireLockWithoutChecking(tid, pid, perm);
                return true;
            } else {
                // else, calculate the dependency for wait-for graph
                /*
                List<TransactionId> lockHolders = getLockHolder(pid);
                for (TransactionId to : lockHolders) {
                    waitForGraph.addEdge(tid, to);
                }
                if (waitForGraph.detectDeadLock(tid, new HashSet<>())) {
                    // TODO: throw new abort error
                }
                if (!conditionsMap.containsKey(tid)) {
                    conditionsMap.put(tid, globalLock.newCondition());
                }
                conditionsMap.get(tid).await();
                 */
                return false;
            }
        } catch (Exception e) {
            // TODO: What exception?
            // throw new TransactionAbortedException();
            e.printStackTrace();
            return false;
        } finally {
            globalLock.unlock();
        }
    }
    public void releaseLock(TransactionId tid) {
        globalLock.lock();
        try {
            releaseAllLocks(tid);
            /*
            for (TransactionId waiter: waitForGraph.getTransactionNoNeedToWait()) {
                if (conditionsMap.containsKey(waiter)) {
                    conditionsMap.get(waiter).signalAll();
                }
            }
            // conditionsMap.remove(tid);
             */
            tidToPids.remove(tid);
        } finally {
            globalLock.unlock();
        }
    }

    public void releaseAllLocks (TransactionId tid) {
        if (tidToPids.containsKey(tid)) {
            Set<PageId> pageIdSet = tidToPids.get(tid);
            for (PageId pid: pageIdSet) {
                if (pidToLocks.containsKey(pid)) {
                    List<PageLock> lockList =  pidToLocks.get(pid);
                    Iterator<PageLock> iterator = lockList.iterator();
                    while (iterator.hasNext()) {
                        PageLock lock = iterator.next();
                        if (lock.tid.equals(tid)) {
                            iterator.remove();
                        }
                    }
                }
            }
        }
    }

    public LockType tidOwnLock(TransactionId tid, PageId pid) {
        if (!pidToLocks.containsKey(pid) || pidToLocks.get(pid).isEmpty()) {
            return LockType.NO_LOCK;
        } else {
            List<PageLock> lockList = pidToLocks.get(pid);
            Iterator<PageLock> it = lockList.iterator();
            while(it.hasNext()) {
                PageLock pageLock = it.next();
                if (pageLock.tid.equals(tid)) {
                    return pageLock.type;
                }
            }
            return LockType.NO_LOCK;
        }
    }

    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        if (!pidToLocks.containsKey(pid) || pidToLocks.get(pid).isEmpty()) {
            return;
        }
        List<PageLock> lockList = pidToLocks.get(pid);
        Iterator<PageLock> it = lockList.iterator();
        PageLock target = null;
        while(it.hasNext()) {
            PageLock pageLock = it.next();
            if (pageLock.tid.equals(tid)) {
                it.remove();
                // tid To pids
                tidToPids.get(tid).remove(pid);
                break;
            }
        }

    }
    // Search the Map<PageId, PageLock> by PageId
    // If the transaction holds no lock on this page, return false
    public boolean canAcquireLock (TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException{
        // No lock.
        if (!pidToLocks.containsKey(pid) || pidToLocks.get(pid).isEmpty()) {
            return true;
        }
        LockType ownLock = tidOwnLock(tid, pid);
        List<PageLock> lockList = pidToLocks.get(pid);
        switch (perm) {
            case READ_ONLY : {
                if (ownLock == LockType.EXCLUSIVE_LOCK || ownLock == LockType.SHARED_LOCK) {
                    return true;
                } else {
                    // exist other's lock
                    return lockList.get(0).type == LockType.SHARED_LOCK;
                }
            }
            case READ_WRITE: {
                if (ownLock == LockType.EXCLUSIVE_LOCK) {
                    return true;
                } else {
                    if (lockList.size() == 1 && lockList.get(0).tid.equals(tid) &&
                            lockList.get(0).type == LockType.SHARED_LOCK) {
                        return true;
                    }
                    return false;
                }
            }
        }
        return false;
    }

    public void acquireLockWithoutChecking(TransactionId tid, PageId pid, Permissions perm) {
        if (!pidToLocks.containsKey(pid)) {
            pidToLocks.put(pid, new ArrayList<>());
        }
        // Add tid to pids.
        if (!tidToPids.containsKey(tid)) {
            tidToPids.put(tid, new HashSet<>());
        }
        tidToPids.get(tid).add(pid);

        List<PageLock> lockList = pidToLocks.get(pid);
        LockType ownLock = tidOwnLock(tid, pid);
        switch (perm) {
            case READ_ONLY: {
                if (ownLock == LockType.EXCLUSIVE_LOCK || ownLock == LockType.SHARED_LOCK) {
                    return;
                } else {
                    pidToLocks.get(pid).add(new PageLock(tid, LockType.SHARED_LOCK));
                }
                break;
            }
            case READ_WRITE: {
                if (ownLock == LockType.EXCLUSIVE_LOCK) {
                    return;
                } else if (ownLock == LockType.SHARED_LOCK) {
                    Iterator<PageLock> iterator = lockList.iterator();
                    while (iterator.hasNext()) {
                        PageLock lock = iterator.next();
                        if (lock.tid.equals(tid)) {
                            lock.type = LockType.EXCLUSIVE_LOCK;
                            // System.out.println("upgrade shared lock");
                        }
                    }
                } else {
                    pidToLocks.get(pid).add(new PageLock(tid, LockType.EXCLUSIVE_LOCK));
                }
            }
        }
    }


    /*
    public List<TransactionId> calculateDependency (TransactionId tid, PageId pid, Permissions perm) {
    }
     */
}
