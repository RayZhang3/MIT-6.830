package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.*;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import static org.junit.Assert.assertTrue;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    public List<Long> cycleTransaction;
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    public static int numPages = DEFAULT_PAGES;
    Map<PageId, DlinkedNode> PagesMap;
    DlinkedNode sentinel;
    DlinkedNode endSentinel;
    private static LockManager lockManager;
    private static WaitForGraph waitForGraph;
    public LockManager getLockManager() {
        return lockManager;
    }
    public WaitForGraph getWaitForGraph() {
        return waitForGraph;
    }
    private class DlinkedNode {
        PageId pid;
        Page page;
        DlinkedNode prev;
        DlinkedNode next;
        DlinkedNode(){};
        DlinkedNode(PageId pid, Page page, DlinkedNode prev, DlinkedNode next) {
            this.pid = pid;
            this.page = page;
            this.prev = prev;
            this.next = next;
        }
        public PageId getPageId() {
            return this.pid;
        }
        public Page getPage() {
            return this.page;
        }

    }
    public synchronized DlinkedNode deleteNode(DlinkedNode node) {
        node.next.prev = node.prev;
        node.prev.next = node.next;
        return node;
    }
    public synchronized void addHeadNode(DlinkedNode node) {
        node.prev = sentinel;
        node.next = sentinel.next;
        sentinel.next.prev = node;
        sentinel.next = node;
    }
    public synchronized DlinkedNode removeLast() {
        DlinkedNode targetNode = endSentinel.prev;
        return deleteNode(targetNode);
    }
    public synchronized void moveToFirst(DlinkedNode node) {
        deleteNode(node);
        addHeadNode(node);
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
         this.PagesMap = new ConcurrentHashMap<>();
         this.numPages = numPages;
         this.sentinel = new DlinkedNode();
         this.endSentinel = new DlinkedNode();
         this.sentinel.next = endSentinel;
         this.endSentinel.prev = sentinel;
         this.lockManager = new LockManager();
         this.waitForGraph = new WaitForGraph();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here

        boolean AccessPermission = false;
            while (!AccessPermission) {
                String processName = Thread.currentThread().getName();
                // print thread information
                //System.out.print(processName + " want to get new Page " + pid.getTableId() + "." + pid.getPageNumber() + " in tid: " + tid.getId());
                LockManager.PageLock pageLock = getLockManager().grantLock(tid, pid, perm);
                if (pageLock != null) {
                    switch (pageLock.lockType) {
                        case EXCLUSIVE_LOCK: {
                            AccessPermission = true;
                            break;
                        }
                        case SHARED_LOCK: {
                            if (perm == Permissions.READ_ONLY) {
                                AccessPermission = true;
                            }
                            break;
                        }
                        case WAITING: {
                            LockManager.PageLock lock = getLockManager().grantLock(tid, pid, perm);
                            if (lock == null) {
                                try {
                                    String thisprocessName = Thread.currentThread().getName();
                                    // print thread information
                                    //System.out.print(", and fail, so ");
                                    //System.out.println(thisprocessName + " waits");
                                    this.wait();
                                } catch (InterruptedException e){
                                    e.printStackTrace();
                                }
                            }
                            switch(lock.lockType) {
                                case EXCLUSIVE_LOCK : {
                                    AccessPermission = true;
                                    break;
                                }
                                case SHARED_LOCK: {
                                    AccessPermission = (perm == Permissions.READ_ONLY);
                                    break;
                                }
                                case WAITING:{
                                    try {
                                        String thisprocessName = Thread.currentThread().getName();
                                        // print thread information
                                        //System.out.print(", and fail, so ");
                                        //System.out.println(thisprocessName + " waits");
                                        this.wait();
                                    } catch (InterruptedException e){
                                        e.printStackTrace();
                                    }
                                }
                            }
                        }
                    }
                }
            }
            /*
            if (cycleTransaction != null) {
                if (cycleTransaction.contains(tid.getId())) {
                    cycleTransaction.remove(tid);
                    transactionComplete(tid, false);
                }
            }
             */
            //System.out.println(", and success");
            assertTrue(AccessPermission);

            if (PagesMap.containsKey(pid)) {
                DlinkedNode node = PagesMap.get(pid);
                moveToFirst(node);
                return node.getPage();
            } else { // The page is not in PageMap
                // The capacity is full, evict page
                if (PagesMap.size() >= numPages) {
                    evictPage();
                }
                DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
                Page page = file.readPage(pid);
                DlinkedNode node = new DlinkedNode(pid, page, null, null);
                addHeadNode(node);
                PagesMap.put(pid, node);

                return node.getPage();
            }

    }

    private boolean isPageLocked(PageId pid) {
        return this.lockManager.isPageLocked(pid);
    }


    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        this.lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return this.lockManager.hasLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        String thisprocessName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        synchronized (this) {
            //List<PageId> PageIds = this.lockManager.transactionRWPageIds(tid);
            List<PageId> PageIds = this.lockManager.transactionPageIds(tid);
            if (commit) {
                try {
                    for (PageId pid: PageIds) {
                        if (PagesMap.containsKey(pid)) {
                            Page targetPage = PagesMap.get(pid).getPage();
                            // use current page contents as the before-image
                            // for the next transaction that modifies this page.
                            flushPage(pid);
                            targetPage.setBeforeImage();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                //restore the dirty page to its on-disk state
                if (PageIds != null){
                    for (PageId pid: PageIds) {
                        if (!PagesMap.containsKey(pid)) {
                            continue;
                        }
                        DlinkedNode dirtyNode = PagesMap.get(pid);
                        Page dirtyPage = dirtyNode.getPage();
                        if (dirtyPage != null &&  dirtyPage.isDirty() != null && dirtyPage.isDirty().equals(tid)) {
                            DbFile file = Database.getCatalog().getDatabaseFile(dirtyPage.getId().getTableId());
                            Page newPage = file.readPage(dirtyPage.getId());
                            newPage.markDirty(false, null);
                            DlinkedNode newNode = new DlinkedNode(pid, newPage, null, null);
                            deleteNode(dirtyNode);
                            addHeadNode(newNode);
                            PagesMap.remove(pid);
                            PagesMap.put(pid, newNode);
                        }
                    }
                }
            }
            this.lockManager.releaseLock(tid);
            // print thread information
            // System.out.println(thisprocessName + " complete " + tid.getId());
            this.waitForGraph.removeVertex(tid);
            // print thread information
            //System.out.println(thisprocessName + " finished remove Vertex" + tid.getId());
            this.notifyAll();
        }

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = file.insertTuple(tid, t);
        updateBufferPool(dirtyPages, tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> dirtyPages = file.deleteTuple(tid, t);
        updateBufferPool(dirtyPages, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid: PagesMap.keySet()) {
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        if (PagesMap.containsKey(pid)) {
            DlinkedNode targetNode = PagesMap.get(pid);
            deleteNode(targetNode);
            PagesMap.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        if (PagesMap.containsKey(pid)){
            Page targetPage = PagesMap.get(pid).getPage();
            // append an update record to the log, with
            // a before-image and after-image.
            TransactionId dirtier = targetPage.isDirty();
            if (dirtier != null){
                Database.getLogFile().logWrite(dirtier, targetPage.getBeforeImage(), targetPage);
                Database.getLogFile().force();
            }

            if (targetPage.isDirty() != null) {
                DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
                file.writePage(targetPage);
                targetPage.markDirty(false, null);
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        List<PageId> RWPageIds= this.lockManager.transactionRWPageIds(tid);
        if (RWPageIds == null) {
            return;
        }
        for (PageId pid: RWPageIds) {
            if (this.PagesMap.containsKey(pid)) {
                Page page = PagesMap.get(pid).getPage();
                if (page.isDirty() != null && page.isDirty().equals(tid)) {
                    flushPage(pid);
                    // use current page contents as the before-image
                    // for the next transaction that modifies this page.
                    page.setBeforeImage();
                }
            }
        }
    }

    private synchronized DlinkedNode findEvictedPage() throws DbException {
        DlinkedNode node = endSentinel.prev;
        while (node != this.sentinel) {
            //if (node.getPage().isDirty() != null && isPageLocked(node.getPageId())) {
            // Page is dirty, skip
            if (node.getPage().isDirty() == null) {
                return node;
            } else {
                node = node.prev;
            }
        }
        assertTrue(node != null);
        throw new DbException("All pages are dirty!");
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        DlinkedNode node = findEvictedPage();
        PageId pid = null;
        try {
            pid = node.getPageId();
            flushPage(pid);
            deleteNode(node);
            PagesMap.remove(pid);
            this.lockManager.releaseLock(pid);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private synchronized void updateBufferPool(List<Page> dirtyPages, TransactionId tid) throws DbException {
        for (Page page: dirtyPages) {
            page.markDirty(true, tid);
            if (PagesMap.containsKey(page.getId())) {
                DlinkedNode oldNode = PagesMap.get(page.getId());
                DlinkedNode newNode = new DlinkedNode(page.getId(), page, null, null);
                deleteNode(oldNode);
                PagesMap.remove(oldNode);
                addHeadNode(newNode);
                PagesMap.put(newNode.getPageId(), newNode);
            } else {
                if (PagesMap.size() >= numPages) {
                    evictPage();
                }
                DlinkedNode node = new DlinkedNode(page.getId(), page, null, null);
                addHeadNode(node);
                PagesMap.put(node.getPageId(), node);
            }
        }
    }
}
