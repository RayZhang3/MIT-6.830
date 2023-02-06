package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;

import javax.xml.crypto.Data;
import java.nio.Buffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import simpledb.transaction.TransactionId;
import simpledb.common.Permissions;

/**
 * Returns an iterator over all the tuples stored in this DbFile. The
 * iterator must use {@link BufferPool#getPage}
 *
 * @return an iterator over all the tuples stored in this DbFile.
 */

public class HeapFileIterator implements DbFileIterator{
    private final HeapFile file;
    private final TransactionId tid;
    private Iterator<Tuple> PageIterator;
    int currPage = 0;
    int currIndex = 0;

    HeapFileIterator(HeapFile file, TransactionId tid){
        this.file = file;
        this.tid = tid;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        HeapPageId pid = new HeapPageId(file.getId(), currPage);
        try {
            HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            this.PageIterator = hp.iterator();
        } catch(ClassCastException e) {
            e.printStackTrace();
        }

        if (Database.getBufferPool() == null) {
            throw new DbException("The BufferPool is null");
        }
        if (tid == null) {
            // Implement in other Labs
            throw new TransactionAbortedException();
        }
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (PageIterator == null) {
            return false;
        }
        while (true) {
            if (PageIterator.hasNext()) {
                return true;
            }
            currPage += 1;
            if (currPage >= file.numPages()) {
                PageIterator = null;
                return false;
            }
            PageId pid = new HeapPageId(file.getId(), currPage);
            try {
                HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                this.PageIterator = hp.iterator();
                return hasNext();
            } catch(ClassCastException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        // if the pageIterator is close.
        if (PageIterator == null) {
            throw new NoSuchElementException();
        }
        while (true) {
            if (PageIterator.hasNext()) {

                Tuple tuple = PageIterator.next();
                //print
                // System.out.print(tuple.toString() + ">>");
                return tuple;
            }
            currPage += 1;
            //print
            // System.out.println();

            if (currPage >= file.numPages()) {
                PageIterator = null;
                throw new NoSuchElementException();
            }
            PageId pid = new HeapPageId(file.getId(), currPage);
            try {
                HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                this.PageIterator = hp.iterator();
            } catch(ClassCastException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.currPage = 0;
        open();
    }

    /**
     * Closes the iterator.
     */
    @Override
    public void close() {
        this.PageIterator = null;
    }
}
