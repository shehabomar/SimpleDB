package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    private ConcurrentHashMap<PageId, Page> bufferPool;
    private ConcurrentHashMap<PageId, Boolean> referenceBits;
    private ArrayList<PageId> clockQueue;
    private int clockHand;
    private int numPages;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */

    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.bufferPool = new ConcurrentHashMap<PageId, Page>();
        this.referenceBits = new ConcurrentHashMap<PageId, Boolean>();
        this.clockQueue = new ArrayList<PageId>();
        this.clockHand = 0;
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
     * The retrieved page should be looked up in the buffer pool. If it
     * is present, it should be returned. If it is not present, it should
     * be added to the buffer pool and returned. If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

        if (bufferPool.containsKey(pid)) {
            // set reference bit to true when page is accessed
            referenceBits.put(pid, true);
            return bufferPool.get(pid);
        } else {
            // check if cache is full and evict if necessary
            if (bufferPool.size() >= numPages) {
                evictPage();
            }

            Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            bufferPool.put(pid, page);

            // initialize reference bit and add to clock queue
            referenceBits.put(pid, true);
            clockQueue.add(pid);

            return page;
        }
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
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> modifiedPages = file.insertTuple(tid, t);

        // mark all modified pages as dirty and update them in buffer pool
        for (Page page : modifiedPages) {
            page.markDirty(true, tid);
            bufferPool.put(page.getId(), page);
        }
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
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        RecordId rid = t.getRecordId();
        if (rid == null) {
            throw new DbException("Tuple does not have a RecordId");
        }

        int tableId = rid.getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> modifiedPages = file.deleteTuple(tid, t);

        // mark all modified pages as dirty and update them in buffer pool
        for (Page page : modifiedPages) {
            page.markDirty(true, tid);
            bufferPool.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : bufferPool.keySet()) {
            flushPage(pid);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * 
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        bufferPool.remove(pid);
        referenceBits.remove(pid);
        clockQueue.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = bufferPool.get(pid);
        if (page == null) {
            return;
        }

        // write dirty page to disk
        TransactionId dirtier = page.isDirty();
        if (dirtier != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool using the Clock eviction algorithm.
     * The Clock algorithm approximates LRU by maintaining a reference bit for each
     * page.
     * 
     * Algorithm:
     * 1. Start at the current clock hand position
     * 2. If the page at clock hand has reference bit = 0 and is not dirty, evict it
     * 3. If the page has reference bit = 1, set it to 0 and move to next page
     * 4. If the page is dirty, skip it on first pass, flush and evict on second
     * pass if needed
     * 5. Move clock hand forward and repeat
     * 
     * This gives recently accessed pages a "second chance" before eviction.
     */
    private synchronized void evictPage() throws DbException {
        if (clockQueue.isEmpty()) {
            throw new DbException("No pages to evict");
        }

        int queueSize = clockQueue.size();
        int attempts = 0;
        int maxAttempts = queueSize * 2; // Two full passes

        while (attempts < maxAttempts) {
            // wrap around if needed
            if (clockHand >= clockQueue.size()) {
                clockHand = 0;
            }

            PageId pid = clockQueue.get(clockHand);
            Page page = bufferPool.get(pid);

            // check if page was removed (shouldn't happen, but defensive)
            if (page == null) {
                clockQueue.remove(clockHand);
                continue;
            }

            Boolean refBit = referenceBits.get(pid);
            boolean isDirty = (page.isDirty() != null);

            // clock algorithm logic
            if (refBit != null && refBit) {
                // reference bit is set - give it a second chance
                referenceBits.put(pid, false);
                clockHand++;
                attempts++;
            } else if (!isDirty) {
                // reference bit is 0 and page is clean - evict this page
                try {
                    flushPage(pid); // flush in case (though it's clean)
                } catch (IOException e) {
                    throw new DbException("Error flushing page during eviction: " + e.getMessage());
                }

                bufferPool.remove(pid);
                referenceBits.remove(pid);
                clockQueue.remove(clockHand);
                return;
            } else {
                // page is dirty - skip on first pass, but clear reference bit
                referenceBits.put(pid, false);
                clockHand++;
                attempts++;

                // on second pass through, evict dirty pages if necessary
                if (attempts >= queueSize) {
                    try {
                        flushPage(pid);
                    } catch (IOException e) {
                        throw new DbException("Error flushing page during eviction: " + e.getMessage());
                    }

                    bufferPool.remove(pid);
                    referenceBits.remove(pid);
                    clockQueue.remove(clockHand);
                    return;
                }
            }
        }

        // If we get here, something went wrong - just evict the page at clock hand
        if (!clockQueue.isEmpty()) {
            if (clockHand >= clockQueue.size()) {
                clockHand = 0;
            }
            PageId pid = clockQueue.get(clockHand);
            try {
                flushPage(pid);
            } catch (IOException e) {
                throw new DbException("Error flushing page during eviction: " + e.getMessage());
            }
            bufferPool.remove(pid);
            referenceBits.remove(pid);
            clockQueue.remove(clockHand);
        }
    }

}
