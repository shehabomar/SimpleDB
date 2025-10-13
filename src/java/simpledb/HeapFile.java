package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *          the file that stores the on-disk backing store for this heap
     *          file.
     */

    File f;
    TupleDesc td;

    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageSize = BufferPool.getPageSize();
        int pageNumber = pid.getPageNumber();

        // Check if the page number is valid
        if (pageNumber < 0 || pageNumber >= numPages()) {
            throw new IllegalArgumentException("Page does not exist in this file");
        }

        int offset = pageNumber * pageSize;
        byte[] data = new byte[pageSize];

        try {
            RandomAccessFile raf = new RandomAccessFile(f, "r");
            raf.seek(offset);

            // Read available bytes (may be less than a full page for the last page)
            int bytesRead = 0;
            int totalBytesRead = 0;
            while (totalBytesRead < pageSize) {
                bytesRead = raf.read(data, totalBytesRead, pageSize - totalBytesRead);
                if (bytesRead == -1) {
                    // EOF reached, fill rest with zeros
                    break;
                }
                totalBytesRead += bytesRead;
            }

            raf.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new IllegalArgumentException("Page does not exist in this file");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        long fileSize = f.length();
        int pageSize = BufferPool.getPageSize();
        return (int) Math.ceil((double) fileSize / pageSize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    /**
     * Inner class that implements DbFileIterator for HeapFile
     */
    private class HeapFileIterator implements DbFileIterator {
        private TransactionId tid;
        private int currentPageNumber;
        private Iterator<Tuple> currentPageIterator;
        private boolean isOpen;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            this.currentPageNumber = 0;
            this.currentPageIterator = null;
            this.isOpen = false;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.isOpen = true;
            this.currentPageNumber = 0;
            this.currentPageIterator = null;
            if (numPages() > 0) {
                loadCurrentPageIterator();
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                return false;
            }

            // If current page iterator has more tuples, return true
            if (currentPageIterator != null && currentPageIterator.hasNext()) {
                return true;
            }

            // Try to advance to next page with tuples
            while (currentPageNumber + 1 < numPages()) {
                currentPageNumber++;
                loadCurrentPageIterator();
                if (currentPageIterator != null && currentPageIterator.hasNext()) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException("No more tuples available");
            }
            return currentPageIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            this.isOpen = false;
            this.currentPageIterator = null;
            this.currentPageNumber = 0;
        }

        /**
         * Helper method to load the iterator for the current page
         */
        private void loadCurrentPageIterator() throws DbException, TransactionAbortedException {
            if (currentPageNumber < numPages()) {
                HeapPageId pageId = new HeapPageId(getId(), currentPageNumber);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                currentPageIterator = page.iterator();
            } else {
                currentPageIterator = null;
            }
        }
    }

}
