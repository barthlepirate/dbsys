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


        // The file on the disk that stores the data for this database table.

    private final File file;

        // The schema of the tuples within this file.

    private final TupleDesc td;


    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // Initialize with the parameters
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // Return the file member
        return file;
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
        // The hash code of the file path ensures uniqueness.
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // return the td member
        return td;
    }

        // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
         /**
         * Reads a page from the associated file based on the given PageId and returns a HeapPage.
         * Returns null if an IOException occurs.
         */
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            int pageSize = BufferPool.getPageSize();
            byte[] data = new byte[pageSize];
            long offset = (long) pid.getPageNumber() * pageSize;
            raf.seek(offset);
            raf.readFully(data);
            raf.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
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
        // Calculates and returns the number of pages in the associated file.
        long fileSize = file.length();
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
        return new DbFileIterator() {
            // Current page index to keep track of the iterator's position.
            private int currentPageIndex = 0;
            // Iterator over tuples in the current page.
            private Iterator<Tuple> currentIterator = null;

            /**
             * Opens the iterator, initializing the state for iteration.
             */
            public void open() throws DbException, TransactionAbortedException {
                currentPageIndex = 0;
                currentIterator = getIteratorForPage();
            }

            /**
             * Checks if there are more tuples to iterate over.
             * @return true if there are more tuples, false otherwise.
             */
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (currentIterator == null) {
                    return false;
                }
                if (!currentIterator.hasNext() && currentPageIndex < numPages() - 1) {
                    currentPageIndex++; // Move to the next page.
                    currentIterator = getIteratorForPage(); // Update iterator for new page.
                    return hasNext(); // Check again for the new page.
                }
                return currentIterator.hasNext(); // Return true if current page has more tuples.
            }

            /**
             * Retrieves the next tuple from the iterator.
             * @return the next tuple in the iteration.
             * @throws NoSuchElementException if there are no more tuples.
             */
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (currentIterator == null || !currentIterator.hasNext()) {
                    throw new NoSuchElementException();
                }
                return currentIterator.next();
            }

            /**
             * Resets the iterator to the start of the file.
             */
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            /**
             * Closes the iterator, resetting its state.
             */
            public void close() {
                currentIterator = null;
            }

            /**
             * Private helper method to get the tuple iterator for the current page.
             * @return an iterator over tuples in the current page.
             */
            private Iterator<Tuple> getIteratorForPage() throws TransactionAbortedException, DbException {
                if (currentPageIndex >= numPages()) {
                    throw new NoSuchElementException("No more pages in file.");
                }
                // Constructing the PageId for the current page.
                PageId pageId = new HeapPageId(getId(), currentPageIndex);
                // Fetching the page from the BufferPool.
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                // Returning the iterator for the fetched page.
                return page.iterator();
            }
        };
    }


}

