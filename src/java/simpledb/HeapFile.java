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

    private final File file;
    private final TupleDesc tupleDesc;
    private final int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
        this.tableId = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
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
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageSize = BufferPool.getPageSize();
        int offset = pid.getPageNumber() * pageSize;

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            if (offset + pageSize > raf.length()) {
                throw new IllegalArgumentException("Page offset exceeds file size.");
            }

            byte[] pageData = new byte[pageSize];
            raf.seek(offset);
            raf.readFully(pageData);

            return new HeapPage((HeapPageId) pid, pageData);
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to read page: " + e.getMessage());
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            int offset = page.getId().getPageNumber() * BufferPool.getPageSize();
            raf.seek(offset);
            raf.write(page.getPageData());
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        long length = file.length();
        long pageSize = BufferPool.getPageSize();
        return (int) Math.ceil((double) length / pageSize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(tableId, i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                return new ArrayList<>(Collections.singletonList(page));
            }
        }

        HeapPageId newPid = new HeapPageId(tableId, numPages());
        HeapPage newPage = new HeapPage(newPid, HeapPage.createEmptyPageData());
        newPage.insertTuple(t);

        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(numPages() * BufferPool.getPageSize());
            raf.write(newPage.getPageData());
        }

        return new ArrayList<>(Collections.singletonList(newPage));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {

        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<>(Collections.singletonList(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        
        return new DbFileIterator() {
            private Iterator<Tuple> tupleIterator;
            private int currentPage;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                currentPage = 0;
                loadNextPage();
            }

            private void loadNextPage() throws TransactionAbortedException, DbException {
                while (currentPage < numPages()) {
                    HeapPageId pid = new HeapPageId(tableId, currentPage++);
                    HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                    tupleIterator = page.iterator();

                    if (tupleIterator.hasNext()) {
                        return;
                    }
                }
                tupleIterator = null;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (tupleIterator == null) {
                    return false;
                }
                if (!tupleIterator.hasNext()) {
                    loadNextPage();
                }
                return tupleIterator != null && tupleIterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return tupleIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                tupleIterator = null;
                currentPage = Integer.MAX_VALUE;
            }
        };
    }
}

