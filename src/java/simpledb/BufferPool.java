package simpledb;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;
    private ConcurrentHashMap<PageId, Page> pool;
    private LockManager lock;

    private static class LockManager {

        private enum LockType {
            NO_LOCK,
            SHARED_LOCK,
            EXCLUSIVE_LOCK,
        }
    
        private class LockState {
            private LockType lockType;
            private final Condition condition;
            private final HashSet<TransactionId> ownerSet;
            private final HashSet<TransactionId> waiterSet;
            private final HashMap<TransactionId, ArrayList<TransactionId>> depGraph;
    
            public LockState(Lock lock, HashMap<TransactionId, ArrayList<TransactionId>> depGraph) {
                this.lockType = LockType.NO_LOCK;
                this.ownerSet = new HashSet<>();
                this.waiterSet = new HashSet<>();
                this.condition = lock.newCondition();
                this.depGraph = depGraph;
            }
    
            public HashSet<TransactionId> getOwners() {
                return ownerSet;
            }
    
            public boolean holdsLock(TransactionId tid) {
                return ownerSet.contains(tid);
            }
    
            public void release(TransactionId tid) {
                if (!ownerSet.contains(tid) || lockType == LockType.NO_LOCK) {
                    return;
                }
                if (lockType == LockType.EXCLUSIVE_LOCK) {
                    assert ownerSet.size() == 1;
                    ownerSet.clear();
                } else if (lockType == LockType.SHARED_LOCK) {
                    ownerSet.remove(tid);
                }
                if (ownerSet.isEmpty()) {
                    lockType = LockType.NO_LOCK;
                }
                // Remove tid from each waiting transaction’s dependency list.
                for (TransactionId waitingTid : waiterSet) {
                    ArrayList<TransactionId> list = depGraph.get(waitingTid);
                    assert list != null;
                    list.remove(tid);
                }
                condition.signalAll();
            }
    
            public boolean canGrantShared(TransactionId tid) {
                if (lockType == LockType.EXCLUSIVE_LOCK) {
                    return ownerSet.contains(tid);
                }
                return true;
            }
    
            public boolean canGrantExclusive(TransactionId tid) {
                if (lockType == LockType.EXCLUSIVE_LOCK) {
                    return ownerSet.contains(tid);
                } else if (lockType == LockType.SHARED_LOCK) {
                    return (ownerSet.size() == 1 && ownerSet.contains(tid));
                }
                return true;
            }
    
            public boolean acquireSharedLock(TransactionId tid) {
                if (!canGrantShared(tid)) {
                    return false;
                }
                lockType = LockType.SHARED_LOCK;
                if (!ownerSet.contains(tid)) {
                    ownerSet.add(tid);
                    for (TransactionId waitingTid : waiterSet) {
                        ArrayList<TransactionId> deps = depGraph.get(waitingTid);
                        if (deps == null) {
                            deps = new ArrayList<>();
                            depGraph.put(waitingTid, deps);
                        }
                        deps.add(tid);
                    }
                }
                return true;
            }
    
            public boolean acquireExclusiveLock(TransactionId tid) {
                if (!canGrantExclusive(tid)) {
                    return false;
                }
                lockType = LockType.EXCLUSIVE_LOCK;
                if (!ownerSet.contains(tid)) {
                    ownerSet.add(tid);
                    for (TransactionId waitingTid : waiterSet) {
                        ArrayList<TransactionId> deps = depGraph.get(waitingTid);
                        if (deps == null) {
                            deps = new ArrayList<>();
                            depGraph.put(waitingTid, deps);
                        }
                        deps.add(tid);
                    }
                }
                return true;
            }
    
            public void waitLock(boolean shared, TransactionId tid)
                    throws TransactionAbortedException {
                if (!waiterSet.contains(tid)) {
                    waiterSet.add(tid);
                    ArrayList<TransactionId> deps = depGraph.get(tid);
                    if (deps == null) {
                        deps = new ArrayList<>();
                        depGraph.put(tid, deps);
                    }
                    for (TransactionId owner : ownerSet) {
                        deps.add(owner);
                    }
                }
                try {
                    if (shared) {
                        while (!canGrantShared(tid)) {
                            condition.await(1, TimeUnit.SECONDS);
                        }
                    } else {
                        while (!canGrantExclusive(tid)) {
                            condition.await(1, TimeUnit.SECONDS);
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new TransactionAbortedException();
                } catch (UnsupportedOperationException uoe) {
                    throw new TransactionAbortedException();
                } catch (ThreadDeath td) {
                    throw new TransactionAbortedException();
                } finally {
                    waiterSet.remove(tid);
                    ArrayList<TransactionId> deps = depGraph.get(tid);
                    assert deps != null;
                    for (TransactionId owner : ownerSet) {
                        deps.remove(owner);
                    }
                }
            }
        }
    
        private final Lock globalLock;
        private final HashMap<PageId, LockState> lockStates;
        private final HashMap<TransactionId, ArrayList<TransactionId>> depGraph;
    
        public LockManager() {
            globalLock = new ReentrantLock();
            lockStates = new HashMap<>();
            depGraph = new HashMap<>();
        }
    
        private boolean checkConnected(TransactionId from, TransactionId to) {
            Queue<TransactionId> queue = new LinkedList<>();
            HashSet<TransactionId> visited = new HashSet<>();
            queue.add(from);
            visited.add(from);
            while (!queue.isEmpty()) {
                TransactionId current = queue.poll();
                ArrayList<TransactionId> neighbors = depGraph.get(current);
                if (neighbors == null) continue;
                for (TransactionId neighbor : neighbors) {
                    if (neighbor.equals(to)) {
                        return true;
                    }
                    if (!visited.contains(neighbor)) {
                        queue.add(neighbor);
                        visited.add(neighbor);
                    }
                }
            }
            return false;
        }
    
        private boolean detectDeadLock(LockState ls, TransactionId waiter) {
            for (TransactionId owner : ls.getOwners()) {
                if (checkConnected(owner, waiter)) {
                    return true;
                }
            }
            return false;
        }
    
        public boolean holdsLock(TransactionId tid, PageId pid) {
            globalLock.lock();
            try {
                LockState ls = lockStates.get(pid);
                return ls != null && ls.holdsLock(tid);
            } finally {
                globalLock.unlock();
            }
        }
    
        public void release(TransactionId tid, PageId pid) {
            globalLock.lock();
            try {
                LockState ls = lockStates.get(pid);
                if (ls != null) {
                    ls.release(tid);
                }
            } finally {
                globalLock.unlock();
            }
        }
    
        public void acquireSharedLock(TransactionId tid, PageId pid)
                throws TransactionAbortedException {
            globalLock.lock();
            LockState ls = lockStates.get(pid);
            if (ls == null) {
                ls = new LockState(globalLock, depGraph);
                lockStates.put(pid, ls);
            }
            while (!ls.acquireSharedLock(tid)) {
                if (detectDeadLock(ls, tid)) {
                    globalLock.unlock();
                    throw new TransactionAbortedException();
                } else {
                    ls.waitLock(true, tid);
                }
            }
            globalLock.unlock();
        }
    
        public void acquireExclusiveLock(TransactionId tid, PageId pid)
                throws TransactionAbortedException {
            globalLock.lock();
            LockState ls = lockStates.get(pid);
            if (ls == null) {
                ls = new LockState(globalLock, depGraph);
                lockStates.put(pid, ls);
            }
            while (!ls.acquireExclusiveLock(tid)) {
                if (detectDeadLock(ls, tid)) {
                    globalLock.unlock();
                    throw new TransactionAbortedException();
                } else {
                    ls.waitLock(false, tid);
                }
            }
            globalLock.unlock();
        }
    }
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pool = new ConcurrentHashMap<>();
        this.lock = new LockManager();
    }
    
    public static int getPageSize() {
        return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }
    
    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.

     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned. If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // Simply acquire the lock. If it times out, let the exception propagate.
        lock.acquireSharedLock(tid, pid);

        Page cachedPage = pool.get(pid);
        if (cachedPage != null) {
            return cachedPage;
        }

        if (pool.size() >= numPages) {
            evictPage();
        }

        DbFile databaseFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page newPage = databaseFile.readPage(pid);
        pool.put(pid, newPage);

        return newPage;
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
        lock.release(tid, pid);
    }
    
    /**
     * Returns true if the specified transaction has a lock on the specified page.
     */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return lock.holdsLock(tid, pid);
    }
    
    /**
     * Commit or abort a given transaction; flush or revert dirty pages and
     * release all locks held by the transaction.
     *
     * @param tid the ID of the transaction
     * @param commit true if committing; false if aborting
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        if (commit) {
            // On commit, flush dirty pages and update their before images.
            for (PageId pid : new ArrayList<>(pool.keySet())) {
                Page p = pool.get(pid);
                if (p != null && tid.equals(p.isDirty())) {
                    flushPage(pid);
                    p.setBeforeImage();
                }
            }
        } else {
            // On abort, revert changes by replacing pages with their on-disk versions.
            for (PageId pid : new ArrayList<>(pool.keySet())) {
                Page p = pool.get(pid);
                if (p != null && tid.equals(p.isDirty())) {
                    pool.put(pid, p.getBeforeImage());
                }
            }
        }
        releaseTransactionLocks(tid);
    }

    // Helper method to release all locks held by a tid.
    private synchronized void releaseTransactionLocks(TransactionId tid) {
        // Iterate through all pages in the LockManager and release tid's lock.
        for (PageId pid : new ArrayList<>(lock.lockStates.keySet())) {
            lock.release(tid, pid);
        }
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
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
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> affectedPages = dbFile.insertTuple(tid, t);
        
        for (Page page : affectedPages) {
            page.markDirty(true, tid);
            pool.put(page.getId(), page);
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
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        
        DbFile dbFile = Database.getCatalog().getDatabaseFile(
            t.getRecordId().getPageId().getTableId());
        List<Page> affectedPages = dbFile.deleteTuple(tid, t);
        
        for (Page page : affectedPages) {
            page.markDirty(true, tid);
            pool.put(page.getId(), page);
        }
    }

    
    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : new ArrayList<>(pool.keySet())) {
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
        pool.remove(pid);
    }
    
    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = pool.get(pid);
        if (page == null) {
            throw new IOException("page not in buffer pool.");
        }
        TransactionId dirtier = page.isDirty();
        if (dirtier != null) {
            Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
            Database.getLogFile().force();
        }
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
    }
    
    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        Iterator<PageId> iterator = pool.keySet().iterator();
        while (iterator.hasNext()) {
            PageId pid = iterator.next();
            Page p = pool.get(pid);
            if (p.isDirty() == null) {
                iterator.remove();
                return;
            }
        }
        throw new DbException("All pages in the BufferPool are dirty. No pages available for eviction.");
    }
}