package org.jgroups.util;

import org.jgroups.annotations.GuardedBy;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Base class for message buffers. Used on the senders (keeping track of sent messages and purging
 * delivered messages) and receivers (delivering messages in the correct order and asking senders for missing messages).
 * @author Bela Ban
 * @since  5.4
 */
public abstract class Buffer<T> implements Iterable<T> {
    protected final Lock           lock=new ReentrantLock();
    protected final AtomicInteger  adders=new AtomicInteger(0);

    public Lock          lock()      {return lock;}
    public AtomicInteger getAdders() {return adders;}

    public abstract long offset();

    public abstract int capacity();

    public abstract int size();

    public abstract boolean isEmpty();

    public abstract long low();

    public abstract long high();

    public abstract long getHighestDelivered();
    public abstract long getHighestReceived();

    public abstract void resetStats();

    public abstract boolean add(long seqno, T element);

    public abstract boolean add(long seqno, T element, Predicate<T> remove_filter);

    public abstract boolean add(List<LongTuple<T>> list);

    public abstract boolean add(List<LongTuple<T>> list, boolean remove_added_elements);

    public abstract boolean add(List<LongTuple<T>> list, boolean remove_added_elements, T const_value);

    public abstract boolean add(MessageBatch batch, Function<T,Long> seqno_getter);

    public abstract boolean add(MessageBatch batch, Function<T,Long> seqno_getter, boolean remove_from_batch, T const_value);

    public abstract T get(long seqno);

    public abstract T _get(long seqno);

    public abstract T remove();

    public abstract T remove(boolean nullify);

    public abstract List<T> removeMany(boolean nullify, int max_results);

    public abstract List<T> removeMany(boolean nullify, int max_results, Predicate<T> filter);

    public abstract <R> R removeMany(boolean nullify, int max_results, Predicate<T> filter,
                                     Supplier<R> result_creator, BiConsumer<R,T> accumulator);

    /**
     * Removes all elements less than or equal to seqno from the table. Does this by nulling entire rows in the matrix
     * and nulling all elements < index(seqno) of the first row that cannot be removed
     * @param seqno
     */
    public int purge(long seqno) {
        return purge(seqno, false);
    }

    public abstract int purge(long seqno, boolean force);

    public void forEach(Visitor<T> visitor, boolean nullify) {
        forEach(getHighestDelivered()+1, getHighestReceived(), visitor, nullify);
    }

    public abstract void forEach(long from, long to, Visitor<T> visitor, boolean nullify);

    public abstract Iterator<T> iterator();

    public abstract Iterator<T> iterator(long from, long to);

    public abstract Stream<T> stream();

    public abstract Stream<T> stream(long from, long to);

    protected abstract boolean _add(long seqno, T element, boolean check_if_resize_needed, Predicate<T> remove_filter);

    @GuardedBy("lock")
    public abstract int computeSize();

    public abstract int numMissing();

    public abstract SeqnoList getMissing();

    public abstract SeqnoList getMissing(int max_msgs);

    public abstract long[] getDigest();

    public abstract <R extends Buffer<T>> R setHighestDelivered(long seqno);

    public abstract String toString();

    public abstract String dump();

    public interface Visitor<T> {
        /**
         * Iteration over the table, used by {@link DynamicBuffer#forEach(long, long, Buffer.Visitor, boolean)}.
         *
         * @param seqno   The current seqno
         * @param element The element at matrix[row][column]
         * @return True if we should continue the iteration, false if we should break out of the iteration
         */
        boolean visit(long seqno, T element);
    }

    /** Returns the number of messages that can be delivered */
    public int getNumDeliverable() {
        NumDeliverable visitor=new NumDeliverable();
        lock.lock();
        try {
            forEach(visitor, false);
            return visitor.getResult();
        }
        finally {
            lock.unlock();
        }
    }

    /** Returns the highest deliverable (= removable) seqno. This may be higher than {@link #getHighestDelivered()},
     * e.g. if elements have been added but not yet removed */
    public long getHighestDeliverable() {
        HighestDeliverable visitor=new HighestDeliverable();
        lock.lock();
        try {
            forEach(visitor, false);
            long retval=visitor.getResult();
            return retval == -1? getHighestDelivered() : retval;
        }
        finally {
            lock.unlock();
        }
    }

    protected class NumDeliverable implements Visitor<T> {
        protected int num_deliverable=0;

        public int getResult() {return num_deliverable;}

        public boolean visit(long seqno, T element) {
            if(element == null)
                return false;
            num_deliverable++;
            return true;
        }
    }

    protected class HighestDeliverable implements Visitor<T> {
        protected long highest_deliverable=-1;

        public long getResult() {return highest_deliverable;}

        public boolean visit(long seqno, T element) {
            if(element == null)
                return false;
            highest_deliverable=seqno;
            return true;
        }
    }
}
