package org.jgroups.util;

import org.jgroups.Message;
import org.jgroups.annotations.GuardedBy;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base class for message buffers. Used on the senders (keeping track of sent messages and purging
 * delivered messages) and receivers (delivering messages in the correct order and asking senders for missing messages).
 * @author Bela Ban
 * @since  5.4
 */
public abstract class Buffer<T> implements Iterable<T> {
    protected final Lock          lock=new ReentrantLock();
    protected final AtomicInteger adders=new AtomicInteger(0);
    protected long                offset;
    protected long                low;
    /** The number of non-null elements */
    protected int                 size;

    public Lock          lock()      {return lock;}
    public AtomicInteger getAdders() {return adders;}
    public long          offset()    {return offset;}
    public long          low()       {return low;}
    public int           size()      {return size;}
    public boolean       isEmpty()   {return size <= 0;}

    // used
    /** Returns the current capacity in the buffer. This value is fixed in a fixed-size buffer
     * (e.g. {@link FixedBuffer}), but can change in a dynamic buffer ({@link DynamicBuffer}) */
    public abstract int capacity();

    public abstract long high();

    // used in setting the digest
    public abstract long getHighestDelivered();

    // used in stable()
    public abstract long getHighestReceived();

    // used
    public abstract void resetStats();

    /**
     * Adds an element if the element at the given index is null. Returns true if no element existed at the given index,
     * else returns false and doesn't set the element.
     * @param seqno
     * @param element
     * @return True if the element at the computed index was null, else false
     */
    // used: single message received
    public boolean add(long seqno, T element) {
        return add(seqno, element, null, Options.DEFAULT());
    }

    /**
     * Adds an element if the element at the given index is null. Returns true if no element existed at the given index,
     * else returns false and doesn't set the element.
     *
     * @param seqno
     * @param element
     * @param remove_filter A filter used to remove all consecutive messages passing the filter (and non-null). This
     *                      doesn't necessarily null a removed message, but may simply advance an index
     *                      (e.g. highest delivered). Ignored if null.
     * @param options
     * @return True if the element at the computed index was null, else false
     */
    // used: send message
    public abstract boolean add(long seqno, T element, Predicate<T> remove_filter, Options options);

    // used: MessageBatch received
    public abstract boolean add(MessageBatch batch, Function<T,Long> seqno_getter, boolean remove_from_batch, T const_value);

    // used: retransmision etc
    public abstract T get(long seqno);

    public abstract T _get(long seqno);

    public abstract T remove();

    public abstract T remove(boolean nullify);

    public List<T> removeMany(boolean nullify, int max_results) {
        return removeMany(nullify, max_results, null);
    }

    public abstract List<T> removeMany(boolean nullify, int max_results, Predicate<T> filter);

    // used in removeAndDeliver()
    public abstract <R> R removeMany(boolean nullify, int max_results, Predicate<T> filter,
                                     Supplier<R> result_creator, BiConsumer<R,T> accumulator);

    /**
     * Removes all elements less than or equal to seqno from the table. Does this by nulling entire rows in the matrix
     * and nulling all elements < index(seqno) of the first row that cannot be removed
     * @param seqno
     */
    // used: in stable() [NAKACK3 only]
    public int purge(long seqno) {
        return purge(seqno, false);
    }

    public abstract int purge(long seqno, boolean force);

    public void forEach(Visitor<T> visitor, boolean nullify) {
        forEach(getHighestDelivered()+1, getHighestReceived(), visitor, nullify);
    }

    public abstract void forEach(long from, long to, Visitor<T> visitor, boolean nullify);

    public abstract Iterator<T> iterator(long from, long to);

    // used
    public abstract Stream<T> stream();

    public abstract Stream<T> stream(long from, long to);

    @GuardedBy("lock")
    public abstract int computeSize();

    // used
    public abstract int numMissing();

    /**
     * Returns a list of missing (= null) elements
     * @return A SeqnoList of missing messages, or null if no messages are missing
     */
    public SeqnoList getMissing() {
        return getMissing(0);
    }

    // used in retransmission
    public abstract SeqnoList getMissing(int max_msgs);

    // used
    public abstract long[] getDigest();

    // used: in setting the digest
    public abstract <R extends Buffer<T>> R setHighestDelivered(long seqno);

    /** Dumps all non-null messages (used for testing) */
    public String dump() {
        lock.lock();
        try {
            return stream(low(), getHighestReceived()).filter(Objects::nonNull).map(Object::toString)
              .collect(Collectors.joining(", "));
        }
        finally {
            lock.unlock();
        }
    }

    public static class Options {
        protected boolean              block;
        protected boolean              remove_from_batch;
        protected Predicate<Message>   remove_filter;
        protected static final Options DEFAULT=new Options();
        public static Options     DEFAULT() {return DEFAULT;}
        public boolean            block()                            {return block;}
        public Options            block(boolean block)               {this.block=block;return this;}
        public boolean            removeFromBatch()                  {return remove_from_batch;}
        public Options            removeFromBatch(boolean r)         {this.remove_from_batch=r; return this;}
        public Predicate<Message> remove_filter()                    {return remove_filter;}
        public Options            removeFilter(Predicate<Message> f) {this.remove_filter=f; return this;}

        @Override
        public String toString() {
            return String.format("block=%b remove_from_batch=%b", block, remove_from_batch);
        }
    }

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
    // used in retransmissions
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

    protected class Missing implements Visitor<T> {
        protected final SeqnoList missing_elements;
        protected final int       max_num_msgs;
        protected int             num_msgs;

        protected Missing(long start, int max_number_of_msgs) {
            missing_elements=new SeqnoList(max_number_of_msgs, start);
            this.max_num_msgs=max_number_of_msgs;
        }

        protected SeqnoList getMissingElements() {return missing_elements;}

        public boolean visit(long seqno, T element) {
            if(element == null) {
                if(++num_msgs > max_num_msgs)
                    return false;
                missing_elements.add(seqno);
            }
            return true;
        }
    }
}
