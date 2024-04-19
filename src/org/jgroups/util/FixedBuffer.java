package org.jgroups.util;

import org.jgroups.annotations.GuardedBy;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Ring buffer of fixed capacity. Indices low and high point to the beginning and end of the buffer. Sequence numbers
 * (seqnos) are mapped to an index by <pre>seqno % capacity</pre>. High can never pass low, and drops or blocks when
 * that's the case.<br/>
 * Note that 'null' is not a valid element.<br/>
 * The design is described in doc/design/FixedBuffer.txt.
 * @author Bela Ban
 * @since  5.4
 */
public class FixedBuffer<T> extends Buffer<T> implements Closeable {
    /** Element array. Should always be sized to a power of 2. */
    protected final T[]       buf;

    /** The lowest seqno. Moved forward by remove and purge */
    protected long            low;

    /** The highest seqno. Moved forward by add(). The next message to be added is high+1 */
    protected long            high;

    protected final long      offset;

    protected final Condition buffer_full=lock.newCondition();

    /** Used to unblock blocked senders on close() */
    protected boolean         open=true;

    public FixedBuffer() {
        this(0);
    }

    public FixedBuffer(long offset) {
        this(16, offset);
    }

    /**
     * Creates a RingBuffer
     * @param capacity The number of elements the ring buffer's array should hold.
     * @param offset The offset. The first element to be added has to be offset +1.
     */
    public FixedBuffer(int capacity, long offset) {
        if(capacity < 1)
            throw new IllegalArgumentException("incorrect capacity of " + capacity);
        if(offset < 0)
            throw new IllegalArgumentException("invalid offset of " + offset);

        // Find a power of 2 >= buffer capacity
        int cap=1;
        while(capacity > cap)
           cap <<= 1;

        this.buf=(T[])new Object[cap];
        this.low=this.high=this.offset=offset;
    }

    @Override
    public long      offset()              {return offset;}
    public final int capacity()            {return buf.length;}
    @Override
    public long      low()                 {return low;}
    public long      high()                {return high;}

    @Override
    public long      getHighestDelivered() {return low;}

    @Override
    public long      getHighestReceived()  {return high;}

    // todo: maintain 'size' field to avoid linear cost of iteration
    public int       size()                {return count(false);}
    @Override
    public boolean   isEmpty()             {return size() == 0;}
    @Override
    public int       numMissing()          {return count(true);}

    public int       spaceUsed()           {return (int)(high - low);}

    public double    saturation() {
        int space=spaceUsed();
        return space == 0? 0.0 : space / (double)capacity();
    }

    @Override
    public int computeSize() {
        return (int)stream().filter(Objects::nonNull).count();
    }

    @Override
    public void resetStats() {

    }

    public boolean add(long seqno, T element) {
        return add(seqno, element, false);
    }

    @Override
    public boolean add(long seqno, T element, Predicate<T> remove_filter) {
        return false;
    }

    @Override
    public boolean add(List<LongTuple<T>> list) {
        return false;
    }

    @Override
    public boolean add(List<LongTuple<T>> list, boolean remove_added_elements) {
        return false;
    }

    @Override
    public boolean add(List<LongTuple<T>> list, boolean remove_added_elements, T const_value) {
        return false;
    }

    @Override
    public boolean add(MessageBatch batch, Function<T,Long> seqno_getter) {
        return false;
    }

    @Override
    public boolean add(MessageBatch batch, Function<T,Long> seqno_getter, boolean remove_from_batch, T const_value) {
        return false;
    }

    /**
     * Adds a new element to the buffer
     * @param seqno The seqno of the element
     * @param element The element
     * @param block If true, add() will block when the buffer is full until there is space. Else, add() will
     * return immediately, either successfully or unsuccessfully (if the buffer is full)
     * @return True if the element was added, false otherwise.
     */
    public boolean add(long seqno, T element, boolean block) {
        lock.lock();
        try {
            if(seqno <= low)
                return false;

            if(seqno - low > capacity() && (!block || !block(seqno)))  // seqno too big
                return false;

            int index=index(seqno);
            if(buf[index] != null)
                return false;
            buf[index]=element;

            // see if high needs to moved forward
            if(seqno > high)
                high=seqno;
            return true;
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Removes the next non-null element and advances low
     * @return T if there was a non-null element at low+1, otherwise null
     */
    public T remove() {
        lock.lock();
        try {
            long tmp=low+1;
            if(tmp > high)
                return null;
            int index=index(tmp);
            T element=buf[index];
            if(element != null) {
                low=tmp;
                buf[index]=null;
                buffer_full.signalAll();
            }
            return element;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public T remove(boolean nullify) {
        return null;
    }

    @Override
    public List<T> removeMany(boolean nullify, int max_results) {
        return removeMany(nullify, max_results, null);
    }

    @Override
    public List<T> removeMany(boolean nullify, int max_results, Predicate<T> filter) {
        return null;
    }

    @Override
    public <R> R removeMany(boolean nullify, int max_results, Predicate<T> filter, Supplier<R> result_creator, BiConsumer<R,T> accumulator) {
        return null;
    }


    public List<T> removeMany(int max_results) {
        List<T> list=null;
        int num_results=0;

        lock.lock();
        try {
            long start=low;
            while(start+1 <= high) {
                int index=index(start+1);
                T element=buf[index];
                if(element == null)
                    break;
                if(list == null)
                    list=new ArrayList<>(max_results > 0? max_results : 20);
                list.add(element);
                buf[index]=null;
                start++;
                if(max_results > 0 && ++num_results >= max_results)
                    break;
            }
            if(start > low) {
                low=start;
                buffer_full.signalAll();
            }
            return list;
        }
        finally {
            lock.unlock();
        }
    }

    public T get(long seqno) {
        lock.lock();
        try {
            if(seqno <= low || seqno > high)
                return null;
            int index=index(seqno);
            return buf[index];
        }
        finally {
            lock.unlock();
        }
    }

    /** Only used for testing !! */
    public T _get(long seqno) {
        int index=index(seqno);
        lock.lock();
        try {
            return index < 0? null : buf[index];
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Only for testing: returns a list of messages in the range [from .. to], including from and to
     * @param from
     * @param to
     * @return A list of messages, or null if none in range [from .. to] was found
     */
    public List<T> get(long from, long to) {
        if(from > to)
            throw new IllegalArgumentException("from (" + from + ") has to be <= to (" + to + ")");
        List<T> retval=null;
        for(long i=from; i <= to; i++) {
            T element=get(i);
            if(element != null) {
                if(retval == null)
                    retval=new ArrayList<>();
                retval.add(element);
            }
        }
        return retval;
    }


    /** Nulls elements between low and seqno and forwards low. Returns the number of nulled elements */
    public int purge(long seqno) {
        int count=0;
        lock.lock();
        try {
            long tmp=low;
            long from=low+1, to=Math.min(seqno, high);
            for(long i=from; i <= to; i++) {
                int index=index(i);
                if(buf[index] != null) {
                    buf[index]=null;
                    count++;
                }
                low++;
            }
            if(low > tmp)
                buffer_full.signalAll();
            return count;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public int purge(long seqno, boolean force) {
        return 0;
    }



    @Override
    public void forEach(long from, long to, Visitor<T> visitor, boolean nullify) {
        for(long i=from; i <= to; i++) {
            int index=index(i);
            T element=buf[index];
            boolean stop=visitor != null && !visitor.visit(from, element);
            if(nullify && element != null) {
                buf[index]=null;
                if(from - low > 0)
                    low=from;
            }
            if(stop)
                break;
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            open=false;
            buffer_full.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    public SeqnoList getMissing() {
        SeqnoList missing=null;
        lock.lock();
        try {
            for(long i=low + 1; i <= high; i++) { // <= high: correct as element at buf[high] will always be non-null!
                if(buf[index(i)] == null) {
                    if(missing == null)
                        missing=new SeqnoList((int)(high - low), low);
                    missing.add(i);
                }
            }
            return missing;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public SeqnoList getMissing(int max_msgs) {
        return null;
    }

    @Override
    public long[] getDigest() {
        return new long[0];
    }

    @Override
    public FixedBuffer<T> setHighestDelivered(long seqno) {
        lock.lock();
        try {
            low=seqno; // low == highest delivered
            return this;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Returns an iterator over the elements of the ring buffer in the range [LOW+1 .. HIGH]
     * @return FixedXmitWindowIterator
     * @throws NoSuchElementException is HD is moved forward during the iteration
     */
    public Iterator<T> iterator() {
        return new FixedXmitWindowIterator(buf);
    }

    @Override
    public Iterator<T> iterator(long from, long to) {
        return null;
    }

    @Override
    public Stream<T> stream() {
        Spliterator<T> sp=Spliterators.spliterator(iterator(), size(), 0);
        return StreamSupport.stream(sp, false);
    }

    @Override
    public Stream<T> stream(long from, long to) {
        Spliterator<T> sp=Spliterators.spliterator(iterator(from, to), size(), 0);
        return StreamSupport.stream(sp, false);
    }

    @Override
    protected boolean _add(long seqno, T element, boolean check_if_resize_needed, Predicate<T> remove_filter) {
        return false;
    }





    public String toString() {
        return String.format("[%,d | %,d] (%,d elements, %,d missing)", low, high, size(), numMissing());
    }

    @Override
    public String dump() {
        return null;
    }

    protected int index(long seqno) {
        // apparently this is faster than mod for n^2 capacity
        return (int)((seqno - offset -1) & (capacity() - 1));
    }

    @GuardedBy("lock")
    protected boolean block(long seqno) {
        while(open && seqno - low > capacity()) {
            try {
                buffer_full.await();
            }
            catch(InterruptedException e) {
            }
        }
        return open;
    }

    protected int count(boolean missing) {
        int retval=0;
        for(long i=low+1; i <= high; i++) {
            int index=index(i);
            T element=buf[index];
            if(missing && element == null)
                retval++;
            if(!missing && element != null)
                retval++;
        }
        return retval;
    }


    protected class FixedXmitWindowIterator implements Iterator<T> {
        protected final T[] buffer;
        protected long      current=low+1;

        public FixedXmitWindowIterator(T[] buffer) {
            this.buffer=buffer;
        }

        public boolean hasNext() {
            return current <= high;
        }

        public T next() {
            if(!hasNext()){
                throw new NoSuchElementException();
            }
            if(current <= low)
                current=low+1;
            return buffer[index(current++)];
        }

        public void remove() {}
    }

}
