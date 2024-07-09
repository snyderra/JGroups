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
 * (seqnos) are mapped to an index by <pre>seqno % capacity</pre>. High can never pass low, and drops the element
 * or blocks when that's the case.<br/>
 * Note that 'null' is not a valid element, but signifies a missing element<br/>
 * The design is described in doc/design/FixedBuffer.txt.
 * @author Bela Ban
 * @since  5.4
 */
public class FixedBuffer<T> extends Buffer<T> implements Closeable {
    /** Holds the elements */
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
        this(32, offset);
    }

    /**
     * Creates a RingBuffer
     *
     * @param capacity The number of elements the ring buffer's array should hold.
     * @param offset   The offset. The first element to be added has to be offset +1.
     */
    public FixedBuffer(int capacity, long offset) {
        if(capacity < 1)
            throw new IllegalArgumentException("incorrect capacity of " + capacity);

        // Find a power of 2 >= buffer capacity
        int cap=Util.getNextHigherPowerOfTwo(capacity);
        this.buf=(T[])new Object[cap];
        this.low=this.high=this.offset=offset;
    }

    @Override
    public long offset() {
        return offset;
    }

    public final int capacity() {
        return buf.length;
    }

    @Override
    public long low() {
        return low;
    }

    public long high() {
        return high;
    }

    @Override
    public long getHighestDelivered() {
        return low;
    }

    @Override
    public long getHighestReceived() {
        return high;
    }

    // todo: maintain 'size' field to avoid linear cost of iteration
    public int size() {
        return count(false);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public int numMissing() {
        return count(true);
    }

    public int spaceUsed() {
        return (int)(high - low);
    }

    public double saturation() {
        int space=spaceUsed();
        return space == 0? 0.0 : space / (double)capacity();
    }

    @Override
    public int computeSize() {
        return count(false);
    }

    @Override
    public void resetStats() {

    }


    /**
     * Adds a new element to the buffer
     *
     * @param seqno         The seqno of the element
     * @param element       The element
     * @param remove_filter
     * @param opts          The options carried with this methods, e.g. whether to block when not enough space is available.
     * @return True if the element was added, false otherwise.
     */
    @Override
    public boolean add(long seqno, T element, Predicate<T> remove_filter, Options opts) {
        boolean block=opts != null && opts.block();
        lock.lock();
        try {
            long dist=seqno - low;
            if(dist <= 0)
                return false;

            if(dist > capacity() && (!block || !block(seqno)))  // seqno too big
                return false;

            int index=index(seqno);
            if(buf[index] != null)
                return false;
            buf[index]=element;

            // see if high needs to moved forward
            if(seqno - high > 0)
                high=seqno;

            if(remove_filter != null && seqno - low > 0) {
                Visitor<T> v=(seq,msg) -> {
                    if(msg == null || !remove_filter.test(msg))
                        return false;
                    if(seq - low > 0)
                        low=seq;
                    return true;
                };
                forEach(getHighestDelivered()+1, getHighestReceived(), v, true, true);
            }

            return true;
        }
        finally {
            lock.unlock();
        }
    }


    @Override
    public boolean add(MessageBatch batch, Function<T,Long> seqno_getter, boolean remove_from_batch, T const_value) {
        if(batch == null || batch.isEmpty())
            return false;
        Objects.requireNonNull(seqno_getter);
        boolean retval=false;
        lock.lock();
        try {
            for(Iterator<?> it=batch.iterator(); it.hasNext(); ) {
                T msg=(T)it.next();
                long seqno=seqno_getter.apply(msg);
                if(seqno < 0)
                    continue;
                T element=const_value != null? const_value : msg;
                boolean added=add(seqno, element, null, Options.DEFAULT());
                retval=retval || added;
                if(!added || remove_from_batch)
                    it.remove();
            }
            return retval;
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Removes the next non-null element and advances low
     *
     * @return T if there was a non-null element at low+1, otherwise null
     */
    public T remove() {
        lock.lock();
        try {
            long tmp=low + 1;
            if(tmp - high > 0)
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
        return remove();
    }

    @Override
    public List<T> removeMany(boolean nullify, int max_results, Predicate<T> filter) {
        return removeMany(true, max_results, filter, LinkedList::new, LinkedList::add);
    }

    @Override
    public <R> R removeMany(boolean nullify, int max_results, Predicate<T> filter, Supplier<R> result_creator, BiConsumer<R,T> accumulator) {
        lock.lock();
        try {
            Remover<R> remover=new Remover<>(max_results, filter, result_creator, accumulator);
            forEach(remover, true);
            return remover.getResult();
        }
        finally {
            lock.unlock();
        }
    }

    public List<T> removeMany(int max_results) {
        List<T> list=null;
        int num_results=0;

        lock.lock();
        try {
            long start=low;
            // while(start + 1 <= high) {
            while(high - start+1 >= 0) {
                int index=index(start + 1);
                T element=buf[index];
                if(element == null)
                    break;
                if(list == null)
                    list=new ArrayList<>(max_results > 0? max_results : 20);
                list.add(element);
                buf[index]=null; // need to null, otherwise a new message add fails if non-null (already inserted)
                start++;
                if(max_results > 0 && ++num_results >= max_results)
                    break;
            }
            if(start - low > 0) {
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
            // if(seqno <= low || seqno > high)
            if(seqno - low <= 0 || seqno - high > 0)
                return null;
            int index=index(seqno);
            return buf[index];
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Only used for testing !!
     */
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
     *
     * @param from
     * @param to
     * @return A list of messages, or null if none in range [from .. to] was found
     */
    public List<T> get(long from, long to) {
        if(from - to > 0)
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


    /**
     * Nulls elements between low and seqno and forwards low. Returns the number of nulled elements.
     */
    public int purge(long seqno) {
        int count=0;
        lock.lock();
        try {
            if(seqno - low <= 0)  // ignore if seqno <= low
                return 0;
            if(high - seqno <= 0) // if seqno is > high, set seqno=high
                seqno=high;

            long tmp=low;
            long from=low + 1;
            int distance=(int)(seqno - from +1);
            for(int i=0; i < distance; i++) {
                int index=index(from);
                if(buf[index] != null) {
                    buf[index]=null;
                    count++;
                }
                low++; from++;
            }
            if(low - tmp > 0)
                buffer_full.signalAll();
            return count;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public int purge(long seqno, boolean force) {
        return purge(seqno);
    }


    @Override
    public void forEach(long from, long to, Visitor<T> visitor, boolean nullify) {
        forEach(from, to, visitor, nullify, false);
    }

    public void forEach(long from, long to, Visitor<T> visitor, boolean nullify, boolean respect_stop) {
        if(from - to > 0) // same as if(from > to), but prevents long overflow
            return;
        int distance=(int)(to - from +1);
        long start=low;
        for(int i=0; i < distance; i++) {
            int index=index(from);
            T element=buf[index];
            boolean stop=visitor != null && !visitor.visit(from, element);
            if(stop && respect_stop)
                break;
            if(nullify && element != null) {
                buf[index]=null;
                if(from - low > 0)
                    low=from;
            }
            if(stop)
                break;
            from++;
        }
        if(nullify && low - start > 0)
            buffer_full.signalAll();
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

    @Override
    public SeqnoList getMissing(int max_msgs) {
        lock.lock();
        try {
            if(isEmpty())
                return null;
            long start_seqno=getHighestDeliverable() + 1;
            int capacity=(int)(high - start_seqno);
            int max_size=max_msgs > 0? Math.min(max_msgs, capacity) : capacity;
            if(max_size <= 0)
                return null;
            Missing missing=new Missing(start_seqno, max_size);
            long to=max_size > 0? Math.min(start_seqno + max_size - 1, high - 1) : high - 1;
            forEach(start_seqno, to, missing, false);
            return missing.getMissingElements();
        }
        finally {
            lock.unlock();
        }
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
     *
     * @return FixedBufferIterator
     * @throws NoSuchElementException is HD is moved forward during the iteration
     */
    public Iterator<T> iterator() {
        return new FixedBufferIterator(buf);
    }

    @Override
    public Iterator<T> iterator(long from, long to) {
        return new FixedBufferIterator(buf);
    }

    @Override
    public Stream<T> stream() {
        Spliterator<T> sp=Spliterators.spliterator(iterator(), size(), 0);
        return StreamSupport.stream(sp, false);
    }

    @Override
    public Stream<T> stream(long from, long to) {
        Spliterator<T> sp=Spliterators.spliterator(iterator(), size(), 0);
        return StreamSupport.stream(sp, false);
    }

    @Override
    public String toString() {
        return String.format("[%,d | %,d] (%,d elements, %,d missing)", low, high, size(), numMissing());
    }

    protected int index(long seqno) {
        // apparently this is faster than mod for n^2 capacity
        return (int)((seqno - offset - 1) & (capacity() - 1));
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

    // todo: will be removed once we have field 'size'
    protected int count(boolean missing) {
        if(high - low <= 0)
            return 0;
        int retval=0;
        long from=low+1, to=high;
        int distance=(int)(to - from +1);
        for(int i=0; i < distance; i++,from++) {
            int index=index(from);
            T element=buf[index];
            if(missing && element == null)
                retval++;
            if(!missing && element != null)
                retval++;
        }
        return retval;
    }

    protected class Remover<R> implements Visitor<T> {
        protected final int max_results;
        protected int num_results;
        protected final Predicate<T> filter;
        protected R result;
        protected Supplier<R> result_creator;
        protected BiConsumer<R,T> result_accumulator;

        public Remover(int max_results, Predicate<T> filter, Supplier<R> creator, BiConsumer<R,T> accumulator) {
            this.max_results=max_results;
            this.filter=filter;
            this.result_creator=creator;
            this.result_accumulator=accumulator;
        }

        public R getResult() {
            return result;
        }

        @GuardedBy("lock")
        public boolean visit(long seqno, T element) {
            if(element == null)
                return false;
            if(filter == null || filter.test(element)) {
                if(result == null)
                    result=result_creator.get();
                result_accumulator.accept(result, element);
                num_results++;
            }
            if(seqno - low > 0)
                low=seqno;
            return max_results == 0 || num_results < max_results;
        }
    }

    protected class FixedBufferIterator implements Iterator<T> {
        protected final T[] buffer;
        protected long      current=low+1;

        public FixedBufferIterator(T[] buffer) {
            this.buffer=buffer;
        }

        public boolean hasNext() {
            //    return current <= high;
            return high - current >= 0;
        }

        public T next() {
            if(!hasNext())
                throw new NoSuchElementException();
            // if(current <= low)
            if(low - current >= 0)
                current=low+1;
            return buffer[index(current++)];
        }

        public void remove() {}
    }

}
