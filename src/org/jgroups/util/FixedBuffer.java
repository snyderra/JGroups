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
        this.low=this.hd=this.high=this.offset=offset;
    }

    @Override public int capacity() {return buf.length;}

    @Override
    public int computeSize() {
        if(high - low <= 0)
            return 0;
        int retval=0;
        long from=low+1, to=high;
        int distance=(int)(to - from +1);
        for(int i=0; i < distance; i++,from++) {
            int index=index(from);
            T element=buf[index];
            if(element != null)
                retval++;
        }
        return retval;
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
            size++;

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
                forEach(highestDelivered()+1, high(), v, true, true);
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
     * Removes the next non-null element and advances hd
     * @return T if there was a non-null element at hd+1, otherwise null
     */
    public T remove() {
        lock.lock();
        try {
            long tmp=hd + 1;
            if(tmp - high > 0)
                return null;
            int index=index(tmp);
            T element=buf[index];
            if(element != null) {
                hd=tmp;
                buf[index]=null;
                size=Math.max(size-1, 0); // cannot be < 0 (well that would be a bug, but let's have this 2nd line of defense !)
                if(hd - low > 0)
                    low=hd;
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

    /** Nulls elements between low and seqno and forwards low. Returns the number of nulled elements */
    @Override
    public int purge(long seqno, boolean force) {
        int count=0;
        lock.lock();
        try {
            if(seqno - low <= 0)  // ignore if seqno <= low
                return 0;

            if(force) {
                if(seqno - high > 0)
                    seqno=high;
            }
            else {
                if(seqno - hd > 0) // we cannot be higher than the highest removed seqno
                    seqno=hd;
            }
            long tmp=low;
            long from=low + 1;
            int distance=(int)(seqno - from +1);
            for(int i=0; i < distance; i++) {
                int index=index(from);
                if(buf[index] != null) {
                    buf[index]=null;
                    size=Math.max(size-1, 0);
                    count++;
                }
                low++; from++;
                hd=Math.max(hd, low);
            }
            /*if(seqno - low > 0)
                low=seqno;
            if(force) {
                if(seqno - hd > 0)
                    hd=seqno;
            }*/
            if(low - tmp > 0)
                buffer_full.signalAll();
            return count;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public void forEach(long from, long to, Visitor<T> visitor, boolean nullify) {
        forEach(from, to, visitor, nullify, false);
    }

    public void forEach(long from, long to, Visitor<T> visitor, boolean nullify, boolean respect_stop) {
        if(from - to > 0) // same as if(from > to), but prevents long overflow
            return;
        int distance=(int)(to - from +1);
        long start=hd;
        for(int i=0; i < distance; i++) {
            int index=index(from);
            T element=buf[index];
            boolean stop=visitor != null && !visitor.visit(from, element);
            if(stop && respect_stop)
                break;
            if(nullify && element != null) {
                buf[index]=null;
                size=Math.max(size-1, 0);
                if(from - hd > 0)
                    hd=from;
                if(from - low > 0)
                    low=from;
            }
            if(stop)
                break;
            from++;
        }
        if(nullify && hd - start > 0)
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



    protected class Remover<R> implements Visitor<T> {
        protected final int          max_results;
        protected int                num_results;
        protected final Predicate<T> filter;
        protected R                  result;
        protected Supplier<R>        result_creator;
        protected BiConsumer<R,T>    result_accumulator;

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
            if(seqno - hd > 0)
                hd=seqno;
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
