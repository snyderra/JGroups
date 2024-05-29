package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.*;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

/**
 * Functional tests of {@link Buffer} implementations ({@link DynamicBuffer} and {@link FixedBuffer}).
 * @author Bela Ban
 * @since 3.1, 5.4
 */
// TODO: move to BufferTest
@Test(groups={Global.FUNCTIONAL,Global.EAP_EXCLUDED},description="Functional tests of Buffer implementations")
public class FixedXmitWindowTest {

    public void testConstructor() {
        FixedBuffer<Integer> buf=new FixedBuffer<>(100, 1);
        System.out.println("buf = " + buf);
        assert buf.capacity() == Util.getNextHigherPowerOfTwo(100);
        assert buf.size() == 0;
    }

    public void testIndex() {
        FixedBuffer<Integer> buf=new FixedBuffer<>(10, 5);
        assertIndices(buf, 5, 5);
        buf.add(6,6); buf.add(7,7);
        buf.remove(); buf.remove();
        long low=buf.low();
        int purged=buf.purge(4);
        assert purged == 0;
        purged=buf.purge(5);
        assert purged == 0;
        purged=buf.purge(6);
        assert purged == 0;
        purged=buf.purge(7);
        assert purged == 0;
        System.out.println("buf = " + buf);
        for(long i=low; i <= 7; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";
    }

    public void testIndexWithRemoveMany() {
        FixedBuffer<Integer> buf=new FixedBuffer<>(10, 5);
        assertIndices(buf, 5, 5);
        buf.add(6,6); buf.add(7,7);
        long low=buf.low();
        buf.removeMany(0);
        System.out.println("buf = " + buf);
        for(long i=low; i <= 7; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";
        assertIndices(buf, 7, 7);
    }

    public void testAddWithInvalidSeqno() {
        FixedBuffer<Integer> buf=new FixedBuffer<>(100, 20);
        assert buf.add(10, 0) == false;
        assert buf.add(20, 0) == false;
        assert buf.size() == 0;
    }

    public void testAdd() {
        FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        buf.add(1, 322649);
        buf.add(2, 100000);
        System.out.println("buf = " + buf);
        assert buf.size() == 2;
    }

    public void testGetHighestDeliverable3() {
        Buffer<Integer> table=new FixedBuffer<>(16, 0);
        for(int i=1; i <= 10; i++)
            table.add(i,i);
        System.out.println("table = " + table);
        table.removeMany(true, 9);
        long highest_deliverable=table.getHighestDeliverable();
        assert highest_deliverable == 10;
    }

    public void testSaturation() {
        FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        for(int i: Arrays.asList(1,2,3,4,5,6,7,8))
            buf.add(i, i);
        System.out.println("buf = " + buf);
        int size=buf.size(), space_used=buf.spaceUsed();
        double saturation=buf.saturation();
        System.out.println("size=" + size + ", space used=" + space_used + ", saturation=" + saturation);
        assert size == 8;
        assert space_used == 8;
        assert saturation == 0.5;

        buf.remove(); buf.remove(); buf.remove();
        size=buf.size();
        space_used=buf.spaceUsed();
        saturation=buf.saturation();
        System.out.println("size=" + size + ", space used=" + space_used + ", saturation=" + saturation);
        assert size == 5;
        assert space_used == 5;
        assert saturation == (double)size / buf.capacity();

        long low=buf.low();
        buf.purge(3);
        for(long i=low; i <= 3; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";
    }

    public void testAddWithWrapAround() {
        FixedBuffer<Integer> buf=new FixedBuffer<>(10, 5);
        for(int i=6; i <=15; i++)
            assert buf.add(i, i) : "addition of seqno " + i + " failed";
        System.out.println("buf = " + buf);
        for(int i=0; i < 3; i++) {
            Integer val=buf.remove();
            System.out.println("removed " + val);
            assert val != null;
        }
        System.out.println("buf = " + buf);

        long low=buf.low();
        buf.purge(8);
        System.out.println("buf = " + buf);
        assert buf.low() == 8;
        for(long i=low; i <= 8; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";

        for(int i=16; i <= 18; i++)
            assert buf.add(i, i);
        System.out.println("buf = " + buf);

        while(buf.remove() != null)
            ;
        System.out.println("buf = " + buf);
        assert buf.size() == 0;
        assert buf.numMissing() == 0;
        low=buf.low();
        buf.purge(18);
        assert buf.low() == 18;
        for(long i=low; i <= 18; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";
    }

    public void testAddWithWrapAroundAndRemoveMany() {
        FixedBuffer<Integer> buf=new FixedBuffer<>(10, 5);
        for(int i=6; i <=15; i++)
            assert buf.add(i, i) : "addition of seqno " + i + " failed";
        System.out.println("buf = " + buf);
        List<Integer> removed=buf.removeMany(3);
        System.out.println("removed " + removed);
        System.out.println("buf = " + buf);
        for(int i: removed)
            assert buf._get(i) == null;
        assertIndices(buf, 8, 15);

        for(int i=16; i <= 18; i++)
            assert buf.add(i, i);
        System.out.println("buf = " + buf);

        removed=buf.removeMany(0);
        System.out.println("buf = " + buf);
        System.out.println("removed = " + removed);
        assert removed.size() == 10;
        for(int i: removed)
            assert buf._get(i) == null;

        assert buf.size() == 0;
        assert buf.numMissing() == 0;
        assertIndices(buf, 18, 18);
    }

    public void testAddBeyondCapacity() {
        FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        for(int i=1; i <=10; i++)
            assert buf.add(i, i);
        System.out.println("buf = " + buf);
    }

    public void testAddMissing() {
        FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        for(int i: Arrays.asList(1,2,4,5,6))
            buf.add(i, i);
        System.out.println("buf = " + buf);
        assert buf.size() == 5 && buf.numMissing() == 1;

        Integer num=buf.remove();
        assert num == 1;
        num=buf.remove();
        assert num == 2;
        num=buf.remove();
        assert num == null;

        buf.add(3, 3);
        System.out.println("buf = " + buf);
        assert buf.size() == 4 && buf.numMissing() == 0;

        for(int i=3; i <= 6; i++) {
            num=buf.remove();
            System.out.println("buf = " + buf);
            assert num == i;
        }

        num=buf.remove();
        assert num == null;
    }


    public void testGetMissing() {
        FixedBuffer<Integer> buf=new FixedBuffer<>(30, 0);
        for(int i: Arrays.asList(2,5,10,11,12,13,15,20,28,30))
            buf.add(i, i);
        System.out.println("buf = " + buf);
        int missing=buf.numMissing();
        System.out.println("missing=" + missing);
        SeqnoList missing_list=buf.getMissing();
        System.out.println("missing_list = " + missing_list);
        assert missing_list.size() == missing;
    }

    public void testGetMissing2() {
        FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        buf.add(1,1);
        SeqnoList missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert missing == null && buf.numMissing() == 0;

        buf=new FixedBuffer<>(10, 0);
        buf.add(10,10);
        missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert buf.numMissing() == missing.size();

        buf=new FixedBuffer<>(10, 0);
        buf.add(5,5);
        missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert buf.numMissing() == missing.size();

        buf=new FixedBuffer<>(10, 0);
        buf.add(5,7);
        missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert buf.numMissing() == missing.size();
    }


    public void testBlockingAddAndClose() {
        final FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        for(int i=0; i <= 10; i++)
            buf.add(i, i, true);
        System.out.println("buf = " + buf);
        new Thread(() -> {
            Util.sleep(1000);
            buf.close();
        }).start();
        int seqno=buf.capacity() +1;
        boolean success=buf.add(seqno, seqno, true);
        System.out.println("buf=" + buf);
        assert !success;
        assert buf.size() == 10;
        assert buf.numMissing() == 0;
    }

    public void testBlockingAddAndPurge() throws InterruptedException {
        final FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        for(int i=0; i <= 10; i++)
            buf.add(i, i, true);
        System.out.println("buf = " + buf);
        Thread thread=new Thread(() -> {
            Util.sleep(1000);
            for(int i=0; i < 3; i++)
                buf.remove();
            buf.purge(3);
        });
        thread.start();
        boolean success=buf.add(11, 11, true);
        System.out.println("buf=" + buf);
        assert success;
        thread.join(10000);
        assert buf.size() == 8;
        assert buf.numMissing() == 0;
    }

    public void testBlockingAddAndPurge2() throws TimeoutException {
        final FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        for(int i=1; i <= buf.capacity(); i++)
            buf.add(i, i, true);
        System.out.println("buf = " + buf);
        BlockingAdder adder=new BlockingAdder(buf, 17, 20);
        adder.start();
        assert buf.size() == 16;
        boolean success=Util.waitUntilTrue(1000, 100, () -> adder.added() > 0);
        assert !success;
        Integer el=buf.remove();
        assert el == 1;
        Util.waitUntil(1000, 100, () -> adder.added() == 1);
        buf.purge(5);
        Util.waitUntil(1000, 100, () -> adder.added() == 4);
        assert buf.size() == 15;
    }

    public void testGet() {
        final FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        for(int i: Arrays.asList(1,2,3,4,5))
            buf.add(i, i);
        assert buf.get(0) == null;
        assert buf.get(1) == 1;
        assert buf.get(10) == null;
        assert buf.get(5) == 5;
        assert buf.get(6) == null;
    }

    public void testGetList() {
        final FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        for(int i: Arrays.asList(1,2,3,4,5))
            buf.add(i, i);
        List<Integer> elements=buf.get(3,5);
        System.out.println("elements = " + elements);
        assert elements != null && elements.size() == 3;
        assert elements.contains(3) && elements.contains(4) && elements.contains(5);

        elements=buf.get(4, 10);
        System.out.println("elements = " + elements);
        assert elements != null && elements.size() == 2;
        assert elements.contains(4) && elements.contains(5);

        elements=buf.get(10, 20);
        assert elements == null;
    }

    public void testRemove() {
        final FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        IntStream.rangeClosed(1,5).forEach(n -> buf.add(n,n));
        System.out.println("buf = " + buf);
        assertIndices(buf, 0, 5);

        for(int i=1; i <= 5; i++) {
            Integer el=buf.remove();
            System.out.println("el = " + el);
            assert el.equals(i);
        }
        assert buf.size() == 0;
    }

    public void testRemove2() {
        final FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        Integer el=buf.remove();
        assert el == null; // low == high
        assert buf.low() == 0;
        buf.add(2,2);
        el=buf.remove();
        assert el == null; // no element at 'low'
        assert buf.low() == 0;
        buf.add(1,1);
        el=buf.remove();
        assert el == 1;
        assert buf.low() == 1;
        el=buf.remove();
        assert el == 2;
        assert buf.low() == 2;
        el=buf.remove();
        assert el == null;
        assert buf.low() == 2;
    }

    public void testRemove3() {
        FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        for(int i=1; i <= 20; i++) {
            assert buf.add(i,i);
            Integer num=buf.remove();
            assert num != null && num == i;
        }
        System.out.println("buf = " + buf);
        assert buf.size() == 0;
        assert buf.numMissing() == 0;
        assert buf.high() == 20;
        assert buf.low() == 20;
    }

    public void testRemoveMany() {
        FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        IntStream.rangeClosed(1,10).forEach(n -> buf.add(n,n));
        List<Integer> list=buf.removeMany(3);
        System.out.println("list = " + list);
        assert list != null && list.size() == 3;

        list=buf.removeMany(0);
        System.out.println("list = " + list);
        assert list != null && list.size() == 7;

        list=buf.removeMany(10);
        assert list == null;

        buf.add(8, 8);
        list=buf.removeMany(0);
        System.out.println("list = " + list);
        assert list == null;
    }

    public void testRemoveManyWithMissingElements() {
        FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        for(int i: Arrays.asList(1,2,3,4,5,6,7,9,10))
            buf.add(i, i);
        List<Integer> list=buf.removeMany(3);
        System.out.println("list = " + list);
        assert list != null && list.size() == 3;
        for(int i: list)
            assert buf._get(i) == null;

        list=buf.removeMany(0);
        System.out.println("list = " + list);
        assert list != null && list.size() == 4;
        for(int i: list)
            assert buf._get(i) == null;

        list=buf.removeMany(10);
        assert list == null;

        buf.add(8, 8);
        list=buf.removeMany(0);
        System.out.println("list = " + list);
        assert list != null && list.size() == 3;
        for(int i: list)
            assert buf._get(i) == null;
    }

    /**
     * Runs NUM adder threads, each adder adds 1 (unique) seqno. When all adders are done, we should have
     * NUM elements in the RingBuffer.
     */
    public void testConcurrentAdd() {
        final int NUM=100;
        final FixedBuffer<Integer> buf=new FixedBuffer<>(NUM, 0);

        CountDownLatch latch=new CountDownLatch(1);
        Adder[] adders=new Adder[NUM];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Adder(latch, i+1, buf);
            adders[i].start();
        }

        System.out.println("releasing threads");
        latch.countDown();
        System.out.print("waiting for threads to be done: ");
        for(Adder adder: adders) {
            try {
                adder.join();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
        for(Adder adder: adders)
            assert adder.success();
        System.out.println("OK");
        System.out.println("buf = " + buf);
        assert buf.size() == NUM;
    }

    /**
     * Creates a RingBuffer and fills it to capacity. Then starts a number of adder threads, each trying to add a
     * seqno, blocking until there is more space. Each adder will block until the remover removes elements, so the
     * adder threads get unblocked and can then add their elements to the buffer.
     */
    public void testConcurrentAddAndRemove() throws InterruptedException {
        final int NUM=5;
        final FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        for(int i=1; i <= 10; i++)
            buf.add(i, i); // fill the buffer, add() will block now

        CountDownLatch latch=new CountDownLatch(1);
        Adder[] adders=new Adder[NUM];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Adder(latch, i+11, buf);
            adders[i].start();
        }

        System.out.println("starting threads");
        latch.countDown();
        System.out.print("waiting for threads to be done: ");

        Thread remover=new Thread("Remover") {
            public void run() {
                Util.sleep(2000);
                List<Integer> list=buf.removeMany(5);
                System.out.println("\nremover: removed = " + list);
            }
        };
        remover.start();

        for(Adder adder: adders) {
            try {
                adder.join();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }

        remover.join(10000);

        System.out.println("OK");
        System.out.println("buf = " + buf);
        assert buf.size() == 10;
        assertIndices(buf, 5,15);

        List<Integer> list=buf.removeMany(0);
        System.out.println("removed = " + list);
        assert list.size() == 10;
        for(int i=6; i <=15; i++)
            assert list.contains(i);
        assertIndices(buf, 15, 15);
    }

    public void testPurge() {
        FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        int purged=buf.purge(0);
        assert purged == 0;
        buf.purge(1);
        assert purged == 0;
        buf.purge(5);
        assert purged == 0;
        buf.purge(32);
        assert purged == 0;
        IntStream.rangeClosed(1,10).forEach(n -> buf.add(n,n));
        assert buf.size() == 10;
        purged=buf.purge(5);
        assert purged == 5 && buf.size() == 5;
    }

    public void testPurge2() {
        FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        for(int i=1; i <=7; i++) {
            boolean rc=buf.add(i, i);
            assert rc;
            Integer el=buf.remove();
            assert el == i;
        }
        System.out.println("buf = " + buf);
        assert buf.size() == 0;
        assertIndices(buf, 7, 7);
        buf.purge(3);
        assertIndices(buf, 7, 7);
        for(long i=0; i <= 3; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";

        buf.purge(6);
        assert buf.get(6) == null;
        buf.purge(7);
        assert buf.get(7) == null;
        assert buf.low() == 7;
        assert buf.size()  == 0;

        for(int i=7; i <= 14; i++) {
            buf.add(i, i);
            buf.remove();
        }

        System.out.println("buf = " + buf);
        assert buf.size() == 0;

        long low=buf.low();
        buf.purge(12);
        System.out.println("buf = " + buf);
        assert buf.low() == 14;
        for(long i=low; i <= 14; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";
    }


    public void testIterator() {
        FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        IntStream.rangeClosed(1,10).forEach(n -> buf.add(n,n));
        int count=0;
        for(Integer num: buf) {
            if(num != null) {
                count++;
                System.out.print(num + " ");
            }
        }
        System.out.println();
        assert count == 10 : "count=" + count;
        boolean rc=buf.add(8, 8);
        assert rc == false;
        count=0;
        for(Integer num: buf) {
            if(num != null) {
                System.out.print(num + " ");
                count++;
            }
        }
        assert count == 10 : "count=" + count;
    }



    protected static <T> void assertIndices(Buffer<T> buf, long low, long high) {
        assert buf.low() == low : String.format("expected low=%,d but was %,d", low, buf.low());
        assert buf.high()  == high : String.format("expected hr=%,d but was %,d", high, buf.high());
    }

    protected static class Adder extends Thread {
        protected final CountDownLatch           latch;
        protected final int                      seqno;
        protected final FixedBuffer<Integer> buf;
        protected boolean                        success;

        public Adder(CountDownLatch latch, int seqno, FixedBuffer<Integer> buf) {
            this.latch=latch;
            this.seqno=seqno;
            this.buf=buf;
        }

        public boolean success() {return success;}

        public void run() {
            try {
                latch.await();
                Util.sleepRandom(10, 500);
                success=buf.add(seqno, seqno, true);
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    protected static class BlockingAdder extends Thread {
        protected final FixedBuffer<Integer> buf;
        protected final int                      from, to;
        protected int                            added=0;

        protected BlockingAdder(FixedBuffer<Integer> buf, int from, int i) {
            this.buf=buf;
            this.from=from;
            to=i;
        }

        public int added() {return added;}

        @Override
        public void run() {
            for(int i=from; i <= to; i++) {
                boolean rc=buf.add(i, i, true);
                if(rc) {
                    added++;
                    System.out.printf("-- added %d\n", i);
                }
            }
            System.out.printf("-- done, added %d elements\n", added);
        }
    }


}
