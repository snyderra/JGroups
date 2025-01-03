package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.Average;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.ByteArray;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.stream.IntStream;

/**
 * @author Bela Ban
 * @since  3.6.8
 */
@Test(groups=Global.FUNCTIONAL)
public class AverageTest {

    public void testAverage() {
        long[] numbers=new long[1000];
        long total=0;

        for(int i=0; i < numbers.length; i++) {
            numbers[i]=Util.random(10000);
            total+=numbers[i];
        }

        double expected_avg=total/1000.0;

        Average a=new Average(1000);
        for(long num: numbers)
            a.add(num);

        double avg=a.average();

        expected_avg=Math.floor(expected_avg);
        avg=Math.floor(avg);

        assert avg == expected_avg;
    }

    public void testAverage2() {
        Average avg=new Average(16);
        double expected_avg=IntStream.rangeClosed(5, 20).sum() / 16.0;
        IntStream.rangeClosed(1,20).forEach(avg::add);
        double actual_avg=avg.average();
        assert actual_avg == expected_avg : String.format("actual: %.2f expected: %.2f\n", actual_avg, expected_avg);
    }


    public void testOverflow() {
        long start=Long.MAX_VALUE/ 500;
        Average avg=new Average();
        for(int i=0; i < 1000; i++)
            avg.add(start++);

        long cnt=avg.count();
        System.out.printf("cnt=%d, avg=%.2f\n", cnt, avg.getAverage());
        assert cnt == 1000; // was reset at i=500
    }

    public void testMinMax() {
        AverageMinMax avg=new AverageMinMax();
        IntStream.rangeClosed(1,10).forEach(avg::add);
        double average=IntStream.rangeClosed(1,10).average().orElse(0.0);
        assert avg.getAverage() == average;
        assert avg.min() == 1;
        assert avg.max() == 10;
    }

    public void testMerge() {
        AverageMinMax avg1=new AverageMinMax(11000), avg2=new AverageMinMax(10000);
        IntStream.rangeClosed(1, 1000).forEach(i -> avg1.add(1));
        IntStream.rangeClosed(1, 10000).forEach(i -> avg2.add(2));
        System.out.printf("avg1: %s, avg2: %s\n", avg1, avg2);
        avg1.merge(avg2);
        System.out.printf("merged avg1: %s\n", avg1);
        assert avg1.count() == 11_000;
        double diff=Math.abs(avg1.average() - 1.90);
        assert diff < 0.01;
        assert avg1.min() == 1;
        assert avg1.max() == 2;
    }

    public void testMerge2() {
        AverageMinMax avg1=new AverageMinMax(10000), avg2=new AverageMinMax(10000);
        IntStream.rangeClosed(1, 10000).forEach(i -> avg2.add(2));
        System.out.printf("avg1: %s, avg2: %s\n", avg1, avg2);
        avg1.merge(avg2);
        System.out.printf("merged avg1: %s\n", avg1);
        assert avg1.count() == 10000;
        assert avg1.average() == 2.0;
        assert avg1.min() == 2;
        assert avg1.max() == 2;
    }

    public void testMerge3() {
        AverageMinMax avg1=new AverageMinMax(100), avg2=new AverageMinMax(200);
        IntStream.rangeClosed(1, 100).forEach(i -> avg1.add(1));
        IntStream.rangeClosed(1, 200).forEach(i -> avg2.add(2));
        System.out.printf("avg1: %s, avg2: %s\n", avg1, avg2);
        avg1.merge(avg2);
        System.out.printf("merged avg1: %s\n", avg1);
        assert avg1.count() == 300;
        assert avg1.average() == 2.0;
        assert avg1.min() == 1;
        assert avg1.max() == 2;
    }

    public void testAverageWithNoElements() {
        Average avg=new AverageMinMax();
        double av=avg.average();
        assert av == 0.0;
    }

    public void testSerialization() throws IOException, ClassNotFoundException {
        Average avg=new Average(128);
        for(int i=0; i < 100; i++)
            avg.add(Util.random(128));
        ByteArray buf=Util.objectToBuffer(avg);
        Average avg2=Util.objectFromBuffer(buf, null);
        assert avg2 != null;
        assert avg.count() == avg2.count();
        assert avg.average() == avg2.average();
    }

}
