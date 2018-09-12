package in.nimbo.moama;

import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;

public class LinkListOptimizeAdderTest {

    @Test
    public void addAll() {
        LinkedList<Integer> list1=new LinkedList<>();
        LinkedList<Integer> list2=new LinkedList<>();
        for (int i = 0; i <100 ; i++) {
            list1.add(i);
        }
        for (int i = 100; i <222 ; i++) {
            list2.add(i);
        }
        LinkListOptimizeAdder.addInOneOrder(list1,list2);
        for (int i = 0; i < 222; i++) {
            Assert.assertEquals((int) list1.get(i), i);
        }
    }

}