package in.nimbo.moama;

import java.lang.reflect.Field;
import java.util.AbstractList;
import java.util.LinkedList;

public class LinkListOptimizeAdder {

    /** this code equal to linkedList1.addInOneOrder(linkList2) but it will run in O(1)
     * WARNING the must important warn for you is with this method linkedList2 will not be usable
     * and don't expect regular behavior from linkList2
     * @param linkedList   it will merged with linkedList2 (destination)
     * @param linkedList2 you must delete it after calling this method(don't use it any more)
     */
    public static void addInOneOrder(LinkedList linkedList, LinkedList linkedList2) {
        synchronized (linkedList2) {
            synchronized (linkedList) {
                try {
                    int finalSize = linkedList.size() + linkedList2.size();
                    Class linkedListClass = LinkedList.class;
                    Field lastNode = linkedListClass.getDeclaredField("last");
                    Field firstNode = linkedListClass.getDeclaredField("first");
                    lastNode.setAccessible(true);
                    firstNode.setAccessible(true);
                    Class nodeClass = lastNode.get(linkedList).getClass();
                    Field nodeNext = nodeClass.getDeclaredField("next");
                    Field nodePrev = nodeClass.getDeclaredField("prev");
                    nodeNext.setAccessible(true);
                    nodePrev.setAccessible(true);
                    Object last1 = lastNode.get(linkedList);
                    Object last2 = lastNode.get(linkedList2);
                    Object first2 = firstNode.get(linkedList2);


                    nodeNext.set(last1, first2);
                    nodePrev.set(first2, last1);
                    lastNode.set(linkedList, last2);
                    //end

                    Field size = linkedListClass.getDeclaredField("size");
                    size.setAccessible(true);
                    size.set(linkedList, finalSize);

                    Field modCount = AbstractList.class.getDeclaredField("modCount");
                    modCount.setAccessible(true);

                    modCount.set(linkedList, ((int) modCount.get(linkedList)) + 1);
                } catch (IllegalAccessException | NoSuchFieldException ignored) {
                }
            }
        }
    }

}
