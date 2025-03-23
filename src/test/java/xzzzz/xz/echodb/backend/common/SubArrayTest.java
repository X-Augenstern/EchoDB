package xzzzz.xz.echodb.backend.common;

import org.junit.Test;

import java.util.Arrays;

public class SubArrayTest {

    @Test
    public void testSubArray() {
        // 创建一个1-10的数组
        byte[] subArray = new byte[10];
        for (int i = 0; i < subArray.length; i++) {
            subArray[i] = (byte) (i + 1);
        }

        // 创建两个SubArray
        SubArray sub1 = new SubArray(subArray, 3, 8);
        SubArray sub2 = new SubArray(subArray, 6, 9);

        // 修改共享内存数组数据，两个 SubArray 的 raw 都指向同一个数组，所以 另一个 sub2 看到的内容也跟着变了
        // 改了 sub1.raw，整个数组也变了：因为 sub1.raw 和原数组 subArray 是同一个对象（同一个引用）
        // Java 的数组是引用类型：所有赋值/传参只是“传地址”，不是“复制一份”
        sub1.raw[7] = (byte) 44;

        // 打印原始数组
        System.out.println("Original Array: ");
        printArray(subArray);

        // 打印共享内存数组
        System.out.println("Subarray1: ");
        printSubArray(sub1);
        System.out.println("Subarray2: ");
        printSubArray(sub2);

        /*
        Original Array:
        [1, 2, 3, 4, 5, 6, 7, 44, 9, 10]
        Subarray1:
        4	5	6	7	44	9
        Subarray2:
        7	44	9	10
         */
    }

    private void printArray(byte[] array) {
        System.out.println(Arrays.toString(array));
    }

    private void printSubArray(SubArray sub) {
        for (int i = sub.start; i <= sub.end; i++) {
            System.out.print(sub.raw[i] + "\t");
        }
        System.out.println();
    }
}
