package com.example;

import java.util.ArrayList;
import java.util.List;
//求出两数之间的自除数
public class test01 {
    public static void main(String[] args) {

        List<Integer> integers = selfDividingNumbers(1, 22);
        System.out.println(integers.toString());

    }

    public static List<Integer> selfDividingNumbers(int left, int rigth) {
        ArrayList<Integer> list = new ArrayList<>();
        for (int j = left; j <= rigth; j++) {
            Boolean flag = isZiChuShu(j);
            if (flag){
                list.add(j);
            }
        }
        return list;
    }

    public static Boolean isZiChuShu(int j) {
        Boolean flag = true;
        int a =j;
        for (int i = String.valueOf(j).length(); i > 0; i--) {
            int k = j % 10;

            if (k == 0) {
                flag = false;
            } else if (a % k != 0) {
                flag = false;
            }
            j /= 10;
        }
        return flag;
    }

    /**
     * 其他解法
     * @param num
     * @return
     */
    public boolean isSelfDividing(int num) {
        int temp = num;
        while (temp > 0) {
            int digit = temp % 10;
            if (digit == 0 || num % digit != 0) {
                return false;
            }
            temp /= 10;
        }
        return true;
    }
}
