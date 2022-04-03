package com.example;

import java.util.HashMap;

//求最长不重复子字符串长度
public class test02 {
    public static void main(String[] args) {

//        System.out.println(stringOfLength("sssssaaa"));
        stringOfLength("sssssaaa");

    }

    private static int stringOfLength(String s) {
        if (s.length() == 0) return 0;
        HashMap<Character, Integer> map = new HashMap<Character, Integer>();
        int max = 0;
        int left = 0;
        for (int i = 0; i < s.length(); i++) {
            if (map.containsKey(s.charAt(i))) {
                left = Math.max(left, map.get(s.charAt(i)) + 1);
            }
            map.put(s.charAt(i), i);
            max = Math.max(max, i - left + 1);
        }
        return max;
    }
}
