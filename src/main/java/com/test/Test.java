package com.test;

import java.util.Arrays;
import java.util.List;

/**
 * Created by raghr010 on 12/21/16.
 */
public class Test {

    public static void main(String[] args) {

        List<String> a = Arrays.asList("a", "b", "c", "d");

        a.stream()
                .filter((b) -> b.equals("a") || b.equals("c"))
                .forEach(System.out::println);
    }

}
