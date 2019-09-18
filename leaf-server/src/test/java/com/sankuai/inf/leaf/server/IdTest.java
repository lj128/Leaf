package com.sankuai.inf.leaf.server;

import org.junit.Test;

import java.util.Random;

/**
 * Created by liujia on  11:26
 */
public class IdTest {
    @Test
    public void test() {
        long num = -1L ^ (-1L << 5L);
        System.out.println(num);
    }
}
