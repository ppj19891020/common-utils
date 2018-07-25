package com.fly.test;


import com.fly.xid.sequence.IdWorker;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdWorkerTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(IdWorkerTest.class);

    @Test
    public void test() throws InterruptedException {
        IdWorker idWorker = new IdWorker(1L);
//        IdWorker idWorker1 = new IdWorker(2L);
//        for (int i=0;i<10000;i++){
//            LOGGER.info("id:{}",idWorker.nextId());
//        }
        while(true){
            Thread.sleep(500l);
            LOGGER.info("id:{}",idWorker.nextId());
        }
    }

}
