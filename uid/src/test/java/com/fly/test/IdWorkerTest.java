package com.fly.test;


import com.fly.utils.Utils;
import com.fly.xid.sequence.IdWorker;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class IdWorkerTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(IdWorkerTest.class);

    @Test
    public void test() throws InterruptedException {
        IdWorker idWorker = new IdWorker(1L);
        Map<Long,Long> values = new HashMap<>();
//        IdWorker idWorker1 = new IdWorker(2L);
//        for (int i=0;i<10000;i++){
//            LOGGER.info("id:{}",idWorker.nextId());
//        }
        while(true){
//            Thread.sleep(500l);
            Long value = idWorker.nextId();
            if(values.containsKey(value)){
                LOGGER.error("value:{} 生成重复",value);
                Utils.halt_process(-1,"生成重复, exit from the jvm");
                return;
            }

            values.put(value,value);
//            LOGGER.info("id:{}",value);
        }
    }

}
