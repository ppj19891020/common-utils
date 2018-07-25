package com.fly.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

public class Utils {

    private final static Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    /**
     * 获得内网IP
     * @return 内网IP
     */
    public static String getIntranetIp(){
        try{
            return InetAddress.getLocalHost().getHostAddress();
        } catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    public static void haltProcess(int val) {
        //Runtime.getRuntime().halt(val);
        System.exit(val);
    }

    /**
     * jvm 退出
     * @param val
     * @param msg
     */
    public static void halt_process(int val, String msg) {
        LOGGER.info("Halting process: " + msg);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        haltProcess(val);
    }

    public static void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {

        }
    }

}
