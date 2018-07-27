package com.fly.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Enumeration;

public class Utils {

    private final static Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    /**
     * 获得内网IP
     * @return 内网IP
     */
    public static String getIntranetIp(){
        try {
            return getLocalHostLANAddress().getHostAddress();
        } catch (Exception e) {
            e.printStackTrace();
            Utils.halt_process(-1,"获取ip初始化失败，jvm退出");
        }
        return null;
    }

    /**
     * 获取ip
     * @return
     * @throws UnknownHostException
     */
    private static InetAddress getLocalHostLANAddress()throws Exception{
        try {
            InetAddress candidateAddress = null;
            // 遍历所有的网络接口
            for (Enumeration ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements();) {
                NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
                // 在所有的接口下再遍历IP
                for (Enumeration inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements();) {
                    InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();
                    if (!inetAddr.isLoopbackAddress()) {// 排除loopback类型地址
                        if (inetAddr.isSiteLocalAddress()) {
                            // 如果是site-local地址，就是它了
                            return inetAddr;
                        } else if (candidateAddress == null) {
                            // site-local类型的地址未被发现，先记录候选地址
                            candidateAddress = inetAddr;
                        }
                    }
                }
            }
            if (candidateAddress != null) {
                return candidateAddress;
            }
            // 如果没有发现 non-loopback地址.只能用最次选的方案
            InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
            if (jdkSuppliedAddress == null) {
                throw new UnknownHostException("The JDK InetAddress.getLocalHost() method unexpectedly returned null.");
            }
            return jdkSuppliedAddress;
        } catch (Exception e) {
            LOGGER.error("Failed to determine LAN address",e);
            throw e;
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
            e.printStackTrace();
        }
        haltProcess(val);
    }

    public static void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static ByteBuffer buffer = ByteBuffer.allocate(8);
    /**
     * byte 数组与 long 的相互转换
     * @param x
     * @return
     */
    public static byte[] longToBytes(long x) {
        buffer.putLong(0, x);
        return buffer.array();
    }

}
