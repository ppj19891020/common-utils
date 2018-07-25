package com.fly.xid.sequence;

import com.fly.utils.Utils;
import com.fly.utils.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 雪花算法id 生成器
 */
public class IdWorker {

    private final static Logger LOGGER = LoggerFactory.getLogger(IdWorker.class);
    // 时间错偏移量
    private final static long twepoch;
    // 机器标识位数
    private final static long workerIdBits = 10L;
    // 机器ID最大值 1024
    private final static long maxWorkerId = -1L ^ (-1L << workerIdBits);
    // 毫秒内自增位
    private final static long sequenceBits = 10L;
    // 机器ID偏左移12位
    private final static long workerIdShift = sequenceBits;
    // 时间毫秒左移22位
    private final static long timestampLeftShift = sequenceBits + workerIdBits;
    // 来对sequence做快速取模操作的
    private final static long sequenceMask = -1L ^ (-1L << sequenceBits);
    // 最后获取的时间错
    private static long lastTimestamp = -1L;
    // 时间回拨最大容受时间
    private  static final int MAX_BACKWARD_MS = 500;

    private long sequence = 0L;
    private Long workerId;

    private AtomicBoolean active = new AtomicBoolean(false);
    //分隔符
    public static final String FILE_SEPERATEOR = File.separator;
    // 根节点 名称
    private final static String ROOT_PATH= FILE_SEPERATEOR + "snowflake";
    // 分布式锁
    private final static String LOCK_PATH = ROOT_PATH + FILE_SEPERATEOR + "lock";
    // 服务器临时节点
    private final static String EPHEMEERAL_PATH = FILE_SEPERATEOR + "server-ephemeral";
    // 切换路径
    private final static String SWITCH_PATH = FILE_SEPERATEOR + "switch";
    // curator
    private static CuratorFramework client = null;
    // zk 节点
    private String zkSequence = null;

    /**
     * 保留workerId和lastTime, 以及备用workerId和其对应的lastTime
     */
    private static Map<Long, Long> workerIdLastTimeMap = new ConcurrentHashMap<>();

    static {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2018, Calendar.JULY, 23);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        twepoch = calendar.getTimeInMillis();
        //初始化zk
        client = ZKUtils.getConnection();
    }

    public IdWorker(Long workerId){
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException("worker Id can't be greater than %d or less than 0");
        }
        this.workerId = workerId;
        // 校验workid是否已存在
        InterProcessLock lock = new InterProcessSemaphoreMutex(client, LOCK_PATH);
        try{
            lock.acquire();
            List<Long> existWorkerIds = this.getExistWorkerIds();
            if(null != existWorkerIds && existWorkerIds.size() > 0){
                if(existWorkerIds.contains(workerId)){
                    LOGGER.error("Failed to make serverNodePath , exit from the jvm");
                    stop();
                    Utils.halt_process(-1,"Failed to make serverNodePath , exit from the jvm");
                }
            }
            // 初始化到zk中，保存到临时节点中
            zkSequence = client.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(ROOT_PATH + EPHEMEERAL_PATH +
                    FILE_SEPERATEOR + Utils.getIntranetIp() + "-",workerId.toString().getBytes());
            LOGGER.info("ip:{} workid:{} 序列号:{}",Utils.getIntranetIp(),workerId,zkSequence);
        }catch (Exception ex){
            LOGGER.error("初始化全局id失败",ex);
            stop();
            Utils.halt_process(-1,"Failed to make serverNodePath , exit from the jvm");
        }finally {
            try {
                lock.release();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        active.set(true);
        LOGGER.info("snowflow初始化成功，workid:{}",workerId);
    }

    public synchronized long nextId() {
        long timestamp = timeGen();
        // 闰秒、服务器时间问题：如果当前时间小于上一次ID生成的时间戳，说明系统时钟回退过这个时候应当抛出异常
        // 更换一个更大的workid和
        if (timestamp < lastTimestamp) {
            long offset = lastTimestamp - timestamp;
            // 如果时钟回拨在可接受范围内, 等待即可
            if(offset <= MAX_BACKWARD_MS){
                try {
                    this.wait(offset << 1);
                    LOGGER.info("offset：{} 小于 {} 延时{}",offset,MAX_BACKWARD_MS,offset << 1);
                    timestamp = this.timeGen();
                    if (timestamp < lastTimestamp) {
                        throw new RuntimeException("服务器始终回拨异常");
                    }
                }catch (Exception ex){
                    LOGGER.error("线程wait失败,开始尝试获取较大的workid",ex);
                    tryGenerateWorkIdByzk(timestamp);
                }
            }else{
                LOGGER.error("时间回拨间隔大于{}，开始重置workid",MAX_BACKWARD_MS);
                tryGenerateWorkIdByzk(timestamp);
            }
        }

        // 解决跨毫秒生成ID序列号始终为偶数的缺陷:如果是同一时间生成的，则进行毫秒内序列
        if (lastTimestamp == timestamp) {
            // 当前毫秒内，则+1
            // 通过位与运算保证计算的结果范围始终是 0-4095
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                // 当前毫秒内计数满了，则等待下一秒
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0;
        }
        lastTimestamp = timestamp;
        // 保存workid 对应的最后时间戳，用于时间回拨验证
        workerIdLastTimeMap.put(workerId,lastTimestamp);

        // ID偏移组合生成最终的ID，并返回ID
        long nextId = ((timestamp - twepoch) << timestampLeftShift)
                | (workerId << workerIdShift) | sequence;
        return nextId;
    }

    /**
     * 尝试获取生成大的workid
     * @param currentMillis 当前时间
     */
    private Long tryGenerateWorkIdByzk(long currentMillis){
        InterProcessLock lock = new InterProcessSemaphoreMutex(client, LOCK_PATH);
        try{
            active.set(false);
            lock.acquire();
            LOGGER.info("时钟发生回拨-原先workid：{} 当前时间:{} 最后时间：{}" ,currentMillis,lastTimestamp,workerId);
            List<Long> existWorkIds = this.getExistWorkerIds();
            if(null == existWorkIds || existWorkIds.size() == 0){
                return null;
            }
            Collections.sort(existWorkIds);
            // 最大workid
            int maxWorkId = existWorkIds.get(existWorkIds.size()-1).intValue();

            for(int i=(maxWorkId+1)%1024;i<1024;i++){
                Long timestamp = workerIdLastTimeMap.get(i);
                lastTimestamp = timestamp==null?0L:timestamp;
                LOGGER.info("开始轮训查找可用workid。最新workid:{}",i);
                if(lastTimestamp <= currentMillis){
                    //切换新的workid
                    String switchStr = currentMillis+"--:"+workerId+"->"+i;
                    LOGGER.info("时钟发生回拨-切换后的workid：{} 当前时间:{} 最后时间：{}" ,currentMillis,lastTimestamp,workerId);
                    this.workerId = Long.valueOf(i);
                    //更新zk的值
                    client.setData().inBackground().forPath(zkSequence,this.workerId.toString().getBytes());
                    //增加一条切换记录
                    client.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(
                            ROOT_PATH + SWITCH_PATH + FILE_SEPERATEOR + Utils.getIntranetIp(),switchStr.getBytes());
                    return lastTimestamp;
                }
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }finally {
            try {
                lock.release();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private long tilNextMillis(final long lastTimestamp) {
        long timestamp = this.timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = this.timeGen();
        }
        return timestamp;
    }

    private long timeGen() {
        return System.currentTimeMillis();
    }

    private void stop() {
        active.set(false);
        if (client != null) {
            client.close();
            client = null;
        }
        Utils.sleepMs(5000);
    }

    /**
     * 获取目前存在的workid
     * @return
     * @throws Exception
     */
    private List<Long> getExistWorkerIds() throws Exception{
        List<Long> existWorkerIds = new ArrayList<Long>();
        List<String> childNodes = ZKUtils.getChildByPath(client,ROOT_PATH + EPHEMEERAL_PATH);
        if(null != childNodes && childNodes.size() > 0){
            for(String childNode:childNodes){
                Long existWorkId = Long.valueOf(new String(client.getData().forPath(ROOT_PATH + EPHEMEERAL_PATH + FILE_SEPERATEOR + childNode)));
                existWorkerIds.add(existWorkId);
            }
        }
        return existWorkerIds;
    }

}
