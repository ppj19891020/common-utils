package com.fly.xid.sequence;

import com.fly.utils.Utils;
import com.fly.utils.ZKUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
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
    // 机器ID偏左移10位
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
    private volatile Long workerId;

    private AtomicBoolean active = new AtomicBoolean(false);
    //分隔符
    public static final String FILE_SEPERATEOR = File.separator;
    // 根节点 名称
    private final static String ROOT_PATH= FILE_SEPERATEOR + "snowflake";
    // 分布式锁
    private final static String LOCK_PATH = ROOT_PATH + FILE_SEPERATEOR + "lock";
    // 服务器临时节点-存放workid
    private final static String EPHEMEERAL_PATH = FILE_SEPERATEOR + "server-ephemeral";
    // 服务器临时节点-存放心跳时间
    private final static String EPHEMEERAL_HEART = FILE_SEPERATEOR + "server-heart";
    // 切换路径
    private final static String SWITCH_PATH = FILE_SEPERATEOR + "switch";
    // curator
    private static CuratorFramework client = null;
    // zk 节点-服务器节点
    private String zkSequence = null;
    // zk 心跳服务节点
    private String serverHeartNodePath = null;
    // 服务器心跳时间间隔 10s
    private final int interval = 10000;
    // 心跳时间监测时间间隔阈值 40s
    private final static Integer TIMESTAMP_THRESHOLD = 40000;

    /**
     * 保留workerId和lastTime, 以及备用workerId和其对应的lastTime
     */
    private static Map<Long, Long> workerIdLastTimeMap = new ConcurrentHashMap<>();

    static {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2018, Calendar.JULY, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        twepoch = calendar.getTimeInMillis();
        //初始化zk
        client = ZKUtils.getConnection();

    }

    public IdWorker(){
        // 校验服务器时间是否正常
        boolean check = this.checkServerTimesamp();
        if(!check){
            stop();
            Utils.halt_process(-1,"server timestamp greater than threshold,server startup fail, exit from the jvm");
        }
        // 校验workid是否已存在
        try{
            InterProcessLock lock = new InterProcessSemaphoreMutex(client, LOCK_PATH);
            try{
                lock.acquire();
                List<Long> existWorkerIds = this.getExistWorkerIds();
                if(null != existWorkerIds && existWorkerIds.size() > 0){
                    Collections.sort(existWorkerIds);
                    Integer maxWorkId = existWorkerIds.get(existWorkerIds.size()-1).intValue() + 1;
                    this.workerId = Long.parseLong(maxWorkId.toString());
                    LOGGER.info("设置workid-最新值:{}",this.workerId);
                }else {
                    this.workerId = 1L;
                    LOGGER.info("设置workid-默认值:{}",this.workerId);
                }
            }finally {
                lock.release();
            }
            // 初始化到zk中，保存到临时节点中
            zkSequence = client.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(ROOT_PATH + EPHEMEERAL_PATH +
                    FILE_SEPERATEOR + Utils.getIntranetIp() + "-",workerId.toString().getBytes());

            // 服务器心跳节点
            String[] zkSequenceSplit = zkSequence.split("/");
            serverHeartNodePath = ROOT_PATH + EPHEMEERAL_HEART +
                    FILE_SEPERATEOR + zkSequenceSplit[zkSequenceSplit.length-1];
            client.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(serverHeartNodePath,Long.valueOf(timeGen()).toString().getBytes());
            active.set(true);
            startHeartBeatThread();
            LOGGER.info("ip:{} workid:{} 序列号:{}",Utils.getIntranetIp(),workerId,zkSequence);
        }catch (Exception ex){
            LOGGER.error("初始化全局id失败",ex);
            stop();
            Utils.halt_process(-1,"Failed to make serverNodePath , exit from the jvm");
        }
        LOGGER.info("snowflow初始化成功，workid:{}",workerId);
    }

    /**
     * 校验服务器时间是否异常
     * @return
     */
    private boolean checkServerTimesamp(){
        try{
            if(null != client){
                Stat stat = client.checkExists().forPath(ROOT_PATH + EPHEMEERAL_HEART);
                if(null == stat){
                    // 表示还没服务器注册过
                    LOGGER.info("服务器还没注册心跳，当前服务器时间正常");
                    return true;
                }
                List<String> peershostPaths = client.getChildren().forPath(ROOT_PATH + EPHEMEERAL_HEART);
                if(null == peershostPaths || peershostPaths.size() == 0){
                    // 表示还没服务器注册过
                    LOGGER.info("服务器还没注册心跳，当前服务器时间正常");
                    return true;
                }
                // 总共时间
                BigDecimal total = BigDecimal.ZERO;
                for(String path:peershostPaths){
                    Long serverTimestanp = Long.valueOf(new String(client.getData().forPath(ROOT_PATH + EPHEMEERAL_HEART + FILE_SEPERATEOR + path)));
                    total = total.add(new BigDecimal(serverTimestanp));
                }
                // 均值
                BigDecimal average = total.divide(new BigDecimal(peershostPaths.size()),2, BigDecimal.ROUND_HALF_EVEN);
                // 时间差大于阈值，则启动失败
                if (Math.abs(average.longValue() - System.currentTimeMillis()) > TIMESTAMP_THRESHOLD){
                    LOGGER.info("校验服务器时间存在异常，超过阈值{}",TIMESTAMP_THRESHOLD);
                    return false;
                }
                LOGGER.info("校验时间通过，当前服务器时间正常");
                return true;
            }
            return false;
        }catch (Exception ex){
            LOGGER.error("当前时间戳校验失败",ex);
            return false;
        }
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
            LOGGER.info("更换workid-原先workid：{} 当前时间:{} 最后时间：{}",workerId,currentMillis,lastTimestamp);
            List<Long> existWorkIds = this.getExistWorkerIds();
            if(null == existWorkIds || existWorkIds.size() == 0){
                return null;
            }
            Collections.sort(existWorkIds);
            // 最大workid
            int maxWorkId = existWorkIds.get(existWorkIds.size()-1).intValue();

            for(int i=(maxWorkId+1)%(int)maxWorkerId;i<(int)maxWorkerId;i++){
                if(existWorkIds.contains(i)){
                    continue;
                }
                Long timestamp = workerIdLastTimeMap.get(i);
                lastTimestamp = timestamp==null?0L:timestamp;
                LOGGER.info("开始轮训查找可用workid。最新workid:{}",i);
                if(lastTimestamp <= currentMillis){
                    //切换新的workid
                    String switchStr = currentMillis+"--:"+workerId+"->"+i;
                    this.workerId = Long.valueOf(i);
                    //初始化workid冲突自动切换/运行中时间回拨自动切换
                    if(StringUtils.isNotEmpty(zkSequence)){
                        //更新zk的值
                        Stat stat =client.checkExists().forPath(zkSequence);
                        if(stat != null){
                            client.setData().inBackground().forPath(zkSequence,this.workerId.toString().getBytes());
                        }
                        LOGGER.info("时钟发生回拨-切换后的workid：{} 当前时间:{} 最后时间：{}" ,workerId,currentMillis,lastTimestamp);
                    }else{
                        LOGGER.info("初始化workid冲突切换-切换后的workid：{} 当前时间:{} 最后时间：{}" ,workerId,currentMillis,lastTimestamp);
                    }
                    //增加一条切换记录
                    client.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(
                            ROOT_PATH + SWITCH_PATH + FILE_SEPERATEOR + Utils.getIntranetIp() + "-",switchStr.getBytes());
                    active.set(true);
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

    /**
     * 服务节点心跳监测
     */
    private void startHeartBeatThread() {
        Thread heartBeat = new Thread(new Runnable() {
            @Override
            public void run() {
                while(active.get() == true) {
                    if( serverHeartNodePath != null ) {
                        if (client != null) {
                            try {
                                Long time = timeGen();
                                client.setData().forPath(serverHeartNodePath,time.toString().getBytes());
                                LOGGER.info("serverHeartNodePath:{} 更新心跳时间:{}",serverHeartNodePath,time);
                            } catch (Exception e) {
                                LOGGER.error("Faild to set heartBeat timestamp for path: " + serverHeartNodePath);
                            }
                        }
                    }
                    Utils.sleepMs(interval);
                }
            }
        });
        heartBeat.setName("heartbeat");
        heartBeat.setDaemon(true);
        heartBeat.start();
    }

}
