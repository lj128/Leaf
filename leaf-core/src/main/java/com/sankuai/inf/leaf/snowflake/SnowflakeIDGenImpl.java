package com.sankuai.inf.leaf.snowflake;

import com.google.common.base.Preconditions;
import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.Status;
import com.sankuai.inf.leaf.common.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * 基于Twitter的Snowflake算法实现分布式高效有序ID
 *
 * <br>
 * SnowFlake的结构如下(每部分用-分开):<br>
 * <br>
 * 0 - 0000000000 0000000000 0000000000 0000000000 0 - 0000000000 - 000000000000 <br>
 * <br>
 * 1位标识，由于long基本类型在Java中是带符号的，最高位是符号位，正数是0，负数是1，所以id一般是正数，最高位是0<br>
 * <br>
 * 41位时间截(毫秒级)，注意，41位时间截不是存储当前时间的时间截，而是存储时间截的差值（当前时间截 - 开始时间截)
 * 这里的的开始时间截，一般是我们的id生成器开始使用的时间，由我们程序来指定的（如下START_TIME属性）。41位的时间截，可以使用69年，年T = (1L << 41) / (1000L * 60 * 60 * 24 * 365) = 69<br>
 * <br>
 * 10位的数据机器位，可以部署在1024个节点<br>
 * <br>
 * 12位序列，毫秒内的计数，12位的计数顺序号支持每个节点每毫秒(同一机器，同一时间截)产生4096个ID序号<br>
 * <br>
 * <br>
 * 加起来刚好64位，为一个Long型。<br>
 * SnowFlake的优点是，整体上按照时间自增排序，并且整个分布式系统内不会产生ID碰撞，并且效率较高，经测试，SnowFlake每秒能够产生26万ID左右。
 * <p>
 * <p>
 *
 */
public class SnowflakeIDGenImpl implements IDGen {

    static private final Logger LOGGER = LoggerFactory.getLogger(SnowflakeIDGenImpl.class);
    //起始时间戳
    private final long START_TIMESTAMP = 1288834974657L;
    //workId占用的位数
    private final long WORKER_ID_BITS = 10L;
    //workerid可以使用的最大值 1023
    private final long MAX_WORKER_ID = -1L ^ (-1L << WORKER_ID_BITS);
    //序列号占用的位数
    private final long SEQUENCE_BITS = 12L;
    //序列号可以使用的最大值 4095
    private final long MAX_SEQUENCE = -1L ^ (-1L << SEQUENCE_BITS);
    //workerid段向左的位移
    private final long WORKER_ID_LEFT_SHIFT = SEQUENCE_BITS;
    //时间戳段向左的位移
    private final long TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;
    private long workerId;
    private long sequence = 0L;
    private long lastTimestamp = -1L;
    public boolean initFlag = false;
    private static final Random RANDOM = new Random();
    private int port;

    @Override
    public boolean init() {
        return true;
    }

    public SnowflakeIDGenImpl(String zkAddress, int port) {
        this.port = port;
        SnowflakeZookeeperHolder holder = new SnowflakeZookeeperHolder(Utils.getIp(), String.valueOf(port), zkAddress);
        initFlag = holder.init();
        if (initFlag) {
            workerId = holder.getWorkerID();
            LOGGER.info("START SUCCESS USE ZK WORKERID-{}", workerId);
        } else {
            Preconditions.checkArgument(initFlag, "Snowflake Id Gen is not init ok");
        }
        Preconditions.checkArgument(workerId >= 0 && workerId <= MAX_WORKER_ID, "workerID must gte 0 and lte 1023");
    }

    @Override
    public synchronized Result get(String key) {
        long timestamp = timeGen();
        if (timestamp < lastTimestamp) {
            //发生时钟回拨
            long offset = lastTimestamp - timestamp;
            if (offset <= 5) {
                //小于5ms的时钟回拨则等待时钟追平
                try {
                    wait(offset << 1);
                    timestamp = timeGen();
                    if (timestamp < lastTimestamp) {
                        return new Result(-1, Status.EXCEPTION);
                    }
                } catch (InterruptedException e) {
                    //等待超时
                    LOGGER.error("wait interrupted");
                    return new Result(-2, Status.EXCEPTION);
                }
            } else {
                //大于5ms的时钟回拨则直接异常退出
                return new Result(-3, Status.EXCEPTION);
            }
        }
        /**
         * 同一毫秒内序号递增，不同毫秒序号随机
         */
        if (lastTimestamp == timestamp) {
            //防止序列号溢出
            sequence = (sequence + 1) & MAX_SEQUENCE;
            if (sequence == 0) {
                //seq 为0的时候表示是下一毫秒时间开始对seq做随机
                sequence = RANDOM.nextInt(100);
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            //如果是新的ms开始
            sequence = RANDOM.nextInt(100);
        }
        lastTimestamp = timestamp;
        long id = ((timestamp - START_TIMESTAMP) << TIMESTAMP_LEFT_SHIFT) | (workerId << WORKER_ID_LEFT_SHIFT) | sequence;
        return new Result(id, Status.SUCCESS);

    }

    protected long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    protected long timeGen() {
        return System.currentTimeMillis();
    }

    public long getWorkerId() {
        return workerId;
    }

}
