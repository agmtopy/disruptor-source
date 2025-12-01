/*
 * Copyright 2011 LMAX Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lmax.disruptor;

import com.lmax.disruptor.util.Util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

abstract class SingleProducerSequencerPad extends AbstractSequencer
{
    protected byte
        p10, p11, p12, p13, p14, p15, p16, p17,
        p20, p21, p22, p23, p24, p25, p26, p27,
        p30, p31, p32, p33, p34, p35, p36, p37,
        p40, p41, p42, p43, p44, p45, p46, p47,
        p50, p51, p52, p53, p54, p55, p56, p57,
        p60, p61, p62, p63, p64, p65, p66, p67,
        p70, p71, p72, p73, p74, p75, p76, p77;

    SingleProducerSequencerPad(final int bufferSize, final WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
    }
}

abstract class SingleProducerSequencerFields extends SingleProducerSequencerPad
{
    SingleProducerSequencerFields(final int bufferSize, final WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
    }

    /**
     * Set to -1 as sequence starting point
     */
    //下一个序号
    long nextValue = Sequence.INITIAL_VALUE;
    //最小消费者进度的本地缓存
    long cachedValue = Sequence.INITIAL_VALUE;
}

/**
 * 单生产者的序列协调类
 * Coordinator for claiming sequences for access to a data structure while tracking dependent {@link Sequence}s.
 * Not safe for use from multiple threads as it does not implement any barriers.
 *
 * <p>* Note on {@link Sequencer#getCursor()}:  With this sequencer the cursor value is updated after the call
 * to {@link Sequencer#publish(long)} is made.
 */

public final class SingleProducerSequencer extends SingleProducerSequencerFields
{
    protected byte
        p10, p11, p12, p13, p14, p15, p16, p17,
        p20, p21, p22, p23, p24, p25, p26, p27,
        p30, p31, p32, p33, p34, p35, p36, p37,
        p40, p41, p42, p43, p44, p45, p46, p47,
        p50, p51, p52, p53, p54, p55, p56, p57,
        p60, p61, p62, p63, p64, p65, p66, p67,
        p70, p71, p72, p73, p74, p75, p76, p77;

    /**
     * Construct a Sequencer with the selected wait strategy and buffer size.
     *
     * @param bufferSize   the size of the buffer that this will sequence over.
     * @param waitStrategy for those waiting on sequences.
     */
    public SingleProducerSequencer(final int bufferSize, final WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
    }

    /**
     * 是否还有可用容量
     * @see Sequencer#hasAvailableCapacity(int)
     */
    @Override
    public boolean hasAvailableCapacity(final int requiredCapacity)
    {
        return hasAvailableCapacity(requiredCapacity, false);
    }

    /**
     * 是否还有可用容量
     * @param requiredCapacity
     * @param doStore
     * @return
     */
    /**
     * 检查是否有足够的容量来存储指定数量的元素
     *
     * @param requiredCapacity 所需的容量大小
     * @param doStore 是否需要存储当前序列值
     * @return 如果有足够容量返回true，否则返回false
     */
    private boolean hasAvailableCapacity(final int requiredCapacity, final boolean doStore)
    {
        long nextValue = this.nextValue;

        // 计算环绕点，即下一个值加上所需容量后得到所需要的序号大小,在用这个序号减去数组长度,得到需要被覆盖的数据的最大索引地址
        long wrapPoint = (nextValue + requiredCapacity) - bufferSize;
        long cachedGatingSequence = this.cachedValue;

        // 检查是否需要更新门控序列,当wrapPoint大于当前的最小的消费序列号时,表示当前的没有足够的空间,需要进一步进行检查
        // 或当前序列值是否超出范围
        // 都需要重新进行检查
        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > nextValue)
        {
            if (doStore)
            {
                cursor.setVolatile(nextValue);  // StoreLoad fence
            }

            // 获取所有门控序列中的最小值作为新的缓存值
            long minSequence = Util.getMinimumSequence(gatingSequences, nextValue);
            //原cachedValue已过期,需要重新赋值
            this.cachedValue = minSequence;

            // 如果环绕点超过了最小序列值，说明没有足够空间
            if (wrapPoint > minSequence)
            {
                return false;
            }
        }
        //直接返回true
        return true;
    }


    /**
     * @see Sequencer#next()
     */
    @Override
    public long next()
    {
        return next(1);
    }

    /**
     * @see Sequencer#next(int)
     */
    @Override
    public long next(final int n)
    {
        //判断线程和生产者是否是最开始的匹配关系,首次获取nextSeq时,会将两者之间存到Map中
        assert sameThread() : "Accessed by two threads - use ProducerType.MULTI!";

        //检测是否超过有效索引长度
        if (n < 1 || n > bufferSize)
        {
            throw new IllegalArgumentException("n must be > 0 and < bufferSize");
        }

        //this.nextValue作为上一次写入索引
        long nextValue = this.nextValue;

        //关注点1:下一次索引+n
        long nextSequence = nextValue + n;

        //关注点2:wrapPoint:理论上的最晚可以消费的索引地址
        long wrapPoint = nextSequence - bufferSize;
        //获取缓存下来的最小消费者序号
        long cachedGatingSequence = this.cachedValue;

        //关注点3:判断nextIndex是否超过当前最小的消费者序号
        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > nextValue)
        {
            //重新设置内存屏障
            cursor.setVolatile(nextValue);  // StoreLoad fence

            //设置局部变量用于暂存最慢的消费索引
            long minSequence;

            //关注点4:循环等待,生产者等待最慢的消费者消费出空闲的位置
            while (wrapPoint > (minSequence = Util.getMinimumSequence(gatingSequences, nextValue)))
            {
                LockSupport.parkNanos(1L); // TODO: Use waitStrategy to spin?
            }
            //重新设置位置
            this.cachedValue = minSequence;
        }

        this.nextValue = nextSequence;

        return nextSequence;
    }

    /**
     * @see Sequencer#tryNext()
     */
    @Override
    public long tryNext() throws InsufficientCapacityException
    {
        return tryNext(1);
    }

    /**
     * @see Sequencer#tryNext(int)
     */
    @Override
    public long tryNext(final int n) throws InsufficientCapacityException
    {
        if (n < 1)
        {
            throw new IllegalArgumentException("n must be > 0");
        }

        if (!hasAvailableCapacity(n, true))
        {
            throw InsufficientCapacityException.INSTANCE;
        }

        long nextSequence = this.nextValue += n;

        return nextSequence;
    }

    /**
     * @see Sequencer#remainingCapacity()
     */
    @Override
    public long remainingCapacity()
    {
        long nextValue = this.nextValue;

        long consumed = Util.getMinimumSequence(gatingSequences, nextValue);
        long produced = nextValue;
        return getBufferSize() - (produced - consumed);
    }

    /**
     * @see Sequencer#claim(long)
     */
    @Override
    public void claim(final long sequence)
    {
        this.nextValue = sequence;
    }

    /**
     * @see Sequencer#publish(long)
     */
    @Override
    public void publish(final long sequence)
    {
        //设置最大序列号
        cursor.set(sequence);
        //通知消费者等待策略唤醒
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * @see Sequencer#publish(long, long)
     */
    @Override
    public void publish(final long lo, final long hi)
    {
        publish(hi);
    }

    /**
     * @see Sequencer#isAvailable(long)
     */
    @Override
    public boolean isAvailable(final long sequence)
    {
        final long currentSequence = cursor.get();
        return sequence <= currentSequence && sequence > currentSequence - bufferSize;
    }

    @Override
    public long getHighestPublishedSequence(final long lowerBound, final long availableSequence)
    {
        return availableSequence;
    }

    @Override
    public String toString()
    {
        return "SingleProducerSequencer{" +
                "bufferSize=" + bufferSize +
                ", waitStrategy=" + waitStrategy +
                ", cursor=" + cursor +
                ", gatingSequences=" + Arrays.toString(gatingSequences) +
                '}';
    }

    private boolean sameThread()
    {
        return ProducerThreadAssertion.isSameThreadProducingTo(this);
    }

    /**
     * Only used when assertions are enabled.
     */
    private static class ProducerThreadAssertion
    {
        /**
         * Tracks the threads publishing to {@code SingleProducerSequencer}s to identify if more than one
         * thread accesses any {@code SingleProducerSequencer}.
         * I.e. it helps developers detect early if they use the wrong
         * {@link com.lmax.disruptor.dsl.ProducerType}.
         */
        private static final Map<SingleProducerSequencer, Thread> PRODUCERS = new HashMap<>();

        public static boolean isSameThreadProducingTo(final SingleProducerSequencer singleProducerSequencer)
        {
            synchronized (PRODUCERS)
            {
                final Thread currentThread = Thread.currentThread();
                if (!PRODUCERS.containsKey(singleProducerSequencer))
                {
                    PRODUCERS.put(singleProducerSequencer, currentThread);
                }
                return PRODUCERS.get(singleProducerSequencer).equals(currentThread);
            }
        }
    }
}
