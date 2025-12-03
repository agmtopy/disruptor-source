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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;


/**
 * 多线程场景下的生产者序号协调器
 * Coordinator for claiming sequences for access to a data structure while tracking dependent {@link Sequence}s.
 * Suitable for use for sequencing across multiple publisher threads.
 *
 * <p>Note on {@link Sequencer#getCursor()}:  With this sequencer the cursor value is updated after the call
 * to {@link Sequencer#next()}, to determine the highest available sequence that can be read, then
 * {@link Sequencer#getHighestPublishedSequence(long, long)} should be used.
 */
public final class MultiProducerSequencer extends AbstractSequencer
{
    //获取更原始的数组操作方法,VarHandle这个类值得学习
    private static final VarHandle AVAILABLE_ARRAY = MethodHandles.arrayElementVarHandle(int[].class);
    //初始化最小消费点
    private final Sequence gatingSequenceCache = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

    // availableBuffer tracks the state of each ringbuffer slot
    // see below for more details on the approach
    //跟踪Buffer数组中每一个slot的状态
    private final int[] availableBuffer;
    //位掩码，用于快速计算序列号在缓冲区中的索引位置
    private final int indexMask;
    //位移量，用于计算可用性标志位,标识圈数
    private final int indexShift;

    /**
     * Construct a Sequencer with the selected wait strategy and buffer size.
     *
     * @param bufferSize   the size of the buffer that this will sequence over.
     * @param waitStrategy for those waiting on sequences.
     */
    public MultiProducerSequencer(final int bufferSize, final WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
        //创建状态映射数组
        availableBuffer = new int[bufferSize];
        //初始化状态映射数组中每个值为-1
        Arrays.fill(availableBuffer, -1);

        //设置位掩码
        indexMask = bufferSize - 1;
        //位移量,即为数值1的最高位的位置数
        indexShift = Util.log2(bufferSize);
    }

    /**
     * @see Sequencer#hasAvailableCapacity(int)
     */
    @Override
    public boolean hasAvailableCapacity(final int requiredCapacity)
    {
        return hasAvailableCapacity(gatingSequences, requiredCapacity, cursor.get());
    }

    /**
     * 判断是否还有可用位置
     * PS:是在多线程场景下进行判断
     * @param gatingSequences
     * @param requiredCapacity  需要的空间数量
     * @param cursorValue   当前生产者生产的位置
     * @return 是否还有所需空间
     */
    private boolean hasAvailableCapacity(final Sequence[] gatingSequences, final int requiredCapacity, final long cursorValue)
    {
        //计算环绕点位置,(生产者位置 + 所需位置) - 数组长度,得到新的环绕点位置,也就是本次新申请结束写入覆盖的位置
        long wrapPoint = (cursorValue + requiredCapacity) - bufferSize;
        //获取最小的消费位置
        long cachedGatingSequence = gatingSequenceCache.get();

        // 检查是否需要更新门控序列,当wrapPoint大于当前的最小的消费序列号时,表示当前的没有足够的空间,需要进一步进行检查
        // 或当前序列值是否超出范围,与SingleProducerSequencer.hasAvailableCapacity方法的处理逻辑类似
        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > cursorValue)
        {
            long minSequence = Util.getMinimumSequence(gatingSequences, cursorValue);
            gatingSequenceCache.set(minSequence);

            if (wrapPoint > minSequence)
            {
                return false;
            }
        }

        return true;
    }

    /**
     * @see Sequencer#claim(long)
     */
    @Override
    public void claim(final long sequence)
    {
        cursor.set(sequence);
    }

    /**
     * 核心方法,获取下一个序号
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
        if (n < 1 || n > bufferSize)
        {
            throw new IllegalArgumentException("n must be > 0 and < bufferSize");
        }

        //关键点1:计算生产者进度,当前生产者进度+n赋值到cursor上,返回原cursor的值,使用VarHandle实现
        long current = cursor.getAndAdd(n);
        //得到写入之后的值
        long nextSequence = current + n;
        //得到循环点
        long wrapPoint = nextSequence - bufferSize;
        //得到当前最慢的消费点
        long cachedGatingSequence = gatingSequenceCache.get();

        //关键点2:判断如果循环点大于消费点说明,本次写入会覆盖,因此需要等待消费位点
        //如果最慢的消费位点大于当前生产者位点,说明GatingSequence已过期
        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > current)
        {
            long gatingSequence;
            //关键点3:获取最新的消费位点,如果循环点还是大于消费位点,证明本次写入还是会覆盖,继续等待
            while (wrapPoint > (gatingSequence = Util.getMinimumSequence(gatingSequences, current)))
            {
                LockSupport.parkNanos(1L); // TODO, should we spin based on the wait strategy?
            }
            //将gatingSequenceCache设置成最新的值
            gatingSequenceCache.set(gatingSequence);
        }

        //返回写入之后的值
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

        long current;
        long next;

        do
        {
            //设置当前生产位点
            current = cursor.get();
            //设置本次生产更新后的位点
            next = current + n;
            //容量不够时直接抛出异常
            if (!hasAvailableCapacity(gatingSequences, n, current))
            {
                throw InsufficientCapacityException.INSTANCE;
            }
        }
        //通过CAS的方式更新
        while (!cursor.compareAndSet(current, next));

        return next;
    }

    /**
     * @see Sequencer#remainingCapacity()
     */
    @Override
    public long remainingCapacity()
    {
        long consumed = Util.getMinimumSequence(gatingSequences, cursor.get());
        long produced = cursor.get();
        return getBufferSize() - (produced - consumed);
    }

    /**
     * @see Sequencer#publish(long)
     */
    @Override
    public void publish(final long sequence)
    {
        setAvailable(sequence);
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * @see Sequencer#publish(long, long)
     */
    @Override
    public void publish(final long lo, final long hi)
    {
        for (long l = lo; l <= hi; l++)
        {
            setAvailable(l);
        }
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * The below methods work on the availableBuffer flag.
     *
     * <p>The prime reason is to avoid a shared sequence object between publisher threads.
     * (Keeping single pointers tracking start and end would require coordination
     * between the threads).
     *
     * <p>--  Firstly we have the constraint that the delta between the cursor and minimum
     * gating sequence will never be larger than the buffer size (the code in
     * next/tryNext in the Sequence takes care of that).
     * -- Given that; take the sequence value and mask off the lower portion of the
     * sequence as the index into the buffer (indexMask). (aka modulo operator)
     * -- The upper portion of the sequence becomes the value to check for availability.
     * ie: it tells us how many times around the ring buffer we've been (aka division)
     * -- Because we can't wrap without the gating sequences moving forward (i.e. the
     * minimum gating sequence is effectively our last available position in the
     * buffer), when we have new data and successfully claimed a slot we can simply
     * write over the top.
     */
    private void setAvailable(final long sequence)
    {
        setAvailableBufferValue(calculateIndex(sequence), calculateAvailabilityFlag(sequence));
    }

    private void setAvailableBufferValue(final int index, final int flag)
    {
        AVAILABLE_ARRAY.setRelease(availableBuffer, index, flag);
    }

    /**
     * @see Sequencer#isAvailable(long)
     */
    @Override
    public boolean isAvailable(final long sequence)
    {
        int index = calculateIndex(sequence);
        int flag = calculateAvailabilityFlag(sequence);
        return (int) AVAILABLE_ARRAY.getAcquire(availableBuffer, index) == flag;
    }

    @Override
    public long getHighestPublishedSequence(final long lowerBound, final long availableSequence)
    {
        for (long sequence = lowerBound; sequence <= availableSequence; sequence++)
        {
            if (!isAvailable(sequence))
            {
                return sequence - 1;
            }
        }

        return availableSequence;
    }

    private int calculateAvailabilityFlag(final long sequence)
    {
        return (int) (sequence >>> indexShift);
    }

    private int calculateIndex(final long sequence)
    {
        return ((int) sequence) & indexMask;
    }

    @Override
    public String toString()
    {
        return "MultiProducerSequencer{" +
                "bufferSize=" + bufferSize +
                ", waitStrategy=" + waitStrategy +
                ", cursor=" + cursor +
                ", gatingSequences=" + Arrays.toString(gatingSequences) +
                '}';
    }
}
