package org.jsj.leveldb;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LevelDB操作
 * - 简单的增删改查
 * - 批量修改
 * - 遍历
 * - 读快照？
 *
 * @author JSJ
 */
@Slf4j
public class LevelDbOperations {

    private LevelDbDataSource dataSource;

    private ReadWriteLock resetDbLock = new ReentrantReadWriteLock();

    public LevelDbOperations(LevelDbDataSource dataSource) {
        this.dataSource = dataSource;
    }

    private DB database() {
        return this.dataSource.getDatabase();
    }

    /**
     * 查找单个键值对
     *
     * @param key
     * @return
     */
    public byte[] getData(byte[] key) {
        resetDbLock.readLock().lock();
        try {
            return database().get(key);
        } catch (DBException e) {
            log.debug(e.getMessage(), e);
        } finally {
            resetDbLock.readLock().unlock();
        }
        return null;
    }

    /**
     * 更新/增加单个键值对
     *
     * @param key
     * @param value
     */
    public void putData(byte[] key, byte[] value) {
        resetDbLock.readLock().lock();
        try {
            database().put(key, value);
        } finally {
            resetDbLock.readLock().unlock();
        }
    }

    /**
     * 更新/增加单个键值对
     *
     * @param key
     * @param value
     * @param options
     */
    public void putData(byte[] key, byte[] value, WriteOptions options) {
        resetDbLock.readLock().lock();
        try {
            database().put(key, value, options);
        } finally {
            resetDbLock.readLock().unlock();
        }
    }

    /**
     * 删除单个键值对
     *
     * @param key
     */
    public void deleteData(byte[] key) {
        resetDbLock.readLock().lock();
        try {
            database().delete(key);
        } finally {
            resetDbLock.readLock().unlock();
        }
    }

    /**
     * 删除单个键值对
     *
     * @param key
     * @param options
     */
    public void deleteData(byte[] key, WriteOptions options) {
        resetDbLock.readLock().lock();
        try {
            database().delete(key, options);
        } finally {
            resetDbLock.readLock().unlock();
        }
    }

    /**
     * 更新/删除多个键值对
     * - 值不为NULL表示更新
     * - 值为NULL表示删除
     *
     * @param rows
     */
    public void updateByBatch(Map<byte[], byte[]> rows) {
        resetDbLock.readLock().lock();
        try {
            updateByBatchInner(rows);
        } catch (Exception e) {
            try {
                updateByBatchInner(rows);
            } catch (Exception e1) {
                throw new RuntimeException(e);
            }
        } finally {
            resetDbLock.readLock().unlock();
        }
    }

    /**
     * 更新/删除多个键值对
     * - 值不为NULL表示更新
     * - 值为NULL表示删除
     *
     * @param rows
     * @param options
     */
    public void updateByBatch(Map<byte[], byte[]> rows, WriteOptions options) {
        resetDbLock.readLock().lock();
        try {
            updateByBatchInner(rows, options);
        } catch (Exception e) {
            try {
                updateByBatchInner(rows);
            } catch (Exception e1) {
                throw new RuntimeException(e);
            }
        } finally {
            resetDbLock.readLock().unlock();
        }
    }

    private void updateByBatchInner(Map<byte[], byte[]> rows) throws Exception {
        try (WriteBatch batch = database().createWriteBatch()) {
            rows.forEach((key, value) -> {
                if (value == null) {
                    batch.delete(key);
                } else {
                    batch.put(key, value);
                }
            });
            database().write(batch);
        }
    }

    private void updateByBatchInner(Map<byte[], byte[]> rows, WriteOptions options) throws Exception {
        try (WriteBatch batch = database().createWriteBatch()) {
            rows.forEach((key, value) -> {
                if (value == null) {
                    batch.delete(key);
                } else {
                    batch.put(key, value);
                }
            });
            database().write(batch, options);
        }
    }

    /**
     * 返回指定键前面的N个键值对
     * LevelDB里键是有序从小到大排列的
     *
     * @param key
     * @param limit
     * @return
     */
    public Set<byte[]> getValuesPrev(byte[] key, long limit) {
        if (limit <= 0) {
            return Sets.newHashSet();
        }
        resetDbLock.readLock().lock();
        try (DBIterator iterator = database().iterator()) {
            Set<byte[]> result = Sets.newHashSet();
            long i = 0;
            byte[] data = getData(key);
            if (Objects.nonNull(data)) {
                result.add(data);
                i++;
            }
            for (iterator.seek(key); iterator.hasPrev() && i++ < limit; iterator.prev()) {
                result.add(iterator.peekPrev().getValue());
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            resetDbLock.readLock().unlock();
        }
    }

    /**
     * 返回指定键前面的N个键值对
     * LevelDB里键是有序从小到大排列的
     *
     * @param key
     * @param limit
     * @return
     */
    public Map<byte[], byte[]> getPrev(byte[] key, long limit) {
        if (limit <= 0) {
            return Collections.emptyMap();
        }
        resetDbLock.readLock().lock();
        try (DBIterator iterator = database().iterator()) {
            Map<byte[], byte[]> result = new HashMap<>();
            long i = 0;
            for (iterator.seek(key); iterator.hasPrev() && i++ < limit; iterator.prev()) {
                Map.Entry<byte[], byte[]> entry = iterator.peekPrev();
                result.put(entry.getKey(), entry.getValue());
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            resetDbLock.readLock().unlock();
        }
    }

    /**
     * 返回指定键后面的N个键值对
     * LevelDB里键是有序从小到大排列的
     *
     * @param key
     * @param limit
     * @return
     */
    public Set<byte[]> getValuesNext(byte[] key, long limit) {
        if (limit <= 0) {
            return Sets.newHashSet();
        }
        resetDbLock.readLock().lock();
        try (DBIterator iterator = database().iterator()) {
            Set<byte[]> result = Sets.newHashSet();
            long i = 0;
            for (iterator.seek(key); iterator.hasNext() && i++ < limit; iterator.next()) {
                result.add(iterator.peekNext().getValue());
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            resetDbLock.readLock().unlock();
        }
    }

    /**
     * 返回指定键后面的N个键值对
     * LevelDB里键是有序从小到大排列的
     *
     * @param key
     * @param limit
     * @return
     */
    public Map<byte[], byte[]> getNext(byte[] key, long limit) {
        if (limit <= 0) {
            return Collections.emptyMap();
        }
        resetDbLock.readLock().lock();
        try (DBIterator iterator = database().iterator()) {
            Map<byte[], byte[]> result = new HashMap<>();
            long i = 0;
            for (iterator.seek(key); iterator.hasNext() && i++ < limit; iterator.next()) {
                Map.Entry<byte[], byte[]> entry = iterator.peekNext();
                result.put(entry.getKey(), entry.getValue());
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            resetDbLock.readLock().unlock();
        }
    }

    /**
     * 返回前面的N个键值对
     *
     * @param limit
     * @return
     */
    public Set<byte[]> getValuesEarliest(long limit) {
        if (limit <= 0) {
            return Sets.newHashSet();
        }
        resetDbLock.readLock().lock();
        try (DBIterator iterator = database().iterator()) {
            Set<byte[]> result = Sets.newHashSet();
            long i = 0;
            iterator.seekToFirst();
            if (iterator.hasPrev()) {
                result.add(iterator.peekPrev().getValue());
                i++;
            }
            for (; iterator.hasNext() && i++ < limit; iterator.next()) {
                result.add(iterator.peekNext().getValue());
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            resetDbLock.readLock().unlock();
        }
    }

    /**
     * 返回后面的N个键值对
     *
     * @param limit
     * @return
     */
    public Set<byte[]> getValuesLatest(long limit) {
        if (limit <= 0) {
            return Sets.newHashSet();
        }
        resetDbLock.readLock().lock();
        try (DBIterator iterator = database().iterator()) {
            Set<byte[]> result = Sets.newHashSet();
            long i = 0;
            iterator.seekToLast();
            if (iterator.hasNext()) {
                result.add(iterator.peekNext().getValue());
                i++;
            }
            for (; iterator.hasPrev() && i++ < limit; iterator.prev()) {
                result.add(iterator.peekPrev().getValue());
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            resetDbLock.readLock().unlock();
        }
    }

    /**
     * 统计键值对的总数
     *
     * @return
     * @throws RuntimeException
     */
    public long getTotal() throws RuntimeException {
        resetDbLock.readLock().lock();
        try (DBIterator iterator = database().iterator()) {
            long total = 0;
            for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                total++;
            }
            return total;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            resetDbLock.readLock().unlock();
        }
    }
}
