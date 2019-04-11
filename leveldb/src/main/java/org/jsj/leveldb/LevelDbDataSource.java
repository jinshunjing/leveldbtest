package org.jsj.leveldb;

//import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.*;

import org.jsj.leveldb.util.FileUtil;

/**
 * LevelDB数据源
 *
 * @author JSJ
 */
@Slf4j
public class LevelDbDataSource {

    private String parentName;
    private String dataBaseName;

    private DBFactory factory;
    private Options options;
    private DB database;
    private boolean alive;

    private ReadWriteLock resetDbLock = new ReentrantReadWriteLock();

    /**
     * 配置
     *
     * @return
     */
    private static Options defaultOptions() {
        Options dbOptions = new Options();
        dbOptions.createIfMissing(true);

        // 是否压缩
        dbOptions.compressionType(CompressionType.NONE);

        // 缓存大小
        dbOptions.cacheSize(0);

        dbOptions.maxOpenFiles(32);
        dbOptions.blockSize(10 * 1024 * 1024);
        dbOptions.writeBufferSize(10 * 1024 * 1024);

        dbOptions.paranoidChecks(true);
        dbOptions.verifyChecksums(true);

        // 自定义键比较器
        //dbOptions.comparator(DBComparator);

        return dbOptions;
    }

    /**
     * constructor.
     */
    public LevelDbDataSource(String parentName, String directory, String name) throws Exception {
        this.dataBaseName = name;
        this.parentName = Paths.get(
                parentName,
                directory
        ).toString();

        ClassLoader cl = LevelDbDataSource.class.getClassLoader();
        factory = (DBFactory) cl.loadClass(
                System.getProperty("leveldb.factory", "org.iq80.leveldb.impl.Iq80DBFactory")).newInstance();

        options = defaultOptions();
    }

    public DB getDatabase() {
        if (!alive) {
            openDB();
        }
        return database;
    }

    public boolean isAlive() {
        return alive;
    }

    /**
     * open database
     */
    public void openDB() {
        resetDbLock.writeLock().lock();
        try {
            log.debug("~> LevelDbDataSource.initDB(): " + dataBaseName);

            if (alive) {
                return;
            }

            if (dataBaseName == null) {
                throw new NullPointerException("no name set to the dbStore");
            }

            try {
                openDatabase();
                alive = true;
            } catch (IOException ioe) {
                throw new RuntimeException("Can't initialize database", ioe);
            }
        } finally {
            resetDbLock.writeLock().unlock();
        }
    }

    private void openDatabase() throws IOException {
        // 数据库路径：parentName / dataBaseName
        final Path dbPath = getDbPath();
        if (!Files.isSymbolicLink(dbPath.getParent())) {
            Files.createDirectories(dbPath.getParent());
        }

        try {
            database = factory.open(dbPath.toFile(), options);
        } catch (IOException e) {
            if (e.getMessage().contains("Corruption:")) {
                factory.repair(dbPath.toFile(), options);
                database = factory.open(dbPath.toFile(), options);
            } else {
                throw e;
            }
        }
    }

    /**
     * close database
     */
    public void closeDB() {
        resetDbLock.writeLock().lock();
        try {
            if (!alive) {
                return;
            }
            database.close();
            alive = false;
        } catch (IOException e) {
            log.error("Failed to find the dbStore file on the closeDB: {} ", dataBaseName);
        } finally {
            resetDbLock.writeLock().unlock();
        }
    }

    /**
     * reset database.
     */
    public void resetDB() {
        closeDB();
        FileUtil.recursiveDelete(getDbPath().toString());
        openDB();
    }

    /**
     * destroy database.
     */
    public void destroyDB(File fileLocation) {
        resetDbLock.writeLock().lock();
        try {
            log.debug("Destroying existing database: " + fileLocation);
            Options options = new Options();
            try {
                factory.destroy(fileLocation, options);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        } finally {
            resetDbLock.writeLock().unlock();
        }
    }

    private Path getDbPath() {
        return Paths.get(parentName, dataBaseName);
    }
}
