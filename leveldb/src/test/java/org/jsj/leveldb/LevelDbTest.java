package org.jsj.leveldb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
public class LevelDbTest {

    @Before
    public void setUp() {
        System.out.println();
        System.out.println();
    }

    @After
    public void tearDown() {
        System.out.println();
        System.out.println();
    }

    @Test
    public void testOpen()  {
        String parentName = "/Users";
        String directory = "jim";
        String databaseName = "leveldbtest";

        LevelDbDataSource dataSource = null;
        try {
            dataSource = new LevelDbDataSource(parentName, directory, databaseName);
            LevelDbOperations operations = new LevelDbOperations(dataSource);


            //dataSource.putData("owner".getBytes(), "jsj".getBytes());
            //Thread.sleep(500L);

            byte[] data = operations.getData("owner".getBytes());
            System.out.println(new String(data));
            Thread.sleep(500L);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dataSource.closeDB();
        }
    }
}
