package com.orientechnologies.ycsb;

import com.orientechnologies.lsmtrie.OLSMTrie;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.yahoo.ycsb.*;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * OrientDB client for YCSB framework.
 * Properties to set:
 * orientdb.path=path to file of local database<br>
 * orientdb.database=ycsb <br>
 * orientdb.user=admin <br>
 * orientdb.password=admin <br>
 *
 * @author Luca Garulli
 */
public class OrientDBClient extends DB {
  private static final String CLASS = "usertable";

  private static final Lock    initLock  = new ReentrantLock();
  private static       boolean dbCreated = false;

  private static volatile ODatabasePool databasePool;
  private static volatile OrientDB      orientDB;

  private static boolean initialized   = false;
  private static int     clientCounter = 0;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    // initialize OrientDB driver
    final Properties props = getProperties();

    String path = "embedded:" + props.getProperty("orientdb.path", "." + File.separator + "build" + File.separator + "databases");
    String dbName = props.getProperty("orientdb.database", "ycsb");

    String user = props.getProperty("orientdb.user", "admin");
    String password = props.getProperty("orientdb.password", "admin");
    Boolean newdb = Boolean.parseBoolean(props.getProperty("orientdb.newdb", "false"));

    initLock.lock();
    try {
      clientCounter++;
      if (!initialized) {
        OGlobalConfiguration.dumpConfiguration(System.out);

        System.out.println("OrientDB loading database with path = " + path);
        orientDB = new OrientDB(path, OrientDBConfig.defaultConfig());

        if (!dbCreated && newdb && orientDB.exists(dbName)) {
          orientDB.drop(dbName);
          System.out.println("OrientDB drop and recreate fresh db");
        }

        if (!orientDB.exists(dbName)) {
          orientDB.create(dbName, ODatabaseType.PLOCAL);

          dbCreated = true;
        }

        ODatabaseDocument db = orientDB.open(dbName, user, password);

        if (!db.getMetadata().getSchema().existsClass(CLASS)) {
          db.getMetadata().getSchema().createClass(CLASS);
          db.command("create index keystore DICTIONARY_HASH_INDEX STRING");
        }

        db.close();

        if (databasePool == null) {
          databasePool = new ODatabasePool(orientDB, dbName, user, password);
        }

        initialized = true;
      }
    } catch (Exception e) {
      System.err.println("Could not initialize OrientDB connection pool for Loader: " + e.toString());
      e.printStackTrace();
    } finally {
      initLock.unlock();
    }

  }

  @Override
  public void cleanup() throws DBException {
    initLock.lock();
    try {
      clientCounter--;
      if (clientCounter == 0) {
        databasePool.close();

        orientDB.close();
      }
    } finally {
      initLock.unlock();
    }

  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values
   * HashMap will be written into the record with the specified
   * record key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   *
   * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    try (ODatabaseDocument db = databasePool.acquire()) {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      final int clusterId = random.nextInt(64) + 1;
      final long clusrePosition = random.nextInt(Integer.MAX_VALUE);

      final OIndex keystore = db.getMetadata().getIndexManager().getIndex("keystore");
      keystore.put(key, new ORecordId(clusterId, clusrePosition));

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   *
   * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    while (true) {
      try (ODatabaseDocument db = databasePool.acquire()) {
        final ODictionary<ORecord> dictionary = db.getMetadata().getIndexManager().getDictionary();
        dictionary.remove(key);
        return Status.OK;
      } catch (OConcurrentModificationException cme) {
        continue;
      } catch (Exception e) {
        e.printStackTrace();
        return Status.ERROR;
      }
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   *
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    try (ODatabaseDocument db = databasePool.acquire()) {
      final OIndex keystore = db.getMetadata().getIndexManager().getIndex("keystore");
      final OIdentifiable rid = (OIdentifiable) keystore.get(key);

      if (rid != null) {
        return Status.OK;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values
   * HashMap will be written into the record with the specified
   * record key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   *
   * @return Zero on success, a non-zero error code on error. See this class's description f or a discussion of error codes.
   */
  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    try (ODatabaseDocument db = databasePool.acquire()) {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      final int clusterId = random.nextInt(64) + 1;
      final long clusrePosition = random.nextInt(Integer.MAX_VALUE);

      final OIndex keystore = db.getMetadata().getIndexManager().getIndex("keystore");
      keystore.put(key, new ORecordId(clusterId, clusrePosition));

      return Status.OK;
    }
  }

  /**
   * Perform a range scan for a set of records in the database.
   * Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   *
   * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    try (ODatabaseDocument db = databasePool.acquire()) {
      final ODictionary<ORecord> dictionary = db.getMetadata().getIndexManager().getDictionary();
      final OIndexCursor entries = dictionary.getIndex().iterateEntriesMajor(startkey, true, true);

      int currentCount = 0;
      while (entries.hasNext()) {
        final ODocument document = entries.next().getRecord();

        final HashMap<String, ByteIterator> map = new HashMap<>();
        result.add(map);

        if (fields != null) {
          for (String field : fields) {
            map.put(field, new StringByteIterator((String) document.field(field)));
          }
        } else {
          for (String field : document.fieldNames()) {
            map.put(field, new StringByteIterator((String) document.field(field)));
          }
        }

        currentCount++;

        if (currentCount >= recordcount)
          break;
      }

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }
}
