package com.orientechnologies.ycsb;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.yahoo.ycsb.*;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.Env.open;
import static org.lmdbjava.EnvFlags.MDB_MAPASYNC;
import static org.lmdbjava.EnvFlags.MDB_NOSYNC;

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

  private static volatile Env<ByteBuffer> env;
  private static volatile Dbi<ByteBuffer> lmdb;

  private static final Timer timer = new Timer();
  private static volatile TimerTask syncTask;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    // initialize OrientDB driver
    final Properties props = getProperties();

    String path = "embedded:." + File.separator + "build" + File.separator + "databases" + File.separator + "odb";
    File lmddbPath = new File("." + File.separator + "build" + File.separator + "databases" + File.separator + "lmdb");
    String lmdbName = "ycsb";

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
        }

        db.close();

        if (databasePool == null) {
          databasePool = new ODatabasePool(orientDB, dbName, user, password);
        }

        try {
          lmddbPath = lmddbPath.getCanonicalFile();
        } catch (IOException e) {
          e.printStackTrace();
        }

        lmddbPath.mkdirs();

        env = create()
            // LMDB also needs to know how large our DB might be. Over-estimating is OK.
            .setMapSize(300L * 1024 * 1024 * 1024)

            // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
            .setMaxDbs(1)
            // Now let's open the Env. The same path can be concurrently opened and
            // used in different processes, but do not open the same path twice in
            // the same process at the same time.
            .setMaxReaders(8).open(lmddbPath, MDB_NOSYNC);

        // We need a Dbi for each DB. A Dbi roughly equates to a sorted map. The
        // MDB_CREATE flag causes the DB to be created if it doesn't already exist.
        lmdb = env.openDbi(lmdbName, MDB_CREATE);

        syncTask = new TimerTask() {
          @Override
          public void run() {
            env.sync(true);
          }
        };

        timer.schedule(syncTask, 1000, 1000);

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

        syncTask.cancel();

        lmdb.close();
        env.close();
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
      final ODocument document = new ODocument(CLASS);

      for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        document.field(entry.getKey(), entry.getValue());
      }

      final byte[] doc = document.toStream();
      final byte[] dkey = key.getBytes();

      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        Pointer keyPointer = new Pointer(Native.malloc(dkey.length));
        keyPointer.write(0, dkey, 0, dkey.length);

        Pointer docPointer = new Pointer(Native.malloc(doc.length));
        docPointer.write(0, doc, 0, doc.length);

        lmdb.put(txn, keyPointer.getByteBuffer(0, dkey.length), docPointer.getByteBuffer(0, doc.length));

        // An explicit commit is required, otherwise Txn.close() rolls it back.
        txn.commit();
        Native.free(Pointer.nativeValue(keyPointer));
        Native.free(Pointer.nativeValue(docPointer));
      }

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
      final byte[] dkey = key.getBytes();
      Pointer keyPointer = new Pointer(Native.malloc(dkey.length));
      keyPointer.write(0, dkey, 0, dkey.length);

      try (Txn<ByteBuffer> rtx = env.txnRead()) {
        final ByteBuffer found = lmdb.get(rtx, keyPointer.getByteBuffer(0, dkey.length));
        if (found != null) {
          final ODocument document = new ODocument(CLASS);
          final byte[] binDoc = new byte[found.limit()];
          found.position(0);
          found.get(binDoc);

          document.fromStream(binDoc);

          if (fields != null) {
            for (String field : fields) {
              result.put(field, new StringByteIterator((String) document.field(field)));
            }
          } else {
            for (String field : document.fieldNames()) {
              result.put(field, new StringByteIterator((String) document.field(field)));
            }
          }

          Native.free(Pointer.nativeValue(keyPointer));

          if (found instanceof DirectBuffer) {
            final Cleaner cleaner = ((DirectBuffer) found).cleaner();
            if (cleaner != null) {
              cleaner.clean();
            }
          }
          return Status.OK;
        }
      }

      Native.free(Pointer.nativeValue(keyPointer));
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
    final byte[] dkey = key.getBytes();

    try (ODatabaseDocument db = databasePool.acquire()) {
      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        Pointer keyPointer = new Pointer(Native.malloc(dkey.length));
        keyPointer.write(0, dkey, 0, dkey.length);

        final ByteBuffer found = lmdb.get(txn, keyPointer.getByteBuffer(0, dkey.length));
        if (found != null) {
          final ODocument document = new ODocument(CLASS);
          final byte[] binDoc = new byte[found.limit()];
          found.position(0);
          found.get(binDoc);

          document.fromStream(binDoc);

          for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
            document.field(entry.getKey(), entry.getValue());
          }

          final byte[] doc = document.toStream();
          Pointer docPointer = new Pointer(Native.malloc(doc.length));
          docPointer.write(0, doc, 0, doc.length);

          lmdb.put(txn, keyPointer.getByteBuffer(0, dkey.length), docPointer.getByteBuffer(0, doc.length));

          txn.commit();
          Native.free(Pointer.nativeValue(keyPointer));
          Native.free(Pointer.nativeValue(docPointer));

          if (found instanceof DirectBuffer) {
            final Cleaner cleaner = ((DirectBuffer) found).cleaner();
            if (cleaner != null) {
              cleaner.clean();
            }
          }

          return Status.OK;
        }

        Native.free(Pointer.nativeValue(keyPointer));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    return Status.ERROR;
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
