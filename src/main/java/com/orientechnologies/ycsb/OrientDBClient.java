package com.orientechnologies.ycsb;

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.yahoo.ycsb.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * OrientDB client for YCSB framework.
 * <p>
 * Properties to set:
 * <p>
 * orientdb.path=path to file of local database<br> orientdb.database=ycsb <br> orientdb.user=admin
 * <br> orientdb.password=admin <br>
 *
 * @author Luca Garulli
 */
public class OrientDBClient extends DB {

  private static final String CLASS = "usertable";

  private static final Lock initLock = new ReentrantLock();
  public static final String YCSB_INDEX_KEY = "$ycsb_index_key";
  private static boolean dbCreated = false;

  private static volatile ODatabasePool databasePool;
  private static volatile OrientDB orientDB;

  private static boolean initialized = false;
  private static int clientCounter = 0;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per
   * client thread.
   */
  public void init() throws DBException {
    // initialize OrientDB driver
    final Properties props = getProperties();

    String path = "embedded:" + props.getProperty("orientdb.path", "." + File.separator
        + "build" + File.separator + "databases");
    String dbName = props.getProperty("orientdb.database", "ycsb");

    String user = props.getProperty("orientdb.user", "admin");
    String password = props.getProperty("orientdb.password", "admin");
    boolean newdb = Boolean.parseBoolean(props.getProperty("orientdb.newdb", "false"));

    initLock.lock();
    try {
      clientCounter++;
      if (!initialized) {
        System.out.println("OrientDB loading database with path = " + path);
        orientDB = new OrientDB(path, OrientDBConfig.defaultConfig());

        if (!dbCreated && newdb && orientDB.exists(dbName)) {
          orientDB.drop(dbName);
          System.out.println("OrientDB drop and recreate fresh db");
        }

        if (!orientDB.exists(dbName)) {
          orientDB.execute("create database " + dbName +
              " plocal users (admin identified by 'admin' role admin)");

          dbCreated = true;
        }

        ODatabaseDocument db = orientDB.open(dbName, user, password);

        if (!db.getMetadata().getSchema().existsClass(CLASS)) {
          var clz = db.getMetadata().getSchema().createClass(CLASS);
          var prop = clz.createProperty(YCSB_INDEX_KEY, OType.STRING);
          prop.createIndex(INDEX_TYPE.UNIQUE);
        }

        db.close();

        if (databasePool == null) {
          databasePool = new ODatabasePool(orientDB, dbName, user, password);
        }

        initialized = true;
      }
    } catch (Exception e) {
      System.err.println("Could not initialize OrientDB connection pool for Loader: " + e);
      //noinspection CallToPrintStackTrace
      e.printStackTrace();
    } finally {
      initLock.unlock();
    }

  }

  @Override
  public void cleanup() {
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
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be
   * written into the record with the specified record key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See this class's description for a
   * discussion of error codes.
   */
  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    try (ODatabaseDocument ignored = databasePool.acquire()) {
      final ODocument document = new ODocument(CLASS);

      for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        document.setProperty(entry.getKey(), entry.getValue());
      }

      document.setProperty(YCSB_INDEX_KEY, key);
      document.save();

      return Status.OK;
    } catch (Exception e) {
      //noinspection CallToPrintStackTrace
      e.printStackTrace();
    }

    return Status.ERROR;
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See this class's description for a
   * discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    while (true) {
      try (ODatabaseDocument db = databasePool.acquire()) {
        db.query("delete from " + CLASS + " where " + YCSB_INDEX_KEY + " = ?", key);
        return Status.OK;
      } catch (OConcurrentModificationException cme) {
        //ignore
      } catch (Exception e) {
        //noinspection CallToPrintStackTrace
        e.printStackTrace();
        return Status.ERROR;
      }
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a
   * HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    try (ODatabaseDocument db = databasePool.acquire()) {
      try (var queryResult = db.query("select * from " + CLASS + " where " + YCSB_INDEX_KEY +
          " = ?", key)) {
        var document = queryResult.next().getElement();
        if (document.isPresent()) {
          if (fields != null) {
            for (String field : fields) {
              var doc = document.get();
              result.put(field, new StringByteIterator(doc.getProperty(field)));
            }
          } else {
            var doc = document.get();
            for (String field : doc.getPropertyNames()) {
              result.put(field, new StringByteIterator(doc.getProperty(field)));
            }
          }
          return Status.OK;
        } else {
          System.out.println("Document was not found");
          return Status.ERROR;
        }
      }
    } catch (Exception e) {
      //noinspection CallToPrintStackTrace
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be
   * written into the record with the specified record key, overwriting any existing values with the
   * same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's description f or a
   * discussion of error codes.
   */
  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    while (true) {
      try (ODatabaseDocument db = databasePool.acquire()) {
        try (var queryResult = db.query("select * from " + CLASS + " where " + YCSB_INDEX_KEY +
            " = ?", key)) {
          var document = queryResult.next().getElement();

          if (document.isPresent()) {
            var doc = document.get();
            for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(values)
                .entrySet()) {

              doc.setProperty(entry.getKey(), entry.getValue());
            }

            doc.save();
            return Status.OK;
          } else {
            System.out.println("Document was not found");
            return Status.ERROR;
          }
        }
      } catch (OConcurrentModificationException cme) {
        //ignore
      } catch (Exception e) {
        //noinspection CallToPrintStackTrace
        e.printStackTrace();
        return Status.ERROR;
      }
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the
   * result will be stored in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one
   *                    record
   * @return Zero on success, a non-zero error code on error. See this class's description for a
   * discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    try (ODatabaseDocument db = databasePool.acquire()) {
      try (var queryResults = db.query("select * from " + CLASS + " where " + YCSB_INDEX_KEY +
          " >=  ?", startkey)) {
        int currentCount = 0;

        while (queryResults.hasNext()) {
          var queryResult = queryResults.next();
          var documentResult = queryResult.getElement();

          if (documentResult.isEmpty()) {
            System.out.println("Document was not found");
            return Status.ERROR;
          }

          final HashMap<String, ByteIterator> map = new HashMap<>();
          result.add(map);

          var document = documentResult.get();
          if (fields != null) {
            for (String field : fields) {
              map.put(field, new StringByteIterator(document.getProperty(field)));
            }
          } else {
            for (String field : document.getPropertyNames()) {
              map.put(field, new StringByteIterator(document.getProperty(field)));
            }
          }

          currentCount++;

          if (currentCount >= recordcount) {
            break;
          }
        }

        return Status.OK;
      }

    } catch (Exception e) {
      //noinspection CallToPrintStackTrace
      e.printStackTrace();
    }
    return Status.ERROR;
  }
}
