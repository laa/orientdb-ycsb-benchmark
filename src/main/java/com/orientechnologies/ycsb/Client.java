package com.orientechnologies.ycsb;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter;

import java.io.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * A thread to periodically show the status of the experiment, to reassure you that progress is being made.
 *
 * @author cooperb
 */
class StatusThread extends Thread {
  /**
   * Counts down each of the clients completing.
   */
  private final CountDownLatch _completeLatch;

  /**
   * The clients that are running.
   */
  private final List<ClientThread> _clients;

  private final String  _label;
  private final boolean _standardstatus;

  /**
   * The interval for reporting status.
   */
  private long _sleeptimeNs;

  /**
   * Creates a new StatusThread.
   *
   * @param completeLatch         The latch that each client thread will {@link CountDownLatch#countDown()} as they complete.
   * @param clients               The clients to collect metrics from.
   * @param label                 The label for the status.
   * @param standardstatus        If true the status is printed to stdout in addition to stderr.
   * @param statusIntervalSeconds The number of seconds between status updates.
   */
  public StatusThread(CountDownLatch completeLatch, List<ClientThread> clients, String label, boolean standardstatus,
      int statusIntervalSeconds) {
    _completeLatch = completeLatch;
    _clients = clients;
    _label = label;
    _standardstatus = standardstatus;
    _sleeptimeNs = TimeUnit.SECONDS.toNanos(statusIntervalSeconds);
  }

  /**
   * Run and periodically report status.
   */
  @Override
  public void run() {
    final long startTimeMs = System.currentTimeMillis();
    final long startTimeNanos = System.nanoTime();
    long deadline = startTimeNanos + _sleeptimeNs;
    long startIntervalMs = startTimeMs;
    long lastTotalOps = 0;

    boolean alldone;

    do {
      long nowMs = System.currentTimeMillis();

      lastTotalOps = computeStats(startTimeMs, startIntervalMs, nowMs, lastTotalOps);

      alldone = waitForClientsUntil(deadline);

      startIntervalMs = nowMs;
      deadline += _sleeptimeNs;
    } while (!alldone);

    // Print the final stats.
    computeStats(startTimeMs, startIntervalMs, System.currentTimeMillis(), lastTotalOps);
  }

  /**
   * Computes and prints the stats.
   *
   * @param startTimeMs     The start time of the test.
   * @param startIntervalMs The start time of this interval.
   * @param endIntervalMs   The end time (now) for the interval.
   * @param lastTotalOps    The last total operations count.
   * @return The current operation count.
   */
  private long computeStats(final long startTimeMs, long startIntervalMs, long endIntervalMs, long lastTotalOps) {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

    long totalops = 0;
    long todoops = 0;

    // Calculate the total number of operations completed.
    for (ClientThread t : _clients) {
      totalops += t.getOpsDone();
      todoops += t.getOpsTodo();
    }

    long interval = endIntervalMs - startTimeMs;
    double throughput = 1000.0 * (((double) totalops) / (double) interval);
    double curthroughput = 1000.0 * (((double) (totalops - lastTotalOps)) / ((double) (endIntervalMs - startIntervalMs)));
    long estremaining = (long) Math.ceil(todoops / throughput);

    DecimalFormat d = new DecimalFormat("#.##");
    String label = _label + format.format(new Date());

    StringBuilder msg = new StringBuilder(label).append(" ").append(interval / 1000).append(" sec: ");
    msg.append(totalops).append(" operations; ");

    if (totalops != 0) {
      msg.append(d.format(curthroughput)).append(" current ops/sec; ");
    }
    if (todoops != 0) {
      msg.append("est completion in ").append(RemainingFormatter.format(estremaining));
    }

    msg.append(Measurements.getMeasurements().getSummary());

    System.err.println(msg);

    if (_standardstatus) {
      System.out.println(msg);
    }
    return totalops;
  }

  /**
   * Waits for all of the client to finish or the deadline to expire.
   *
   * @param deadline The current deadline.
   * @return True if all of the clients completed.
   */
  private boolean waitForClientsUntil(long deadline) {
    boolean alldone = false;
    long now = System.nanoTime();

    while (!alldone && now < deadline) {
      try {
        alldone = _completeLatch.await(deadline - now, TimeUnit.NANOSECONDS);
      } catch (InterruptedException ie) {
        // If we are interrupted the thread is being asked to shutdown.
        // Return true to indicate that and reset the interrupt state
        // of the thread.
        Thread.currentThread().interrupt();
        alldone = true;
      }
      now = System.nanoTime();
    }

    return alldone;
  }
}

/**
 * Turn seconds remaining into more useful units.
 * i.e. if there are hours or days worth of seconds, use them.
 */
class RemainingFormatter {
  public static StringBuilder format(long seconds) {
    StringBuilder time = new StringBuilder();
    long days = TimeUnit.SECONDS.toDays(seconds);
    if (days > 0) {
      time.append(days).append(" days ");
      seconds -= TimeUnit.DAYS.toSeconds(days);
    }
    long hours = TimeUnit.SECONDS.toHours(seconds);
    if (hours > 0) {
      time.append(hours).append(" hours ");
      seconds -= TimeUnit.HOURS.toSeconds(hours);
    }
    /* Only include minute granularity if we're < 1 day. */
    if (days < 1) {
      long minutes = TimeUnit.SECONDS.toMinutes(seconds);
      if (minutes > 0) {
        time.append(minutes).append(" minutes ");
        seconds -= TimeUnit.MINUTES.toSeconds(seconds);
      }
    }
    /* Only bother to include seconds if we're < 1 minute */
    if (time.length() == 0) {
      time.append(seconds).append(" seconds ");
    }
    return time;
  }
}

/**
 * A thread for executing transactions or data inserts to the database.
 *
 * @author cooperb
 */
class ClientThread extends Thread {
  private static boolean        _spinSleep;
  final          Measurements   _measurements;
  /**
   * Counts down each of the clients completing.
   */
  private final  CountDownLatch _completeLatch;
  DB         _db;
  boolean    _dotransactions;
  Workload   _workload;
  int        _opcount;
  double     _targetOpsPerMs;
  int        _opsdone;
  int        _threadid;
  int        _threadcount;
  Object     _workloadstate;
  Properties _props;
  long       _targetOpsTickNs;

  /**
   * Constructor.
   *
   * @param db                   the DB implementation to use
   * @param dotransactions       true to do transactions, false to insert data
   * @param workload             the workload to use
   * @param props                the properties defining the experiment
   * @param opcount              the number of operations (transactions or inserts) to do
   * @param targetperthreadperms target number of operations per thread per ms
   * @param completeLatch        The latch tracking the completion of all clients.
   */
  public ClientThread(DB db, boolean dotransactions, Workload workload, Properties props, int opcount, double targetperthreadperms,
      CountDownLatch completeLatch) {
    _db = db;
    _dotransactions = dotransactions;
    _workload = workload;
    _opcount = opcount;
    _opsdone = 0;
    if (targetperthreadperms > 0) {
      _targetOpsPerMs = targetperthreadperms;
      _targetOpsTickNs = (long) (1000000 / _targetOpsPerMs);
    }
    _props = props;
    _measurements = Measurements.getMeasurements();
    _spinSleep = Boolean.valueOf(_props.getProperty("spin.sleep", "false"));
    _completeLatch = completeLatch;
  }

  static void sleepUntil(long deadline) {
    long now = System.nanoTime();
    while ((now = System.nanoTime()) < deadline) {
      if (!_spinSleep) {
        LockSupport.parkNanos(deadline - now);
      }
    }
  }

  public int getOpsDone() {
    return _opsdone;
  }

  @Override
  public void run() {
    try {
      _db.init();
    } catch (DBException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return;
    }

    try {
      _workloadstate = _workload.initThread(_props, _threadid, _threadcount);
    } catch (WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return;
    }

    //NOTE: Switching to using nanoTime and parkNanos for time management here such that the measurements
    // and the client thread have the same view on time.

    //spread the thread operations out so they don't all hit the DB at the same time
    // GH issue 4 - throws exception if _target>1 because random.nextInt argument must be >0
    // and the sleep() doesn't make sense for granularities < 1 ms anyway
    if ((_targetOpsPerMs > 0) && (_targetOpsPerMs <= 1.0)) {
      long randomMinorDelay = Utils.random().nextInt((int) _targetOpsTickNs);
      sleepUntil(System.nanoTime() + randomMinorDelay);
    }
    try {
      if (_dotransactions) {
        long startTimeNanos = System.nanoTime();

        while (((_opcount == 0) || (_opsdone < _opcount)) && !_workload.isStopRequested()) {

          if (!_workload.doTransaction(_db, _workloadstate)) {
            break;
          }

          _opsdone++;

          throttleNanos(startTimeNanos);
        }
      } else {
        long startTimeNanos = System.nanoTime();

        while (((_opcount == 0) || (_opsdone < _opcount)) && !_workload.isStopRequested()) {

          if (!_workload.doInsert(_db, _workloadstate)) {
            break;
          }

          _opsdone++;

          throttleNanos(startTimeNanos);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }

    try {
      _measurements.setIntendedStartTimeNs(0);
      _db.cleanup();
    } catch (DBException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return;
    } finally {
      _completeLatch.countDown();
    }
  }

  private void throttleNanos(long startTimeNanos) {
    //throttle the operations
    if (_targetOpsPerMs > 0) {
      // delay until next tick
      long deadline = startTimeNanos + _opsdone * _targetOpsTickNs;
      sleepUntil(deadline);
      _measurements.setIntendedStartTimeNs(deadline);
    }
  }

  /**
   * the total amount of work this thread is still expected to do
   */
  public int getOpsTodo() {
    int todo = _opcount - _opsdone;
    return todo < 0 ? 0 : todo;
  }
}

/**
 * Main class for executing YCSB.
 */

public class Client {
  public static final String DEFAULT_RECORD_COUNT = "0";

  /**
   * The target number of operations to perform.
   */
  public static final String OPERATION_COUNT_PROPERTY = "operationcount";

  /**
   * The number of records to load into the database initially.
   */
  public static final String RECORD_COUNT_PROPERTY = "recordcount";

  /**
   * The workload class to be loaded.
   */
  public static final String WORKLOAD_PROPERTY = "workload";

  /**
   * The database class to be used.
   */
  public static final String DB_PROPERTY = "db";

  /**
   * The exporter class to be used. The default is
   * com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter.
   */
  public static final String EXPORTER_PROPERTY = "exporter";

  /**
   * If set to the path of a file, YCSB will write all output to this file
   * instead of STDOUT.
   */
  public static final String EXPORT_FILE_PROPERTY = "exportfile";

  /**
   * The number of YCSB client threads to run.
   */
  public static final String THREAD_COUNT_PROPERTY = "threadcount";

  /**
   * Indicates how many inserts to do, if less than recordcount. Useful for partitioning
   * the load among multiple servers, if the client is the bottleneck. Additionally, workloads
   * should support the "insertstart" property, which tells them which record to start at.
   */
  public static final String INSERT_COUNT_PROPERTY = "insertcount";

  /**
   * Target number of operations per second
   */
  public static final String TARGET_PROPERTY = "target";

  /**
   * The maximum amount of time (in seconds) for which the benchmark will be run.
   */
  public static final String MAX_EXECUTION_TIME = "maxexecutiontime";

  /**
   * Whether or not this is the transaction phase (run) or not (load).
   */
  public static final String DO_TRANSACTIONS_PROPERTY = "dotransactions";

  public static boolean checkRequiredProperties(Properties props) {
    if (props.getProperty(WORKLOAD_PROPERTY) == null) {
      System.out.println("Missing property: " + WORKLOAD_PROPERTY);
      return false;
    }

    return true;
  }

  /**
   * Exports the measurements to either sysout or a file using the exporter
   * loaded from conf.
   *
   * @throws IOException Either failed to write to output stream or failed to close it.
   */
  private static void exportMeasurements(Properties props, int opcount, long runtime) throws IOException {
    MeasurementsExporter exporter = null;
    try {
      // if no destination file is provided the results will be written to stdout
      OutputStream out;
      String exportFile = props.getProperty(EXPORT_FILE_PROPERTY);
      if (exportFile == null) {
        out = System.out;
      } else {
        out = new FileOutputStream(exportFile);
      }

      // if no exporter is provided the default text one will be used
      String exporterStr = props.getProperty(EXPORTER_PROPERTY, "com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter");
      try {
        exporter = (MeasurementsExporter) Class.forName(exporterStr).getConstructor(OutputStream.class).newInstance(out);
      } catch (Exception e) {
        System.err.println("Could not find exporter " + exporterStr + ", will use default text reporter.");
        e.printStackTrace();
        exporter = new TextMeasurementsExporter(out);
      }

      exporter.write("OVERALL", "RunTime(ms)", runtime);
      double throughput = 1000.0 * (opcount) / (runtime);
      exporter.write("OVERALL", "Throughput(ops/sec)", throughput);

      Measurements.getMeasurements().exportMeasurements(exporter);
    } finally {
      if (exporter != null) {
        exporter.close();
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    boolean dotransactions;
    int threadcount;
    int target;
    boolean status = false;
    String label;

    props.setProperty("hdrhistogram.fileoutput", System.getProperty("ycsb.hdrhistogram.fileoutput", "true"));
    props.setProperty("hdrhistogram.output.path",
        System.getProperty("ycsb.hdrhistogram.output.path", "." + File.separator + "target" + File.separator + "hdr" + File.separator));

    final int tcount = Integer.parseInt(System.getProperty("ycsb.threads", "1"));
    props.setProperty(THREAD_COUNT_PROPERTY, String.valueOf(tcount));

    final String starget = System.getProperty("ycsb.target");
    if (starget != null) {
      final int ttarget = Integer.parseInt(starget);
      props.setProperty(TARGET_PROPERTY, String.valueOf(ttarget));
    }

    final String sload = System.getProperty("ycsb.load");
    if (sload == null) {
      System.out.println("System property ycsb.load must be specified.");
      System.exit(1);
    }

    dotransactions = !Boolean.valueOf(sload);

    if (Boolean.valueOf(System.getProperty("ycsb.status", "false"))) {
      status = true;
    }

    label = System.getProperty("ycsb.label", "");

    final String sworkload = System.getProperty("ycsb.workload");
    if (sworkload == null) {
      System.out.println("YCSB workload has to be specified (ycsb.workload)");
      System.exit(1);
    }

    final InputStream stream = Client.class.getResourceAsStream("/workloads/" + sworkload);
    if (stream == null) {
      System.out.println("Workload with name " + sworkload + " does not exist");
      System.exit(1);
    }

    props.load(stream);
    stream.close();

    final String soperationCount = System.getProperty("ycsb.operationcount");
    if (soperationCount != null) {
      final int operationCount = Integer.parseInt(soperationCount);
      props.setProperty(OPERATION_COUNT_PROPERTY, String.valueOf(operationCount));
    }

    final String srecordCount = System.getProperty("ycsb.recordcount");
    if (srecordCount != null) {
      final int recordCount = Integer.parseInt(srecordCount);
      props.setProperty(RECORD_COUNT_PROPERTY, String.valueOf(recordCount));
    }

    props.putAll(System.getProperties());

    if (!checkRequiredProperties(props)) {
      System.exit(1);
    }

    props.setProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(dotransactions));

    long maxExecutionTime = Integer.parseInt(props.getProperty(MAX_EXECUTION_TIME, "0"));

    //get number of threads, target and db
    threadcount = Integer.parseInt(props.getProperty(THREAD_COUNT_PROPERTY, "1"));
    target = Integer.parseInt(props.getProperty(TARGET_PROPERTY, "0"));

    //compute the target throughput
    double targetperthreadperms = -1;
    if (target > 0) {
      double targetperthread = ((double) target) / ((double) threadcount);
      targetperthreadperms = targetperthread / 1000.0;
    }

    System.out.println("YCSB Client 0.7");
    System.out.println();
    System.err.println("Loading workload...");

    //show a warning message that creating the workload is taking a while
    //but only do so if it is taking longer than 2 seconds
    //(showing the message right away if the setup wasn't taking very long was confusing people)
    Thread warningthread = new Thread() {
      @Override
      public void run() {
        try {
          sleep(2000);
        } catch (InterruptedException e) {
          return;
        }
        System.err.println(" (might take a few minutes for large data sets)");
      }
    };

    warningthread.start();

    //set up measurements
    Measurements.setProperties(props);

    //load the workload
    ClassLoader classLoader = Client.class.getClassLoader();

    Workload workload = null;

    try {
      Class workloadclass = classLoader.loadClass(props.getProperty(WORKLOAD_PROPERTY));

      workload = (Workload) workloadclass.newInstance();
    } catch (Exception e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }

    try {
      workload.init(props);
    } catch (WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }

    warningthread.interrupt();

    //run the workload

    System.err.println("Starting test.");

    int opcount;
    if (dotransactions) {
      opcount = Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY, "0"));
    } else {
      if (props.containsKey(INSERT_COUNT_PROPERTY)) {
        opcount = Integer.parseInt(props.getProperty(INSERT_COUNT_PROPERTY, "0"));
      } else {
        opcount = Integer.parseInt(props.getProperty(RECORD_COUNT_PROPERTY, DEFAULT_RECORD_COUNT));
      }
    }

    CountDownLatch completeLatch = new CountDownLatch(threadcount);
    final List<ClientThread> clients = new ArrayList<ClientThread>(threadcount);
    for (int threadid = 0; threadid < threadcount; threadid++) {
      final DB db = new DBWrapper(new OrientDBClient());
      db.setProperties(props);

      int threadopcount = opcount / threadcount;

      // ensure correct number of operations, in case opcount is not a multiple of threadcount
      if (threadid < opcount % threadcount) {
        ++threadopcount;
      }

      ClientThread t = new ClientThread(db, dotransactions, workload, props, threadopcount, targetperthreadperms, completeLatch);

      clients.add(t);
    }

    StatusThread statusthread = null;

    if (status) {
      boolean standardstatus = false;
      if (props.getProperty(Measurements.MEASUREMENT_TYPE_PROPERTY, "").compareTo("timeseries") == 0) {
        standardstatus = true;
      }
      int statusIntervalSeconds = Integer.parseInt(props.getProperty("status.interval", "10"));
      statusthread = new StatusThread(completeLatch, clients, label, standardstatus, statusIntervalSeconds);
      statusthread.start();
    }

    long st = System.currentTimeMillis();

    for (Thread t : clients) {
      t.start();
    }

    Thread terminator = null;

    if (maxExecutionTime > 0) {
      terminator = new TerminatorThread(maxExecutionTime, clients, workload);
      terminator.start();
    }

    int opsDone = 0;

    for (Thread t : clients) {
      try {
        t.join();
        opsDone += ((ClientThread) t).getOpsDone();
      } catch (InterruptedException e) {
      }
    }

    long en = System.currentTimeMillis();

    if (terminator != null && !terminator.isInterrupted()) {
      terminator.interrupt();
    }

    if (status) {
      // wake up status thread if it's asleep
      statusthread.interrupt();
      // at this point we assume all the monitored threads are already gone as per above join loop.
      try {
        statusthread.join();
      } catch (InterruptedException e) {
      }
    }

    try {
      workload.cleanup();
    } catch (WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }

    try {
      exportMeasurements(props, opsDone, en - st);
    } catch (IOException e) {
      System.err.println("Could not export measurements, error: " + e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }

    System.exit(0);
  }
}
