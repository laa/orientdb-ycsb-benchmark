# orientdb-ycsb-benchmark
YCSB benchmark for OrientDB project which was refactored to run inside of gradle. Based on 0.7 version of YCSB.
This benchmark is used only to run embedded database not remote one

Following YCSB properties are supported (should be provided as maven system properties):

`ycsb.threads` - amount of parallel threads for given benchmark, 1 by default.<br>
`ycsb.target` - target of load test, amount of operations per second, unlimited by default.<br>
`ycsb.load` - if set to `true` test will run in load mode for given workload, if set to `false` run mode will be used, required.<br>
`ycsb.status` - whether test will show periodical status report.<br>
`ycsb.workload` - name of workload to run, required.<br>
`ycsb.operationcount` - amount of operations to execute for pervaded workload. It is used in run mode.<br>
`ycsb.recordcount` - amount of records inserted/to be inserted in load mode. It is used both in load and run modes<br>
`ycsb.hdrhistogram.fileoutput` - output result of performance measurements as hdrhistogram file, `false` by default.<br>
`ycsb.hdrhistogram.output.path` - path to location of hdrhistogram files, by default "./build/hdr".<br>
`ycsb.settings` - path to property file which may contain all settings listed here and OrientDB configuration properties. 
Empty by default. Any setting provided as system property will override one in property file. Property file is only way to
provide OrientDB configuration properties for embedded storage.<br>
`orientdb.path` - path to directory where all OrientDB databases are placed, "./build/databases" by default.<br>
`orientdb.database`- name of database which will be used in benchmark "ycsb" by default<br>
`orientdb.user` - OrientDB user name, "admin" by default.<br>
`orientdb.password` - OrientDB user password, "admin" by default<br>
`orientdb.newdb` - create new database before running a workload.<br>
`jvm.agent.path` - path to JVM agent, useful in case of remote profiling<br>
`csvstatusfile` - Name of CSV file which will be used to store current status of benchmark, if `csvmeasurements` parameter is not set
first column of CSV file will contain time passed since start of test and second column current throughput<br>
`csvmeasurements` - List of names of measurements, separated by coma, values of which will be stored in CSV file along with benchmark
status. Values of measurements will be contained in CSV file according to order they were listed in value of property<br>
`ycsb.loggc` - Path to the GC log file , empty be default


Typcal use case of running of workloads consist of following commands:<br>
`gradle build` - build project<br>
or<br>
`gradle clean build` - build project and clean old data<br>
then <br>
`gradle benchmark -Dycsb.load=true -Dycsb.workload=workloada -Dycsb.status=true -Dorientdb.newdb=true -Dycsb.recordcount=1000 -Dycsb.threads=8` - create new database and load initial data of "workloada" workload.<br>
`gradle benchmark -Dycsb.load=false -Dycsb.workload=workloada -Dycsb.status=true -Dorientdb.newdb=false -Dycsb.operationcount=500 -Dycsb.recordcount=1000 -Dycsb.threads=8` - run "workloada" workload. Do not forget to porvide record count property, otherwise your distirbution of requests will be incorrect<br>




