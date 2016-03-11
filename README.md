# orientdb-ycsb-benchmark
YCSB benchmark for OrientDB project which was refactored to run inside of maven. Based on 0.7 version of YCSB.

Following YCSB properties are supported through system properties:

`ycsb.threads` - amount of parallel threads for given benchmark, 1 by default.<br>
`ycsb.target` - target of load test, amount of operations per second, unlimited by defautl.<br>
`ycsb.load` - if set to `true` test will run in load mode for given workload, if set to `false` run mode will be used, required.<br>
`ycsb.status` - whether test will show periodical status report.<br>
`ycsb.workload` - name of workload to run, required.<br>
`ycsb.operationcount` - amount of operations to execute for porvided workload.<br>
`ycsb.hdrhistogram.fileoutput` - output result of performance measurements as hdrhistogram file, true by default.<br>
`ycsb.hdrhistogram.output.path` - path to location of hdrhistogram files, by default "./target/hdr".<br>
`orientdb.url` - url to OrientDB instance, "./target/databases/ycsb" by default.<br>
`orientdb.user` - OrientDB user name, "admin" by default.<br>
`orientdb.password` - OrientDB user password, "admin" by default<br>
`orientdb.newdb` - create new database before running a workload.<br>

Typcal use case of running of workloads consist of following commands:<br>
`mvn package` - build project<br>
or<br>
`mvn clean` - clean old data<br>
then <br>
`mvn exec:java -Dycsb.load=true -Dycsb.workload=workloada -Dycsb.status=true -Dorientdb.newdb=true -Dycsb.operationcount=1000` - create new database and load initial data of "workloada" workload.<br>
`mvn exec:java -Dycsb.load=false -Dycsb.workload=workloada -Dycsb.status=true -Dorientdb.newdb=false -Dycsb.operationcount=1000` - run "workloada" workload.<br>




