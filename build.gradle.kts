plugins {
    java
}

repositories {
    mavenCentral()
    mavenLocal()
}

val jacksonApiVersion by extra { "2.6.1" }

dependencies {
    implementation("com.orientechnologies:orientdb-core:3.2.27")
    implementation("com.fasterxml.jackson.core:jackson-core:2.16.1")
    implementation("org.hdrhistogram:HdrHistogram:2.1.12")
    implementation("org.apache.commons:commons-csv:1.10.0")
}

tasks.register("benchmark") {
    doLast {
        val hdrhistogramFileoutput = System.getProperty("ycsb.hdrhistogram.fileoutput")
        val hdrhistogramOutputPath = System.getProperty("ycsb.hdrhistogram.output.path")
        val threads = System.getProperty("ycsb.threads")
        val target = System.getProperty("ycsb.target")
        val load = System.getProperty("ycsb.load")
        val status = System.getProperty("ycsb.status")
        val label = System.getProperty("ycsb.label")
        val workload = System.getProperty("ycsb.workload")
        val operationcount = System.getProperty("ycsb.operationcount")
        val recordcount = System.getProperty("ycsb.recordcount")
        val path = System.getProperty("orientdb.path")
        val dbName = System.getProperty("orientdb.database")
        val user = System.getProperty("orientdb.user")
        val password = System.getProperty("orientdb.password")
        val newdb = System.getProperty("orientdb.newdb")
        val settings = System.getProperty("ycsb.settings")
        val jvmAgent = System.getProperty("jvm.agent.path")
        val debug = System.getProperty("ycsb.debug")
        val csvStatusFile = System.getProperty("csvstatusfile")
        val csvMeasurements = System.getProperty("csvmeasurements")
        val measurementInterval = System.getProperty("ycsb.measurement.interval")
        val loggc = System.getProperty("ycsb.loggc")
        val fieldlengthdistribution = System.getProperty("ycsb.fieldlengthdistribution")
        val requestdistribution = System.getProperty("ycsb.requestdistribution")

        val javaExec = System.getProperty("java.home") + "/bin/java"
        val classPath =
            sourceSets["main"].runtimeClasspath.files.joinToString(separator = File.pathSeparator)

        val params = mutableListOf(javaExec)

        debug?.let { params.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005") }
        jvmAgent?.let { params.add("-agentpath:$it") }

        params.add("-XX:MaxDirectMemorySize=512g")

        loggc?.let {
            params.add("-XX:+PrintGCDetails")
            params.add("-XX:+PrintGCDateStamps")
            params.add("-Xloggc:$it")
        }

        mapOf(
            "orientdb.path" to path,
            "orientdb.database" to dbName,
            "ycsb.hdrhistogram.fileoutput" to hdrhistogramFileoutput,
            "ycsb.hdrhistogram.output.path" to hdrhistogramOutputPath,
            "ycsb.threads" to threads,
            "ycsb.target" to target,
            "ycsb.load" to load,
            "ycsb.status" to status,
            "ycsb.label" to label,
            "ycsb.workload" to workload,
            "ycsb.operationcount" to operationcount,
            "ycsb.recordcount" to recordcount,
            "orientdb.user" to user,
            "orientdb.password" to password,
            "orientdb.newdb" to newdb,
            "ycsb.settings" to settings,
            "csvstatusfile" to csvStatusFile,
            "csvmeasurements" to csvMeasurements,
            "ycsb.measurement.interval" to measurementInterval,
            "ycsb.fieldlengthdistribution" to fieldlengthdistribution,
            "ycsb.requestdistribution" to requestdistribution
        ).forEach { (key, value) ->
            value?.let { params.add("-D$key=$it") }
        }

        params.addAll(
            listOf(
                "-Dcom.sun.management.jmxremote",
                "-Dcom.sun.management.jmxremote.port=1617",
                "-Dcom.sun.management.jmxremote.authenticate=false",
                "-Dcom.sun.management.jmxremote.ssl=false",
                "-classpath",
                classPath,
                "com.orientechnologies.ycsb.Client"
            )
        )

        println(params)


        // Continue from ProcessBuilder
        val processBuilder = ProcessBuilder(params)
        val process = processBuilder.start()

        val eist = Thread {
            process.errorStream.bufferedReader().lines().forEach { line ->
                println(line)
            }
        }
        eist.isDaemon = true


        val ist = Thread {
            process.inputStream.bufferedReader().lines().forEach { line ->
                println(line)
            }
        }
        ist.isDaemon = true
        ist.start()


        eist.start()

        // Wait for the process to complete
        val exitCode = process.waitFor()

        // Check for normal termination
        if (exitCode != 0) {
            throw RuntimeException("Benchmark process terminated with exit code $exitCode")
        }
    }
}

