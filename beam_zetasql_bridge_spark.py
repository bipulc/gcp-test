#! /usr/bin/env python3

import argparse
import logging
import json
import time
import os
import socket
from google.cloud import storage
from google.cloud import bigquery
# Import module
import jpype

# Enable Java imports
import jpype.imports

# Pull in types
from jpype.types import *


class spark_zsql_context:
    def __init__(self, exec_mode):
        # Beam packages
        from org.apache.beam.sdk.extensions.sql import BeamSqlCli
        from org.apache.beam.sdk.extensions.sql.meta.store import InMemoryMetaStore
        from org.apache.beam.sdk.extensions.sql.meta.provider.text import TextTableProvider
        from org.apache.beam.sdk.options import PipelineOptionsFactory
        from org.apache.beam.runners.spark import SparkPipelineOptions
        from org.apache.beam.runners.spark import SparkRunner
        from org.apache.beam.sdk.extensions.sql.impl import BeamSqlPipelineOptions

        sparkOptions = PipelineOptionsFactory().as_(SparkPipelineOptions.class_)
        sparkOptions.setRunner(SparkRunner.class_)
        sparkOptions.setReusesSparkContext(True)

        options = sparkOptions.as_(BeamSqlPipelineOptions.class_)
        options.setPlannerName("org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner")

        metaStore = InMemoryMetaStore()
        metaStore.registerProvider(TextTableProvider())

        self.cli = BeamSqlCli().metaStore(metaStore, False, options)
        self.cli.execute("SET project=`data-analytics-bk`")
        self.cli.execute("SET tempLocation='gs://dp_beamsql'")
        self.cli.execute("SET runner='SparkRunner'")
        if exec_mode == 'local':
            self.cli.execute("SET sparkMaster='local[*]'")
        elif exec_mode == 'cluster':
            self.cli.execute("SET sparkMaster='yarn-client'")
        else:
            print("Wrong execution mode specified, please use local or cluster")
            exit(1)
        self.cli.execute("SET plannerName='org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner'")
        self.cli.execute("SET reusesSparkContext='True'")


    def spark_run(self, config_dict):
        print("SPARK query exec work in progress")
        # import the Java modules for BQ
        import java.lang

        try:
            sqllocation = config_dict['sqlloc']
            schemaLocation = config_dict['schemaloc']
            dataLocation = config_dict['dataloc']
            targetSchema = config_dict['targetschema']
            targetTable = config_dict['targettable']

            sourceDdl = getSQLText(schemaLocation) + " '" + dataLocation + "'"
            print("Launching Java beamSQL source DDL: " + sourceDdl)
            self.cli.execute(sourceDdl)
            targetDdl = getSQLText(targetSchema)
            print("Launching Java beamSQL target DDL: " + targetDdl)
            self.cli.execute(targetDdl)
            print("Launching Java beamSQL query, for SQL file located at: " + sqllocation)
            # sql = 'INSERT INTO ' + targetTable + ' ' + getSQLText(sqllocation).replace('da_streaming_pipeline.', '')
            # sql = 'EXPORT DATA OPTIONS (path=' + targetTable + ') AS ' + \
            #      getSQLText(sqllocation).replace('da_streaming_pipeline.', '')
            sql = getSQLText(sqllocation).replace('da_streaming_pipeline.', '')
            print("Launching Java beamSQL query in Spark, for SQL: " + sql)
            # limit(self.cli.executeDml(sql, targetTable), 240)
            self.cli.executeDml(sql, targetTable)
        except Exception as ex:
            print("Caught the exception : ", str(ex))
            print(ex.stacktrace())


def getSQLText(sqlfile_loc, printsql=False):
    # Function to take SQL File location on GCS and retrun SQL Text

    # # get SQL text from GCS into a variable

    sqlfile_d = os.path.normpath(sqlfile_loc).split(os.sep)
    sqlfile_bucket = sqlfile_d[1]
    sqlfile_name = sqlfile_d[2]+'/'+sqlfile_d[3]

    storage_client = storage.Client()
    bucket = storage_client.bucket(sqlfile_bucket)
    sqlfile_blob = bucket.blob(sqlfile_name)

    sqlcode = sqlfile_blob.download_as_string().decode('utf-8')
    if printsql:
        print("Query Code: ")
        print(sqlcode)
        print(" ")

    return sqlcode


def bq_run(bq_client,config_dict):

    # Parse config_dict into separate elements
    sqlfile_loc = config_dict['sqlloc']

    # get SQL text from GCS into a variable
    sqlcode = getSQLText(sqlfile_loc)

    # execute query on BigQuery
    try:
        query_job = bq_client.query(sqlcode)
        results = query_job.result() # wait for query job to complete
        print("Query Result: ")
        #Print result on stdout
        for row in results:
            print(list(row.items()))
        print(" ")
    except Exception as err:
        print(err)


def read_config_file(config_bucket_id, config_file_name, query_exec_target):
    # Create Storage Client
    storage_client = storage.Client()

    # Read config file from GCS bucket and convert to Dict.
    bucket = storage_client.bucket(config_bucket_id)
    filedata = bucket.blob(config_file_name)

    json_data = filedata.download_as_string()
    cdata = json.loads(json_data)

    level = 1
    execlist = []
    if query_exec_target == 'BQ':
        for p in cdata[query_exec_target]:
            execlist.append(
                {'level': level, 'parentstep': p['execstep'], 'execstep': p['execstep'], 'sqlloc': p['sqlloc']})
            level = level+1
            try:
                for q in p['dependencies']:
                    execlist.append({'level': level, 'parentstep': p['execstep'], 'execstep': q['execstep'], 'sqlloc': q['sqlloc']})
            except KeyError:
                pass
    elif query_exec_target == 'SPARK':
        for p in cdata[query_exec_target]:
            execlist.append({'level': level, 'parentstep': p['execstep'], 'execstep': p['execstep'],
                             'sqlloc': p['sqlloc'], 'dataloc': p['dataloc'],
                             'schemaloc': p['schemaloc'], 'targetschema': p['targetschema'],
                             'targettable': p['targettable']})
            level = level+1
            try:
                for q in p['dependencies']:
                    execlist.append({'level': level, 'parentstep': p['execstep'], 'execstep': q['execstep'],
                                     'sqlloc': q['sqlloc'], 'dataloc': q['dataloc'],
                                     'schemaloc': q['schemaloc'], 'targetschema': q['targetschema'],
                                     'targettable': q['targettable']})
            except KeyError:
                pass

    return execlist


def limit(method, timeout):
    """ Convert a Java method to asynchronous call with a specified timeout. """
    def f(*args):
        @jpype.JImplements(java.util.concurrent.Callable)
        class g:
            @jpype.JOverride
            def call(self):
                return method(*args)
        future = java.util.concurrent.FutureTask(g())
        java.util.Thread(future).start()
        try:
            timeunit = java.util.concurrent.TimeUnit.MILLISECONDS
            return future.get(int(timeout*1000), timeunit)
        except java.util.concurrent.TimeoutException as ex:
            future.cancel(True)
        raise RuntimeError("canceled", ex)
        return f


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    # Collect command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", type=str, help="location of config file in GCS", required=True)
    parser.add_argument("-t", type=str, help="query execution target - BQ|SPARK", required=True)
    parser.add_argument("-m", type=str, help="execution mode - local|cluster", required=False)
    parser.add_argument('--debug', action='store_true', help='enable debug', required=False)

    args = parser.parse_args()

    config_file = args.c
    query_exec_target = args.t
    exec_mode = args.m
    debug = args.debug

    if query_exec_target not in ['BQ', 'SPARK']:
        print("query_exec_target should be either BQ or SPARK")
        exit(1)

    # Read config file from GCS
    # Extract bucket name and file name from GCS file path
    # GCS file path gs://zetasqldemo/config/zetademoconfig.json will should be passed as "zetasqldemo"
    # and "config/zetademoconfig.json"
    # to GCS sdk call

    config_file_d = os.path.normpath(config_file).split(os.sep)
    config_bucket_id = config_file_d[1]
    config_file_name = config_file_d[2] + '/' + config_file_d[3]

    execlist = read_config_file(config_bucket_id, config_file_name,query_exec_target)

    if query_exec_target == 'BQ':
        # Create and reuse BQ Client
        bq_client = bigquery.Client()
        for i in range(0, len(execlist)):
            bq_run(bq_client, execlist[i])
    elif query_exec_target == 'SPARK':
        jpype.addClassPath('jpype_bridge/libs/*')
        if args.debug:
            jvmArgs = ["-Xmx8000M", "-Xint", "-Xdebug", "-Xnoagent",
                       "-Xrunjdwp:transport=dt_socket,server=y,address=12999,suspend=n"]
        else:
            jvmArgs = ["-Xmx8000M", "-Dspark.yarn.stagingDir=hdfs://" + socket.gethostname() + ":8020/tmp"
                       , "-Dspark.executor.extraClassPath=/home/davidsabater/guava-29.0-jre.jar", "-Dspark.driver.extraClassPath=/home/davidsabater/guava-29.0-jre.jar"]
        #               , "-Dspark.executor.userClassPathFirst=true", "-Dspark.driver.userClassPathFirst=true"]

        # Launch the JVM
        jpype.startJVM(convertStrings=False, *jvmArgs)
        # import the Java modules for BQ
        from java.lang import Exception
        import java.util
        # print(jpype.getDefaultJVMPath())
        if args.debug:
            input("pause to attach debugger")
        zsqlc = spark_zsql_context(exec_mode)
        for i in range(0, len(execlist)):
            # Run java classes
            zsqlc.spark_run(execlist[i])
        input("press any key to exit, this is to keep Spark UI alive")
        # Shutdown the jvm
        #jpype.shutdownJVM()

# Example Call
# query_exec.py -c gs://zetasqldemo/config/zetademoconfig.json -t BQ
