# https://192.168.109.9/rmorla/cloudbursting-software/blob/master/plotting.py

#from fabric.api import *
import time
#import schedule
#import methods
import configs
import json
#import requests
import math
import csv
import urllib2


# // ==================================================================================================
# // ====================================== MAP TASK STATS COLLECTOR ==================================
# // ==================================================================================================

def writeToCSV(data, path):
    """
    Method to write to CSV file.

    :param data: values to write.
    :param path: path to CSV file.
    """

    with open(path, "wb") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        for line in data:
            writer.writerow(line)

# Method to retrieve map task stats from HDFS and store them on a CSV file
#def mapTaskTimeCollector(mainJobId, year, month, day, jobIdFrom, jobIdTo):
def mapTaskTimeCollector():
    """
    Method to retrieve map task stats from HDFS and store them on a CSV file

    This method queries the HDFS to retrieve the log file for the specified job range, calculates the
    completion time of each map task and stores them in a CSV file.
    Example of the location of a log file:
    /tmp/hadoop-yarn/staging/history/done/2015/03/31/000000/job_1427124456889_0133-1427800042976-hadoop-
    PcapTotalStats_gen-1427800051691-2-2-SUCCEEDED-default-1427800033500.jhist
    """

    # Set job id range
    configs.jobIdCounter = int(1448994181336)
    configs.analyzedJobIdCounter = int(1635456456)


    '''
    # If the next job to analyze is the next job to be submitted, then that means that the system is up
    # to date.
    if configs.analyzedJobIdCounter == configs.jobIdCounter + 1:
        print '--- Logs are up to date ---'
        return

    # Build command to query HDFS
    command = 'hadoop fs -du -s /tmp/hadoop-yarn/staging/history/done/'

    # Get current date
    date = year + "/" + month + "/" + day
    command = command + date + '/000000/job_' + mainJobId + '_'

    if 100 <= configs.analyzedJobIdCounter <= 999:
        command += '0' + str(configs.analyzedJobIdCounter)

    elif 10 <= configs.analyzedJobIdCounter <= 99:
        command += '00' + str(configs.analyzedJobIdCounter)

    elif 0 <= configs.analyzedJobIdCounter <= 9:
        command += '000' + str(configs.analyzedJobIdCounter)

    else:
        command += str(configs.analyzedJobIdCounter)

    command += '-*'
    # Prepare SSH properties
    env.host_string = configs.LOCAL_DATA_CENTER_ADDRESS
    env.user = 'ubuntu'
    env.key_filename = configs.KEY_PATH

    with settings(warn_only=True):
        with shell_env(
                PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/opt/hive/bin:/opt/hadoop/bin"):
            result = sudo(command, user='hadoop')

    try:
        outputLogSize, outputLogName = result.split("  ", 1)

    except ValueError:
        print '--- Log file for job ' + str(configs.analyzedJobIdCounter) + ' is not yet ready ---'
        return

    ws = 'http://' + configs.LOCAL_DATA_CENTER_ADDRESS + ':50070/webhdfs/v1' + outputLogName + \
         '?op=OPEN&user.name=hadoop'
    '''
    fh = open("job.json", "r")
    #logResponse = requests.get(ws)
    logResponse = fh.read()
    logResponse = logResponse.split("\n")

    fh.close()

    # Clear the map tasks list for the new log and append the job id
    configs.mapTasks = list()
    jobIdList = list()
    jobIdList.append("Job number: " + str(configs.analyzedJobIdCounter))
    configs.mapTasks.append(jobIdList)

    for i in range(len(logResponse)):

        mapStartList = list()

        if logResponse[i] == '' or logResponse[i] == 'Avro-Json':
            continue

        else:
            jsonLogResponse = json.loads(logResponse[i])
            #print jsonLogResponse

#{"type":"AM_STARTED","event":{"org.apache.hadoop.mapreduce.jobhistory.AMStarted":{"applicationAttemptId":"appattempt_1434909112123_0058_000001","startTime":1435015351386,"containerId":"container_1434909112123_0058_01_000001","nodeManagerHost":"fournodecluster-cb-worker-4-agg-004.novalocal","nodeManagerPort":45799,"nodeManagerHttpPort":8042}}}

            if jsonLogResponse['type'] == 'AM_STARTED':
                am_host = jsonLogResponse['event']['org.apache.hadoop.mapreduce.jobhistory.AMStarted']['nodeManagerHost']


            if jsonLogResponse['type'] == 'MAP_ATTEMPT_STARTED':
                mapTaskId = jsonLogResponse['event']['org.apache.hadoop.mapreduce.jobhistory.TaskAttemptStarted']['attemptId']
                mapTaskStartTime = jsonLogResponse['event']['org.apache.hadoop.mapreduce.jobhistory.TaskAttemptStarted']['startTime']
                mapLocality = jsonLogResponse['event']['org.apache.hadoop.mapreduce.jobhistory.TaskAttemptStarted']['locality']

                mapStartList.append(mapTaskId)
                mapStartList.append(mapTaskStartTime)
                mapStartList.append(mapLocality)
                configs.mapTasks.append(mapStartList)

            elif jsonLogResponse['type'] == 'MAP_ATTEMPT_FINISHED':
                mapFinishTaskId = jsonLogResponse['event']['org.apache.hadoop.mapreduce.jobhistory.MapAttemptFinished']['attemptId']
                mapTaskFinishTime = jsonLogResponse['event']['org.apache.hadoop.mapreduce.jobhistory.MapAttemptFinished']['finishTime']
                hostname = jsonLogResponse['event']['org.apache.hadoop.mapreduce.jobhistory.MapAttemptFinished']['hostname']
                state = jsonLogResponse['event']['org.apache.hadoop.mapreduce.jobhistory.MapAttemptFinished']['state']
                state_split = state.split(' ')
                print state_split
                filename = state_split[4]
                #block_pos_and_size = state_split[6]
                #block_pos_and_size_split = block_pos_and_size.split('+')
                #block_size = block_pos_and_size_split[1]

                # Search for the corresponding map task in the map task list and append the finish time
                for j in range(len(configs.mapTasks)):

                    mapEntry = configs.mapTasks[j]

                    if mapEntry[0] == mapFinishTaskId:
                        mapTaskStartTime = mapEntry[1]
                        mapEntry.append(mapTaskFinishTime)
                        mapEntry.append(mapTaskFinishTime - mapTaskStartTime)
                        mapEntry.append(hostname)
                        mapEntry.append(filename)
                        #mapEntry.append(block_size)

    toremove = list()
    for ix in range(len(configs.mapTasks)):
        mapEntry = configs.mapTasks[ix]
        if len(mapEntry) <= 3:
            toremove.append(configs.mapTasks[ix])
    for it in toremove:
        configs.mapTasks.remove(it)

    # sort
    configs.mapTasks = sorted(configs.mapTasks, key=lambda maptaskrecord: maptaskrecord[0])

    # Write to CSV
    #writeToCSV(configs.mapTasks, "job-" + str(configs.analyzedJobIdCounter) + '.csv')
    writeToCSV(configs.mapTasks, "job-1.csv")
    #target = open("result.csv", 'w')
    #target.write(configs.mapTasks)
    #target.close()
    
    # Update the Analyzed Job Id Counter
    configs.analyzedJobIdCounter += 1


# // ==================================================================================================
# // ==================================================================================================
# // ==================================================================================================

mapTaskTimeCollector()

