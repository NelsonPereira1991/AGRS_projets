__author__ = 'pedrorochagoncalves'
from fabric.api import *
from threading import Thread
from requests.exceptions import ConnectionError
import requests
import json
import time
import sys
import os
import configs
import csv
import systemMethods
from select import select

# // ==================================================================================================
# // ========================================== AUX METHODS ===========================================
# // ==================================================================================================


# stdout flush thread
def stdflushworker():
    """thread worker function"""
    print "running stdflushworker " + str(configs.STDOUTFLUSHTHREADON)
    sys.stdout.flush()
    while configs.STDOUTFLUSHTHREADON == 1:
        sys.stdout.flush()
        time.sleep(0.5)
    return




# @TODO: To use with the Machine Learning Algorithm - Future Work
# Method to get completion time for finished jobs and write to a CSV file
def storeCompletionTimeOfJobs(path):
    """
    Method to get completion time for finished jobs and write to a CSV file.
    :param path: path to CSV file.
    """


# @TODO: To use with the Machine Learning Algorithm - Future Work
# Method to write to CSV file
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



# // ==================================================================================================
# // ===================================== CLUSTER MONITOR METHODS ====================================
# // ==================================================================================================


# Method to get the average progress of the running jobs in the cluster
def getAvgProgressRunningJobs():
    """
    Method to get the average progress of the running jobs in the cluster.

    This method queries the Resource Manager trough the REST API and gets the progress
    (in percent) of the running jobs. It then calculates an average progress of the running jobs.
    :return: Average Progress of the running jobs
    """

    wsAddress = 'http://' + configs.LOCAL_DATA_CENTER_ADDRESS + ':8088/ws/v1/cluster/apps'

    runningJobsResponse = requests.get(wsAddress)
    jsonRunningJobsResponse = json.loads(runningJobsResponse.text)

    if jsonRunningJobsResponse['apps'] is not None:
        runningJobs = jsonRunningJobsResponse['apps']['app']
        avgProgress = 0
        numRunningJobs = 0

        for i in range(0, len(runningJobs)):

            if runningJobs[i]['state'] == 'RUNNING':
                avgProgress += runningJobs[i]['progress']
                numRunningJobs += 1

        if numRunningJobs != 0:
            avgProgress = avgProgress / numRunningJobs

        return avgProgress

    else:
        return 0


# Method to get the current number of Jobs running in the cluster
def getNumRunningJobs():
    """
    Method to get the current number of Jobs running in the cluster.

    This method gets the number of running jobs in the cluster through the REST API.
    :return: Number of running jobs
    """

    wsAddress = 'http://' + configs.LOCAL_DATA_CENTER_ADDRESS + ':8088/ws/v1/cluster/appstatistics?' \
                                                                'states=running&applicationTypes=mapreduce'
    appStatisticsResponse = requests.get(wsAddress)
    jsonAppStatisticsResponse = json.loads(appStatisticsResponse.text)

    numRunningJobs = jsonAppStatisticsResponse['appStatInfo']['statItem'][0]['count']

    return numRunningJobs


# Method to get total number of containers in the cluster
def getTotalNumOfContainers():
    """
    Method to get total number of containers in the cluster.

    This method queries the the RM to retrieve the total number of active nodes and the
    total number of RAM in the cluster. These can be used together to know how many
    containers the cluster has.
    :return: Total number of containers in the cluster.
    """

    wsAddress = 'http://' + configs.LOCAL_DATA_CENTER_ADDRESS + ':8088/ws/v1/cluster/metrics'

    metricsResponse = requests.get(wsAddress)
    jsonMetricsResponse = json.loads(metricsResponse.text)

    numTotalNodes = jsonMetricsResponse['clusterMetrics']['activeNodes']
    totalRAM = jsonMetricsResponse['clusterMetrics']['totalMB']  # in MB

    ramPerNode = totalRAM / numTotalNodes
    containersPerNode = ramPerNode / configs.RAM_PER_CONTAINER

    return containersPerNode * numTotalNodes


# Method to get the input file size in HDFS
def getInputFileSizeFromHDFS(inputFile):
    """
    Method to get the input file size from the HDFS

    This method queries HDFS to get the job input file size. This will be used by the load balancer
    to estimate how many containers are required for the job.
    :return: input file size
    """

    # Prepare SSH properties
    env.host_string = configs.LOCAL_DATA_CENTER_ADDRESS
    env.user = 'ubuntu'
    env.key_filename = configs.KEY_PATH

    command = 'hadoop fs -du pcapFiles/' + inputFile

    with shell_env(
            PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/opt/hive/bin:/opt/hadoop/bin"):
        with cd('pcapJobs'):
            result = sudo(command, user='hadoop')
            inputFileSize, inputFileName = result.split(" ", 1)

    return int(inputFileSize) / configs.ONE_MEGABYTE


# Method to get the input file size in FS
def getInputFileSizeFromFS(inputFile):
    """
    Method to get the input file size from the FS

    This method queries the FS to get the job input file size. This will be used by the load balancer
    to estimate how many containers are required for the job.
    :return: input file size
    """

#    command = 'stat -f \'%z\' ' + configs.PCAP_REMOTE_DIRECTORY_PATH + inputFile

#    with settings(warn_only=True):
#        inputFileSize = local(command, capture=True)


    statinfo = os.stat(configs.PCAP_REMOTE_DIRECTORY_PATH + inputFile)
    inputFileSize = statinfo.st_size

    return int(inputFileSize) / configs.ONE_MEGABYTE


# Method to get the local data center's current resource usage
def getCurrentResourceUsage():
    """
    Method to return the local data center's current resource usage.

    This method uses the Resource Manager's REST API to query the RM for the cluster's
    resource usage.
    :return: array containing the current resource usage info
    """

    wsAddress = 'http://' + configs.LOCAL_DATA_CENTER_ADDRESS + ':8088/ws/v1/cluster/scheduler'

    try:

        # Get Cluster's scheduler resource state @TODO:COLOCAR TRY
        schedulerResponse = requests.get(wsAddress)
        jsonSchedulerResponse = json.loads(schedulerResponse.text)
        currentUsedCapacity = jsonSchedulerResponse['scheduler']['schedulerInfo']['usedCapacity']
        numContainers = jsonSchedulerResponse['scheduler']['schedulerInfo']['queues']['queue'][0]['numContainers']
        numRunningApps = jsonSchedulerResponse['scheduler']['schedulerInfo']['queues']['queue'][0]['numActiveApplications']
        numPendingApps = jsonSchedulerResponse['scheduler']['schedulerInfo']['queues']['queue'][0]['numPendingApplications']
        usedMemory = jsonSchedulerResponse['scheduler']['schedulerInfo']['queues']['queue'][0]['resourcesUsed']['memory']
        usedVCores = jsonSchedulerResponse['scheduler']['schedulerInfo']['queues']['queue'][0]['resourcesUsed']['vCores']

        return (currentUsedCapacity, numContainers, numRunningApps, numPendingApps, usedMemory, usedVCores)

    except TypeError:
        print " --> Resource Manager REST API not responding correctly <-- "
        return None

    except ConnectionError:
        print " --> Resource Manager REST API is offline or unreachable <-- "
        return None



# Method to calculate average resource usage and determine if the local data center can process incoming jobs
def calculateResourceUsageAverage():
    """
    Method to calculate the average resource usage.

    This method calculates average resource usage by calling getCurrentResourceUsage periodically
    and saving the data in a global variable.
    """

    # Get current resource usage values
    currentResourceUsage = getCurrentResourceUsage()

    # If REST API timed out or did not respond, do not try to update resource usage
    if currentResourceUsage is None:
        return

    currentUsedCapacity, numContainers, numRunningApps, numPendingApps, usedMemory, usedVCores = currentResourceUsage

    # Reset Average after 5 seconds
    currentTime = int(round(time.time()))

    if currentTime - configs.startTime >= 15:
        # print "RESET A MEDIA!"
        configs.numberReadings = 0
        configs.sumCapacityReadings = 0
        configs.sumContainerReadings = 0
        configs.startTime = int(round(time.time()))

    configs.sumCapacityReadings += currentUsedCapacity
    configs.sumContainerReadings += numContainers
    configs.numberReadings += 1
    configs.avgLocalDataCenterCapacity = configs.sumCapacityReadings / configs.numberReadings
    configs.avgUsedContainers = configs.sumContainerReadings / configs.numberReadings

    # print 'Used Capacity -  ' + str(configs.avgLocalDataCenterCapacity)
    # print 'Used Containers - ' + str(configs.avgUsedContainers)


# // ==================================================================================================
# // ==================================================================================================
# // ==================================================================================================


# Method to print INFO message
def printInfo(message):
    """
    Method to print custom important INFO messages

    """

    header = "=" * 70
    print header
    print header
    print "\t %s" % message
    print header
    print header + "\n"

    sys.stdout.flush()


# Method to print Cluster Status
def printClusterStatus():
    """
    Method to print Cluster Status.

    This method prints the cluster status in a clean way.
    """

    header = "=" * 60
    print header
    print header

    while configs.printStatusRunThread:
        print "USED CLUSTER CAPACITY = " + "{:05.2f}".format(configs.avgLocalDataCenterCapacity) + \
              " || USED CONTAINERS = " + str(configs.avgUsedContainers) + '\r',

    sys.stdout.flush()

# Method to print the current resource capacity
def printCurrentResourceCapacity():
    """
    Method to print the local data center's current resource capacity.

    This method uses the Resource Manager's REST API to query the RM for the cluster's
    resource capacity usage.
    """

    wsAddress = 'http://' + configs.LOCAL_DATA_CENTER_ADDRESS + ':8088/ws/v1/cluster/scheduler'

    # Get Cluster resource state
    serverResponse = requests.get(wsAddress)
    jsonResponse = json.loads(serverResponse.text)

    # Get current used capacity (in percent)
    currentUsedCapacity = jsonResponse['scheduler']['schedulerInfo']['usedCapacity']
    currentUsedCapacityMessage = "INFO: Current FREE Cluster Capacity: " + str(100 - currentUsedCapacity) + "%"
    printInfo(currentUsedCapacityMessage)


# Method to query user for what jobs to run
def mainMenu():
    """
    Method to query user for what jobs to run.

    This method will return the amount of jobs and
    the type of job to run.
    """

    # Ask the user for which job to launch
    print "What job type do you want to run?"
    print "1) PcapTotalStats"
    print "2) PcapTotalFlowStats"
    print "3) PcapRate"
    print "4) PcapCountUp"
    print "5) Let me check how she's doin'!!"
    print "6) None. Can I go now?"

    try:

        while True:

            #jobType = int(raw_input("> "))


            timeout = 5
            rlist, _, _ = select([sys.stdin], [], [], timeout)

            if rlist:
                jobType = sys.stdin.readline()
                jobType = int(jobType)
                print("You chose: %d\n" % jobType)

            else:
                print "No input. Moving on..."
                continue


            if jobType in (1, 2, 3, 4, 6):
                break

            # Print Cluster Status
            elif jobType == 5:

                configs.printStatusRunThread = True
                os.system('cls' if os.name == 'nt' else 'clear')
                print "Press Q and enter to stop monitoring"
                printClusterStatusThread = Thread(target=printClusterStatus, name="PrintClusterStatus")
                printClusterStatusThread.start()

                while True:
                    stop = raw_input()

                    if stop == 'Q':
                        configs.printStatusRunThread = False
                        os.system('cls' if os.name == 'nt' else 'clear')
                        break

            else:
                print "The specified job type was not found."

        # Stop Program Execution
        if jobType == 6:
            configs.scheduledWorkRunThread = False
            configs.scheduleSystemProcessRunThread = False
            sys.exit(0)

        inputFile = raw_input(
            "Specify the input file to process [traffic.pcap, equinix.pcap,6v5GB.pcap]: ")  # @TODO: Ir buscar os ficheiros

        numberJobs = int(raw_input("How many jobs do you want to launch? "))

        return (jobType, numberJobs, inputFile)

    except KeyboardInterrupt:
        sys.exit(0)


# Method to create a thread to execute a specified function
def runThreaded(function):
    """
    Method to create a thread to execute a specified function.

    This method will create a thread to execute the passed function
    :param function: The function to execute in a new thread.
    """

    jobThread = Thread(target=function)
    jobThread.start()


# DEBUG MODE
def debugMode():
    """
    Method used for debugging.
    """

    while configs.scheduleSystemProcessRunThread:

        # Prompt the user for the jobs
        jobType, numberJobs, inputFile = mainMenu()

        # Hand over to the Load Balancer
        systemMethods.loadBalancer(jobType, inputFile, numberJobs)


# Method to exit the application
def exitApp():
    """
    Method used to exit the Application safely.
    :return:
    """

    printInfo("Goodbye my lord")
    configs.scheduledWorkRunThread = False
    configs.scheduleSystemProcessRunThread = False
    configs.STDOUTFLUSHTHREADON = 0
    sys.exit(0)


# Method to clean up HDFS
def cleanUpHDFS():
    """
    Method used to clean up HDFS.
    :return:
    """

    # Prepare SSH properties
    env.host_string = configs.LOCAL_DATA_CENTER_ADDRESS
    env.user = 'ubuntu'
    env.key_filename = configs.KEY_PATH

    command = 'hadoop fs -rmr pcapFiles/dump_*'

    with shell_env(
            PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/opt/hive/bin:/opt/hadoop/bin"):
        with cd('pcapJobs'):
            sudo(command, user='hadoop')


# Method to write HDFS uploads to CSV
def writeHDFSUploadStatsToCSV(pcapFileName, pcapFileSize, cloudAddress,  uploadDuration="N/A", starttime=0, endtime=0):
    """
    Method to write statistics about the upload to HDFS.

    :param pcapFileName: Name of the PCAP file
    :param pcapFileSize: Size of the PCAP file
    :param uploadDuration: Duration of the upload to the HDFS (public or private)
    :param cloudAddress: Where the file was uploaded to
    """

    timeList = list()

    if cloudAddress == configs.LOCAL_DATA_CENTER_ADDRESS:
        timeList.append("LOCAL DATA CENTER")
    else:
        timeList.append("PUBLIC CLOUD")

    timeList.append(pcapFileName)
    timeList.append(pcapFileSize)
    timeList.append(uploadDuration)
    timeList.append(starttime)
    timeList.append(endtime)
    configs.hdfsUploadTimeList.append(timeList)
    writeToCSV(configs.hdfsUploadTimeList, configs.UPLOAD_TIME_CSV_PATH)

