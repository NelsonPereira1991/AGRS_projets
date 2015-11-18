__author__ = 'pedrorochagoncalves'

import Queue

# // =======================================================================================================
# // ======================================== GLOBAL VARIABLES =============================================
# // =======================================================================================================

# //////////////////////////////////////////////////////////////////////////////////////////
# ////////////////////////////////////// CONSTANTS /////////////////////////////////////////
# //////////////////////////////////////////////////////////////////////////////////////////


global LOCAL_DATA_CENTER_ADDRESS
LOCAL_DATA_CENTER_ADDRESS = '172.17.0.94'

global USE_PUBLIC_CLOUD
USE_PUBLIC_CLOUD = False

global PUBLIC_CLOUD_ADDRESS
PUBLIC_CLOUD_ADDRESS = '172.16.3.106'

global HOME_FOLDER
HOME_FOLDER = '/home/openstack/cloudbursting-software'

global PCAP_REMOTE_DIRECTORY_PATH
PCAP_REMOTE_DIRECTORY_PATH = HOME_FOLDER + '/pcapFiles/'

global TCPDUMP_DIRECTORY_PATH
TCPDUMP_DIRECTORY_PATH = HOME_FOLDER + '/tcpDumpDir/'

global KEY_PATH
KEY_PATH = HOME_FOLDER + '/keys/key.pem'

global UPLOAD_TIME_CSV_PATH
UPLOAD_TIME_CSV_PATH = HOME_FOLDER + '/uploads/uploadTime.csv'

global JOBLOGS_DIRECTORY_PATH
JOBLOGS_DIRECTORY_PATH = HOME_FOLDER + '/jobLogs/'

global MAP_TASK_STATS_DIRECTORY_PATH
MAP_TASK_STATS_DIRECTORY_PATH = HOME_FOLDER + '/jobLogs/'

global MAX_LOCAL_NUMBER_OF_JOBS
MAX_LOCAL_NUMBER_OF_JOBS = 3

global ONE_MEGABYTE
ONE_MEGABYTE = 1048576

global HDFS_BLOCK_SIZE
HDFS_BLOCK_SIZE = 128  # In MB

global RAM_PER_CONTAINER
RAM_PER_CONTAINER = 2048  # In MB

global AVG_SINGLE_JOB_DURATION
AVG_SINGLE_JOB_DURATION = 375  # in seconds

global SIMPLE_MODE  # Activate the Load Balancer in simple mode (container utilization check only)
SIMPLE_MODE = True

global STUPID_MODE
STUPID_MODE = False

global OFFLINE_MODE  # Deactivate TCPDUMP and stop after no more captures are found in the tcpdumpdir
OFFLINE_MODE = True

global STDOUTFLUSHTHREADON # FLUSH STDOUT
STDOUTFLUSHTHREADON = 1


# //////////////////////////////////////////////////////////////////////////////////////////
# ////////////////////////////////////// OTHER VARS ////////////////////////////////////////
# //////////////////////////////////////////////////////////////////////////////////////////


# Scheduled Work Loop Flag
global scheduledWorkRunThread
scheduledWorkRunThread = True

# Scheduled System Process Loop Flag
global scheduleSystemProcessRunThread
scheduleSystemProcessRunThread = True

# Print Cluster Status Loop Flag
global printStatusRunThread
printStatusRunThread = True

# Average Cluster Capacity in use - This variable will be accessed by the load balancer to check
# if it has to burst a job or launch it locally
global avgLocalDataCenterCapacity
avgLocalDataCenterCapacity = 0

# Average Used Containers
global avgUsedContainers
avgUsedContainers = 0

# Number of readings performed
global numberReadings
numberReadings = 0

# Capacity Sum used to calculate Average
global sumCapacityReadings
sumCapacityReadings = 0

# Container Sum used to calculate Average
global sumContainerReadings
sumContainerReadings = 0

# Start time to reset counter
global startTime
startTime = 0

# Last found PCAP file
global lastFoundPcapFile
lastFoundPcapFile = 0

# List used to write upload times to CSV file
global hdfsUploadTimeList
hdfsUploadTimeList = list()

# Last job submit time
global lastJobSubmitTime
lastJobSubmitTime = 0

# Last job predicted finish time
global lastJobFinishTime
lastJobFinishTime = 0

# Last job predicted upload duration
global lastJobPredictedUploadDuration

# Last job predicted upload finish time
global lastJobPredictedUploadFinishTime
lastJobPredictedUploadFinishTime = 0

# Last job penultimate wave finish time
global lastJobPenultimateWaveFinishTime
lastJobPenultimateWaveFinishTime = 0

# Last job estimated max number of containers
global estimatedMaxNumContainers
estimatedMaxNumContainers = 0

# Number of map waves of the last submitted job
global runningJobNumWaveMaps
runningJobNumWaveMaps = 0

# Number of maps in the last wave (non-complete) of the last submitted job
global runningJobNumMapsLastWave
runningJobNumMapsLastWave = 0

# Number of files currently being uploaded to the HDFS
global numUploads
numUploads = 0

# Average Local HDFS upload speed
global avgLocalHDFSUploadSpeed
avgLocalHDFSUploadSpeed = 10  # in MB/s

# Local HDFS Upload speed sum to calculate average
global sumLocalUploadSpeedReadings
sumLocalUploadSpeedReadings = 3

# Local HDFS Upload speed number of readings
global numLocalUploadReadings
numLocalUploadReadings = 1

# Average Public HDFS upload speed
global avgPublicHDFSUploadSpeed
avgPublicHDFSUploadSpeed = 10  # in MB/s

# Public HDFS Upload speed sum to calculate average
global sumPublicUploadSpeedReadings
sumPublicUploadSpeedReadings = 3

# Public HDFS Upload speed number of readings
global numPublicUploadReadings
numPublicUploadReadings = 1

# Map Tasks list
global mapTasks
mapTasks = list()


# //////////////////////////////////////////////////////////////////////////////////////////
# ////////////////////////////////////// MACHINE LEARNING VARS /////////////////////////////
# //////////////////////////////////////////////////////////////////////////////////////////


global INPUT_VAR_JOB_DATA_PATH  # @TODO: To use with the Machine Learning Algorithm - Future Work
INPUT_VAR_JOB_DATA_PATH = '/Users/pedrorochagoncalves/Downloads/csvMachineLearning/inputVars.csv'

global OUTPUT_VAR_JOB_DATA_PATH  # @TODO: To use with the Machine Learning Algorithm - Future Work
OUTPUT_VAR_JOB_DATA_PATH = '/Users/pedrorochagoncalves/Downloads/csvMachineLearning/'

# Job ID counter (Contains the latest job ID value)
# @TODO: To use with the Machine Learning Algorithm - Future Work
global jobIdCounter
jobIdCounter = 141

# Analyzed job ID counter (contains the last analyzed job ID value)
# @TODO: To use with the Machine Learning Algorithm - Future Work
global analyzedJobIdCounter
analyzedJobIdCounter = 141

# List to detail if job needs to be skipped for the Machine Learning Algorithm
# @TODO: To use with the Machine Learning Algorithm - Future Work
global joinJobList
joinJobList = Queue.Queue()

# List used to write input values to the CSV file
# @TODO: To use with the Machine Learning Algorithm - Future Work
global inputValues
inputValues = list()

# List of inputValues used to write to the CSV file
# @TODO: To use with the Machine Learning Algorithm - Future Work
global inputValuesList
inputValuesList = list()

# List used to write output values to the CSV file
# @TODO: To use with the Machine Learning Algorithm - Future Work
global outputValues
outputValues = list()

# List of outputValues used to write to the CSV file
# @TODO: To use with the Machine Learning Algorithm - Future Work
global outputValuesList
outputValuesList = list()

# Job Completion Time Aux for the cases where jobs spawn a second job
# @TODO: To use with the Machine Learning Algorithm - Future Work
global auxJobCompletionTime
auxJobCompletionTime = None


# // ==================================================================================================
# // ==================================================================================================
# // ==================================================================================================
