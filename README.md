Parse the kafka topic logs

## A. klog

Parse a kafka data log file(s) and extract some features according to the options:

	--file filename
	--topic topicname
	--root foldername
	--offset someInt

When retrieving the logs of a topic (*root = broker_name + kafka_logs_folder*) so that every log related to this topic in the target broker are parsed (usually 1 lead and other replicas).

When passing a filename (*broker_name + complete path*) only the logs of this filename are extracted.

Note: either provide the topic/root or the file but not both options.

Examples:
1. Extract all logs from a file FILENAME

	klog log -f FILENAME
2. Extract the logs associated to a topic TOPIC

	klog log -t TOPIC
3. Same but with kafka logs in folder FOLDER

	klog log -t TOPIC -r FOLDER
4. Extract only the messages roughly corresponding to the offset OFFSET

	klog log -t TOPIC -o OFFSET