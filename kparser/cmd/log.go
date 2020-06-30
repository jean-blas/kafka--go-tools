package cmd

// use it as :
// ./main log -f /tmp/kafka-logs-0/topic1-0/00000000000000000000.log
// or
// ./main log -r /tmp/kafka-logs-0 -t topic1
// or even using the default value for kafka broker on my machine
// ./main log -t topic1

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/cobra"
)

// logCmd represents the log command
var logCmd = &cobra.Command{
	Use:   "log",
	Short: "Open a kafka log file and parse it",
	Long: `Parse a kafka data log file(s) and extract some features according to the options:
	--file filename
	--topic topicname
	--root foldername

When retrieving the logs of a topic, root = broker_name + kafka_logs_folder so that every log related to this topic
in the target broker are parsed (usually 1 lead and other replicas).
When passing a filename (broker_name + complete path) only the logs of this filename are extracted.

Note: either provide the topic/root or the file but not both options.`,

	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("log called with option topic", topic, "and root", root, "and filename", filename)
		var buf []byte
		var logs []string
		var err error
		t := fmt.Sprintf("%s-*", topic)
		if topic != "" {
			buf, err = findInPath(t, root)
			must(err)
			logs, err = findLogs(buf)
			must(err)
			if len(logs) == 0 {
				return
			}
		} else if filename != "" {
			logs = append(logs, filename)
		}
		data := make([]byte, 0)
		for _, log := range logs {
			buf, err = readLog(log)
			must(err)
			data = append(data, buf...)
		}
		ds := bytesToString(data)
		for _, d := range ds {
			fmt.Println(d)
		}
	},
}

var filename string
var topic string
var root string

func init() {
	rootCmd.AddCommand(logCmd)

	logCmd.Flags().StringVarP(&filename, "file", "f", "", "Kafka log filename")
	logCmd.Flags().StringVarP(&topic, "topic", "t", "", "topic name")
	logCmd.Flags().StringVarP(&root, "root", "r", "/tmp/kafka-logs-0", "root folder name of kafka server logs")
}

// Find the logs from the given paths
func findLogs(paths []byte) ([]string, error) {
	rs := make([]string, 0)
	ps := bytesToString(paths)
	for _, p := range ps {
		// Check that file exists and is readable
		if _, err := os.Stat(p); err != nil {
			continue
		}
		bs, err := findInPath("*.log", p)
		if err != nil {
			return nil, err
		}
		rs = append(rs, bytesToString(bs)...)
	}
	return rs, nil
}

// []byte => []string
func bytesToString(bs []byte) []string {
	rs := make([]string, 0)
	ps := strings.Split(string(bs), "\n")
	for _, p := range ps {
		if strings.Trim(p, " ") != "" {
			rs = append(rs, p)
		}
	}
	return rs
}

// Find the pattern in the root path recursively
// Display the error if any
func findInPath(pattern, root string) ([]byte, error) {
	var err error
	// Check that root exists and is readable
	if _, err = os.Stat(root); err != nil {
		return nil, err
	}
	// Run the unix find command
	command := exec.Command("find", root, "-name", pattern)
	var stdout, stderr io.ReadCloser
	if stdout, err = command.StdoutPipe(); err != nil {
		return nil, err
	}
	if stderr, err = command.StderrPipe(); err != nil {
		return nil, err
	}
	if err := command.Start(); err != nil {
		return nil, err
	}
	var buf []byte
	if buf, err = ioutil.ReadAll(stdout); err != nil {
		return nil, err
	}
	if slurp, _ := ioutil.ReadAll(stderr); slurp != nil {
		fmt.Printf("%s\n", slurp)
	}
	_ = command.Wait()
	return buf, nil
}

// Convenient function to display an error and exit(1)
func must(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Read the kafka log file and return the lines as []byte
func readLog(filename string) ([]byte, error) {
	var err error
	// Check that file exists and is readable
	if _, err = os.Stat(filename); err != nil {
		return nil, err
	}
	// Read the lines with kafka tool
	command := exec.Command("kafka-run-class.sh", "kafka.tools.DumpLogSegments", "--print-data-log", "--files", filename)
	var stdout io.ReadCloser
	if stdout, err = command.StdoutPipe(); err != nil {
		return nil, err
	}
	if err := command.Start(); err != nil {
		return nil, err
	}
	var buf []byte
	if buf, err = ioutil.ReadAll(stdout); err != nil {
		return nil, err
	}
	if err := command.Wait(); err != nil {
		return nil, err
	}
	return buf, nil
}
