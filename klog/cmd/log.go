package cmd

// use it as :
// ./main log -f /tmp/kafka-logs-0/topic1-0/00000000000000000000.log
// or
// ./main log -r /tmp/kafka-logs-0 -t topic1
// or even using the default value for kafka broker on my machine
// ./main log -t topic1

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
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
	--offset someInt

When retrieving the logs of a topic, root = broker_name + kafka_logs_folder so that every log related to this topic
in the target broker are parsed (usually 1 lead and other replicas).
When passing a filename (broker_name + complete path) only the logs of this filename are extracted.

Note: either provide the topic/root or the file but not both options.

Examples:
1. Extract all logs from a file FILENAME
	klog log -f FILENAME
2. Extract the logs associated to a topic TOPIC
	klog log -t TOPIC
3. Same but with kafka logs in folder FOLDER
	klog log -t TOPIC -r FOLDER
4. Extract only the messages roughly corresponding to the offset OFFSET
	klog log -t TOPIC -o OFFSET`,

	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("log called with option topic", topic, "and root", root, "and filename", filename,
			"and offset", offset)
		var buf []byte
		var files []string
		var err error
		t := fmt.Sprintf("%s-*", topic)
		if topic != "" {
			buf, err = findInPath(t, root)
			must(err)
			if offset >= 0 {
				files, err = findFiles(buf, ".index")
			} else {
				files, err = findFiles(buf, ".log")
			}
			must(err)
			if len(files) == 0 {
				return
			}
		} else if filename != "" {
			files = append(files, filename)
		}
		if offset >= 0 {
			for _, f := range files {
				readLogFromIndex(f, uint32(offset))
			}
		} else {
			data := make([]byte, 0)
			for _, f := range files {
				buf, err = readLog(f)
				must(err)
				data = append(data, buf...)
			}
			ds := bytesToString(data)
			for _, d := range ds {
				fmt.Println(d)
			}
		}
	},
}

var filename string
var topic string
var root string
var offset int32

func init() {
	rootCmd.AddCommand(logCmd)

	logCmd.Flags().StringVarP(&filename, "file", "f", "", "Kafka log filename")
	logCmd.Flags().StringVarP(&topic, "topic", "t", "", "topic name")
	logCmd.Flags().StringVarP(&root, "root", "r", "/tmp/kafka-logs-0", "root folder name of kafka server logs")
	logCmd.Flags().Int32VarP(&offset, "offset", "o", -1, "offset of the message to retrieve in the log")
}

// Find the files with the given extension from the given paths
func findFiles(paths []byte, extension string) ([]string, error) {
	rs := make([]string, 0)
	ps := bytesToString(paths)
	for _, p := range ps {
		// Check that file exists and is readable
		if _, err := os.Stat(p); err != nil {
			continue
		}
		bs, err := findInPath("*"+extension, p)
		if err != nil {
			return nil, err
		}
		rs = append(rs, bytesToString(bs)...)
	}
	return rs, nil
}

// []byte => []string
func bytesToString(bs []byte) []string {
	res := make([]string, 0)
	ps := strings.Split(string(bs), "\n")
	for _, p := range ps {
		if strings.Trim(p, " ") != "" {
			res = append(res, p)
		}
	}
	return res
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

type offsetPos struct {
	offset, pos uint32
}

// Get the positions begin and end bytes to read to get some messages with the given offset
func getPosFromIndex(filename string, wantedOffset uint32) (offsetPos, offsetPos, error) {
	file, err := os.Open(filename)
	if err != nil {
		return offsetPos{}, offsetPos{}, err
	}
	defer file.Close()

	newops := offsetPos{0, 0}
	opsm1 := newops
	buf := make([]byte, 4*2)
	for {
		n, err := file.Read(buf)
		if n < 8 || err == io.EOF {
			return offsetPos{}, offsetPos{}, errors.New("EOF : offset not found in the interval")
		}
		offset := binary.BigEndian.Uint32(buf[:4])
		if offset == 0 {
			return offsetPos{}, offsetPos{}, errors.New("Offset = 0 : not found in the interval")
		}
		pos := binary.BigEndian.Uint32(buf[4:n])
		opsm1 = newops
		newops = offsetPos{offset: offset, pos: pos}
		if offset > wantedOffset {
			return opsm1, newops, nil
		}
	}
}

// read the .index file,
// extract the offset min/max with its position min/max to lookup in .log file
// locate the .log file and extract the bytes between position min and max
// create a temp file with these bytes
// call readlog on this temp file
func readLogFromIndex(filename string, wantedOffset uint32) error {
	from, to, err := getPosFromIndex(filename, wantedOffset)
	if err != nil {
		return err
	}

	filelog, err := os.Open(replaceExtension(filename, ".log"))
	if err != nil {
		return err
	}
	defer filelog.Close()

	_, err = filelog.Seek(int64(from.pos), 0)
	if err != nil {
		return err
	}

	size := to.pos - from.pos
	buff := make([]byte, size)
	n, err := filelog.Read(buff)
	if n < int(size) {
		return errors.New("Read smaller message than expected")
	}

	filetmp := filepath.Join(os.TempDir(), filepath.Base(filelog.Name()))
	defer os.Remove(filetmp)
	err = ioutil.WriteFile(filetmp, buff, 0755)
	if err != nil {
		return err
	}

	data, err := readLog(filetmp)
	ds := bytesToString(data)
	for _, d := range ds {
		fmt.Println(d)
	}
	return nil
}

// replace the extension of the filename with the given extension
func replaceExtension(filename, extension string) string {
	return strings.TrimSuffix(filename, filepath.Ext(filename)) + extension
}
