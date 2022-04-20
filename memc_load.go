package main

import (
	"flag"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
)

func processLog(device_memc map[string]string, pattern string, dry bool) {

	results := map[string]int{
		"processed": 0,
		"errors":    0,
	}

	log.Info("Processing...")
	files, err := getFiles(pattern)
	if err != nil {
		log.Error("Failed to get files to parse")
		os.Exit(1)
	}
	if len(files) == 0 {
		log.Info("Everything is up-to-date. Nothing to parse")
		os.Exit(0)
	}

	results["processed"] = len(files)
	log.Info(
		fmt.Sprintf(
			"Total processed: %d; Total errors: %d",
			results["processed"],
			results["errors"],
		))
	log.Info("Exiting")
}

// Iterate over <dir> in given pattern and return all files
// matching <pattern>:
// 	Usage:
// 		files, err = getFiles("/misc/tarz/.*.tar.gz")
func getFiles(pattern string) ([]fs.FileInfo, error) {
	s := strings.Split(pattern, "/")
	file_pattern := s[len(s)-1]
	if file_pattern == "" {
		log.Warning(
			"File pattern is a bare directory without actual pattern: ", pattern)
		log.Warning("All files in dir would be processed")
	}
	dir := strings.Join(s[:len(s)-1], "/")

	var matched_files []fs.FileInfo
	var validFile = regexp.MustCompile(file_pattern)

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Error(err)
		return matched_files, err
	}
	for _, f := range files {
		// list only files matching pattern, check out any folders
		if validFile.MatchString(f.Name()) &&
			!f.IsDir() &&
			strings.HasPrefix(f.Name(), ".") {
			matched_files = append(matched_files, f)
		}
	}

	return matched_files, nil
}

// Initiate logging settings using given <settings> map
func setLogging(settings map[string]string) {

	// settings log output
	logfile := settings["logfile"]
	var file *os.File
	var err error
	if logfile != "stdout" {
		file, err = os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		file = os.Stdout
	}
	log.SetOutput(file)

	// setting log level
	loglevel := settings["loglevel"]
	switch loglevel {
	case "info":
		log.SetLevel(log.InfoLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	}
}

func getDefaultPattern() string {
	default_dict := os.Getenv("default_dict")
	if default_dict == "" {
		default_dict = "."
	}
	default_pattern := fmt.Sprintf("%s/.*.tsv.gz", default_dict)
	return default_pattern
}

func main() {

	logfile := flag.String("logfile", "stdout", "Logfile name")
	loglevel := flag.String("loglevel", "info", "For debug level use debug")
	idfa := flag.String("idfa", "127.0.0.1:33013", "idfa address")
	gaid := flag.String("gaid", "127.0.0.1:33014", "gaid address")
	adid := flag.String("adid", "127.0.0.1:33015", "adid address")
	dvid := flag.String("dvid", "127.0.0.1:33016", "dvid address")
	pattern := flag.String("pattern", getDefaultPattern(), "example: <dir>/*.tsv.gz")
	dry := flag.Bool("dry", false, "turn in dryrun (without actual memcaching)")
	flag.Parse()

	// set logging
	logset := make(map[string]string)
	logset["logfile"] = *logfile
	logset["loglevel"] = *loglevel
	setLogging(logset)

	// pack all device_memc addresses into a map
	device_memc := make(map[string]string)
	device_memc["idfa"] = *idfa
	device_memc["gaid"] = *gaid
	device_memc["adid"] = *adid
	device_memc["dvid"] = *dvid

	log.WithFields(log.Fields{
		"loglevel": *loglevel,
		"logfile":  *logfile,
		"idfa":     *idfa,
		"gaid":     *gaid,
		"adid":     *adid,
		"dvid":     *dvid,
		"dry":      *dry,
		"pattern":  *pattern,
	}).Info("Starting the application")

	processLog(device_memc, *pattern, *dry)

}
