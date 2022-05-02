package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

type AppsInstalled struct {
	dev_type string
	dev_id   string
	lat      float64
	lon      float64
	apps     []uint64
}

func processLog(device_memc map[string]string, pattern string, dry bool) {

	results := map[string]int{
		"processed": 0,
		"errors":    0,
	}

	log.Info("Processing...")
	files, dir, err := getFiles(pattern)
	if err != nil {
		log.Error("Failed to get files to parse")
		os.Exit(1)
	}
	if len(files) == 0 {
		log.Info("Everything is up-to-date. Nothing to parse")
		os.Exit(0)
	}

	for _, f := range files {
		fd, err := os.Open(fmt.Sprintf("%s/%s", dir, f.Name()))
		if err != nil {
			log.Error(fmt.Sprintf("Cannot open file %s. Error: %s", f.Name(), err))
		}
		scanner := bufio.NewScanner(fd)

		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			appsinstalled, err := parseAppinstalled(line)
			if err != nil {
				log.Debug(err)
				results["errors"]++
			} else {
				address := device_memc[appsinstalled.dev_type]
				insertAppsinstalled(appsinstalled, address, dry)
				results["processed"]++
			}
		}

		fd.Close()
	}

	// results["processed"] = len(files)
	log.Info(
		fmt.Sprintf(
			"Total processed: %d; Total errors: %d",
			results["processed"],
			results["errors"],
		))
	log.Info("Exiting")
}

// Writing to memcache (or to log) parsed apps
func insertAppsinstalled(appsinstalled AppsInstalled, address string, dry bool) {

	uapps := &UserApps{}
	uapps.Lat = appsinstalled.lat
	uapps.Lon = appsinstalled.lon
	uapps.Apps = appsinstalled.apps

	out, err := proto.Marshal(uapps)
	if err != nil {
		log.Error("Failed to encode user apps:", err)
	}

	key := fmt.Sprintf("%s:%s", appsinstalled.dev_type, appsinstalled.dev_id)

	if dry {
		var apps []string
		for _, i := range uapps.GetApps() {
			apps = append(apps, strconv.FormatUint(i, 10))
		}
		log.Debug(
			fmt.Sprintf("%s - %s -> %s", address, key, strings.Join(apps, " ")))
	} else {
		// TODO: memc real writer
		log.Debug(
			fmt.Sprintf("Writing to memc %s key %s this string: %s", address, key, out))
	}
}

// Parse one line of logs file and return struct AppsInstalled
func parseAppinstalled(line string) (AppsInstalled, error) {
	line = strings.TrimSpace(line)
	prts := strings.Split(line, "\t")
	if len(prts) != 5 {
		log.Info("Cannot parse line: %s", line)
		return AppsInstalled{}, fmt.Errorf("Cannot parse line: %s", line)
	}
	dev_type, dev_id := prts[0], prts[1]
	lat, errlat := strconv.ParseFloat(prts[2], 2)
	lon, errlon := strconv.ParseFloat(prts[3], 2)
	if errlat != nil || errlon != nil {
		log.Info("Cannot parse geocoords: %s", line)
		return AppsInstalled{}, fmt.Errorf("Cannot parse geocoords: %s", line)
	}
	raw_apps := strings.Split(prts[4], ",")
	var apps []uint64
	for _, raw_app := range raw_apps {
		raw_app := strings.TrimSpace(raw_app)
		app, err := strconv.ParseUint(raw_app, 0, 64)
		if err == nil {
			apps = append(apps, app)
		}
	}
	if len(apps) == 0 {
		log.Info("Cannot parse apps: %s", line)
		return AppsInstalled{}, fmt.Errorf("Cannot parse apps: %s", line)
	}
	return AppsInstalled{dev_type, dev_id, lat, lon, apps}, nil
}

// Iterate over <dir> in given pattern and return all files
// matching <pattern>:
// 	Usage:
// 		files, err = getFiles("/misc/tarz/.*.tar.gz")
func getFiles(pattern string) ([]fs.FileInfo, string, error) {
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
		return matched_files, dir, err
	}
	for _, f := range files {
		// list only files matching pattern, check out any folders
		if validFile.MatchString(f.Name()) &&
			!f.IsDir() &&
			!strings.HasPrefix(f.Name(), ".") {
			matched_files = append(matched_files, f)
		}
	}

	return matched_files, dir, nil
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
	default_dir := os.Getenv("default_dir")
	if default_dir == "" {
		default_dir = "."
	}
	default_pattern := fmt.Sprintf("%s/.*.tsv.gz", default_dir)
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
