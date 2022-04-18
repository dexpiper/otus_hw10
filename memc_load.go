package main

import (
	"flag"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus" // logging
)

func processLog(device_memc map[string]string, pattern string, dry bool) {
	// main logic
	results := map[string]int{
		"processed": 0,
		"errors":    0,
	}
	log.Info("Processing")
	log.Info(
		fmt.Sprintf(
			"Total processed: %d; Total errors: %d",
			results["processed"],
			results["errors"],
		))
	log.Info("Exiting")
}

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
	default_pattern := fmt.Sprintf("%s/*.tsv.gz", default_dict)
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

	logset := make(map[string]string)
	logset["logfile"] = *logfile
	logset["loglevel"] = *loglevel
	setLogging(logset)

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
