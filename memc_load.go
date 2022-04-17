package main

import (
	"flag"
	"os"

	log "github.com/sirupsen/logrus"
)

func set_logging(settings map[string]string) {

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

func main() {
	logfile := flag.String("logfile", "stdout", "Logfile name")
	loglevel := flag.String("loglevel", "info", "For debug level use debug")
	flag.Parse()
	settings := make(map[string]string)
	settings["logfile"] = *logfile
	settings["loglevel"] = *loglevel
	set_logging(settings)
	log.WithFields(log.Fields{
		"loglevel": *loglevel,
		"logfile":  *logfile,
	}).Info("Starting the application")
	log.Debug("Something noteworthy happened")
	log.Error("Something went wrong")
}
