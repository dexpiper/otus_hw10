package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

type StartOptions struct {
	Device_memc map[string]string
	Pattern     *string
	Dry         *bool
	Err_rate    *float64
	Workers     *int
}

type AppsInstalled struct {
	Dev_type string
	Dev_id   string
	Lat      float64
	Lon      float64
	Apps     []uint64
}

type Job struct {
	Appsinstalled AppsInstalled
	Memc          *memcache.Client
	Address       string
	Dry           bool
	Err           error
}

var wg sync.WaitGroup

// consume Job from jobs chan, do the Job,
// than write any errors into resulting <err> chan
func consume(jobs <-chan *Job, errs chan<- *Job) {
	defer wg.Done()
	for job := range jobs {
		err := insertAppsinstalled(
			job.Appsinstalled, job.Memc, job.Address, job.Dry)
		job.Err = err
		errs <- job
	}
}

// calculating job results: taking ready Job from chan
// and fill in results_map
func analyze(jobs <-chan *Job, result chan<- map[string]int) {
	results_map := map[string]int{
		"processed": 0,
		"errors":    0,
	}
	for job := range jobs {

		if job.Err != nil {
			results_map["errors"]++
		} else {
			results_map["processed"]++
		}
	}
	// single result
	result <- results_map
}

func processLog(opts StartOptions) {

	log.Info("Starting...")
	files, dir, err := getFiles(*opts.Pattern)
	if err != nil {
		log.Error("Failed to get files to parse")
		os.Exit(1)
	}
	if len(files) == 0 {
		log.Info("Everything is up-to-date. Nothing to parse")
		os.Exit(0)
	}

	log.Info(fmt.Sprintf("Found total %v files in %s", len(files), dir))
	memc_pool := getMemcPool(opts.Device_memc)

	for _, f := range files {

		// initializing chans
		jobs := make(chan *Job, 100)        // Buffered channel
		errs := make(chan *Job, 100)        // Buffered channel
		result := make(chan map[string]int) // Unbuffered channel

		// starting consumer goroutine
		for i := 0; i < *opts.Workers; i++ {
			wg.Add(1)
			go consume(jobs, errs)
		}

		// starting analyzer goroutine
		go analyze(errs, result)

		// opening and un-gunzipping file
		fd, err := os.Open(fmt.Sprintf("%s/%s", dir, f.Name()))
		if err != nil {
			log.Error(fmt.Sprintf("Cannot open file %s. Error: %s", f.Name(), err))
		}

		fz, err := gzip.NewReader(fd)
		if err != nil {
			log.Error(fmt.Sprintf("Cannot gunzip file %s. Error: %s", f.Name(), err))
		}

		// iterating over each string in file
		scanner := bufio.NewScanner(fz)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			appsinstalled, err := parseAppinstalled(line)
			if err != nil {
				log.Debug(err)
				errs <- &Job{Err: err}
			} else {
				address := opts.Device_memc[appsinstalled.Dev_type]
				memc := memc_pool[appsinstalled.Dev_type]

				// scheduling Job
				jobs <- &Job{
					Appsinstalled: appsinstalled,
					Memc:          memc,
					Address:       address,
					Dry:           *opts.Dry,
					Err:           nil,
				}
			}
		}

		close(jobs)
		log.Info(fmt.Sprintf("Processing file %s ...", f.Name()))
		wg.Wait()

		close(errs)
		log.Debug(fmt.Sprintf(
			"All jobs for %s are scheduled, waiting for analyzer", f.Name()))
		results := <-result
		close(result)

		log.Debug(fmt.Sprintf("All results for %s are analyzed", f.Name()))

		// making final calculations
		var err_rate float64
		errors := results["errors"]
		processed := results["processed"]
		if processed != 0 {
			err_rate = float64(errors) / float64(processed)
		} else {
			err_rate = 1.0
		}
		if err_rate >= *opts.Err_rate {
			log.Error(fmt.Sprintf(
				"High error rate (%.2f > %v). Failed load", err_rate, *opts.Err_rate))
		} else {
			log.Info(
				fmt.Sprintf(
					"Successful load %s. Total processed: %d; Total errors: %d",
					f.Name(),
					processed,
					errors,
				))
		}
		fz.Close()
		fd.Close()
	}

	// TODO: renaming file
	log.Info("Exiting")
}

// Writing to memcache (or to log) parsed apps
func insertAppsinstalled(
	appsinstalled AppsInstalled,
	memc *memcache.Client,
	address string, dry bool) error {

	uapps := &UserApps{}
	uapps.Lat = appsinstalled.Lat
	uapps.Lon = appsinstalled.Lon
	uapps.Apps = appsinstalled.Apps

	out, err := proto.Marshal(uapps)
	if err != nil {
		log.Error("Failed to encode user apps:", err)
	}

	key := fmt.Sprintf("%s:%s", appsinstalled.Dev_type, appsinstalled.Dev_id)

	if dry {

		// pretending that we write into memc
		var apps []string
		for _, i := range uapps.GetApps() {
			apps = append(apps, strconv.FormatUint(i, 10))
		}
		log.Debug(
			fmt.Sprintf("%s - %s -> %s", address, key, strings.Join(apps, " ")))
		return nil
	} else {

		// actually writing to memc with given memc Client
		// TODO: retry
		err := memc.Set(&memcache.Item{
			Key:   key,
			Value: out,
		})
		if err != nil {
			log.Error(
				fmt.Sprintf(
					"Cannot write to memc %s key %s. Error: %s", address, key, err))
			return memcache.ErrServerError
		} else {
			log.Debug(
				fmt.Sprintf("Writing to memc server %s: key %s", address, key))
			return nil
		}
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

// get a map of memcached clients for each device type
func getMemcPool(device_memc map[string]string) map[string]*memcache.Client {
	pool := make(map[string]*memcache.Client)
	for device_name, addr := range device_memc {
		cl := memcache.New(addr)
		pool[device_name] = cl
	}
	return pool
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
	err_rate := flag.Float64(
		"err_rate", 0.01, "Use float64 for defining acceptable error rate")
	workers := flag.Int("workers", 5, "Number of workers (5 by default)")
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
		"loglevel":   *loglevel,
		"logfile":    *logfile,
		"idfa":       *idfa,
		"gaid":       *gaid,
		"adid":       *adid,
		"dvid":       *dvid,
		"dry":        *dry,
		"pattern":    *pattern,
		"error_rate": *err_rate,
		"workers":    *workers,
	}).Info("Starting the application")

	processLog(StartOptions{device_memc, pattern, dry, err_rate, workers})

}
