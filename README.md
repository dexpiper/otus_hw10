# Usage

## Run:

1) Clone repo with:

`$ git clone <repo address>`

2) cd to dir with repo

`$ go mod tidy`

`$ go run memc_load [--logfile] [--loglevel] [--idfa] [--gaid] [--adid] [--dvid] [--pattern] [--dry] [--err_rate] [--workers] [--rename]`


* --log: store logs in <i>filename</i>. By default logs go to stdout.
* --loglevel: use "debug" to set level to "debug" (default: info)
* --pattern: dir and name pattern to find .gz files to process
* --idfa, --gaid, --adid, --dvid: server addresses to store device memc's
* --dry: dryrun without writing into memchached, logs would go to file or stdout
* --err_rate: defining acceptable error rate (float). Default is 0.01
* --workers: number of workers. Default is 5.
* --rename: set to false to forbit .gz files renaming after processing (by default successfully processed files renamed with a dot before file name)



## Example run

`$ go run memc_load --pattern="/home/alex/Downloads/*.tsv.gz" --loglevel=info --err_rate=0.03 --rename=false --dry`


```
INFO[0000] Starting the application                      adid="127.0.0.1:33015" dry=true dvid="127.0.0.1:33016" error_rate=0.03 gaid="127.0.0.1:33014" idfa="127.0.0.1:33013" logfile=stdout loglevel=info pattern="/home/alex/Downloads/*.tsv.gz" rename=false workers=5

INFO[0000] Starting...

INFO[0000] Found total 3 files in /home/alex/Downloads

INFO[0000] File 20170929000000.tsv.gz sheduled for processing

INFO[0000] File 20170929000100.tsv.gz sheduled for processing

INFO[0000] File 20170929000200.tsv.gz sheduled for processing

INFO[0000] All 3 files are sheduled.

INFO[0000] Please wait for fileprocessors done the reading...

INFO[0117] File 20170929000200.tsv.gz is read to the end and closed

INFO[0117] File 20170929000000.tsv.gz is read to the end and closed

INFO[0123] File 20170929000100.tsv.gz is read to the end and closed

INFO[0123] Closing jobs chan

INFO[0123] Waiting for consumers to shut down

INFO[0123] Waiting for analyzer to analyze the results

INFO[0123] Successful load. Total processed: 10269498; Total errors: 0

INFO[0123] Exiting

INFO[0123] Execution time: 2m3.500413335s

```

