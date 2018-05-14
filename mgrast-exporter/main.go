package main

import (
	"flag"
	"fmt"
	"github.com/MG-RAST/MG-RAST-exporter/mgrast-exporter/exporter"
	"net/url"
	"os"
	"strings"
)

var exportDirDefault = os.Getenv("EXPORT_DIR")
var shockUrlDefault = os.Getenv("SHOCK_URL")
var fileSizeDefault = int64(2)
var stageNameDefault = "screen"

var flags *flag.FlagSet

func usage() {
	fmt.Fprintf(os.Stdout, fmt.Sprintf("\nUsage: %s command [options]\n", os.Args[0]))
	fmt.Fprintf(
		os.Stdout,
		"\n"+
			"Commands:\n"+
			"\n"+
			"  export --directory [--project --size --stage]\n"+
			"           Export compressed files from MG-RAST object store.\n"+
			"           All or single project, from a given pipeline stage.\n"+
			"  clean  --directory\n"+
			"           Remove any files not in index list and prune last index end file.\n"+
			"           Used to cleanup after interrupted export.\n"+
			"  remove --directory [--count]\n"+
			"           Remove <count> number indexes from end of index list.\n"+
			"           Remove their files and prune last index file.\n"+
			"  index  --directory [--force]\n"+
			"           Rebuilds export index if missing.\n",
	)
	fmt.Fprintf(os.Stdout, fmt.Sprintf("\nOptions:\n\n"))
	flags.PrintDefaults()
	fmt.Fprintf(os.Stdout, fmt.Sprintf("\nEnvironment variables that can be used: EXPORT_DIR, SHOCK_URL\n\n"))
}

func main() {
	var exportDir string
	var shockUrl string
	var projectID string
	var stageName string
	var fileSize int64
	var count int
	var force bool
	var debug bool
	var help bool
	var err error

	flags = flag.NewFlagSet("name", flag.ContinueOnError)

	flags.StringVar(&exportDir, "directory", exportDirDefault, "export directory path")
	flags.StringVar(&shockUrl, "shock", shockUrlDefault, "url of Shock server")
	flags.StringVar(&projectID, "project", "", "project ID to export")
	flags.StringVar(&stageName, "stage", stageNameDefault, "pipeline stage name for export file")
	flags.Int64Var(&fileSize, "size", fileSizeDefault, "export file size in GB")
	flags.IntVar(&count, "count", 1, "number of indexes to remove, in reverse order of creation")
	flags.BoolVar(&force, "force", false, "force build index if already exists")
	flags.BoolVar(&debug, "debug", false, "print debug messages")
	flags.BoolVar(&help, "help", false, "this message")

	if len(os.Args) < 2 {
		flags.Parse(os.Args)
		usage()
		os.Exit(1)
	}
	flags.Parse(os.Args[2:])

	if os.Args[1] == "-h" || os.Args[1] == "--help" || os.Args[1] == "-help" {
		help = true
	}
	if help {
		usage()
		os.Exit(0)
	}

	if strings.HasPrefix(os.Args[1], "-") {
		fmt.Fprintf(os.Stderr, "missing command\n")
		usage()
		os.Exit(1)
	}

	if debug {
		fmt.Fprintf(os.Stdout, "running in debug mode\n")
	}
	if exportDir == "" {
		fmt.Fprintf(os.Stderr, fmt.Sprintf("export directory must be set\n"))
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, fmt.Sprintf("export dir path: %s\n", exportDir))
	os.MkdirAll(exportDir, 0777)

	exportTool := exporter.NewExporter(exportDir, stageName, fileSize, debug)
	command := os.Args[1]

	switch command {
	case "export":
		// check url
		if shockUrl == "" {
			fmt.Fprintf(os.Stderr, "shock url must be set\n")
			os.Exit(1)
		}
		shockHost, err := url.Parse(shockUrl)
		if err != nil {
			fmt.Fprintf(os.Stderr, fmt.Sprintf("shock url %s cannot be parsed: %s\n", shockUrl, err.Error()))
			os.Exit(1)
		}
		if shockHost.Scheme == "" {
			shockHost.Scheme = "http"
		}
		fmt.Fprintf(os.Stdout, fmt.Sprintf("shock host url: %s\n", shockHost.String()))
		// check project
		if projectID != "" {
			fmt.Fprintf(os.Stdout, fmt.Sprintf("exporting project: %s\n", projectID))
		} else {
			fmt.Fprintf(os.Stdout, "exporting all projects\n")
		}
		// init and run
		err = exportTool.Init(projectID, shockHost.String())
		if err != nil {
			fmt.Fprintf(os.Stderr, fmt.Sprintf("unable to initalize exporter: %s\n", shockUrl, err.Error()))
			os.Exit(1)
		}
		err = exportTool.Export()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
			os.Exit(1)
		}
		break
	case "clean":
		err = exportTool.Clean()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
			os.Exit(1)
		}
		break
	case "remove":
		err = exportTool.Remove(count)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
			os.Exit(1)
		}
		break
	case "index":
		err = exportTool.Index(force)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
			os.Exit(1)
		}
		break
	case "help":
		usage()
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, fmt.Sprintf("\"%s\" unknown command \n", command))
		usage()
		os.Exit(1)
	}
}
