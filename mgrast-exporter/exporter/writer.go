package exporter

import (
	"fmt"
	"github.com/MG-RAST/MG-RAST-exporter/mgrast-exporter/file"
	"github.com/MG-RAST/MG-RAST-exporter/mgrast-exporter/index"
	"os"
)

var (
	RecordWriter = NewRecordWriter()
)

func NewRecordWriter() *RWBuffer {
	return &RWBuffer{
		RecBuffer: make(chan *Record, 1024),
		Done:      make(chan bool, 1),
	}
}

type RWBuffer struct {
	RecBuffer chan *Record
	Done      chan bool
	Path      string
	Size      int64
	Debug     bool
}

func (b *RWBuffer) Init(path string, size int64, debug bool) {
	if debug {
		b.Size = size * 1024 * 1024
	} else {
		b.Size = size * 1024 * 1024 * 1024
	}
	b.Path = path
	b.Debug = debug
}

func (b *RWBuffer) WriterHandle(simpleWrite bool, startFile int, startRec int) {
	if b.Debug {
		fmt.Fprintf(os.Stdout, "starting WriterHandle\n")
	}

	// get starting file
	if startFile == 0 || startRec == 0 {
		if index.ExportIndex.Len() == 0 {
			startFile = 1
			startRec = 1
		} else {
			startFile = (*index.ExportIndex)[index.ExportIndex.Len()-1].EndFile
			startRec = (*index.ExportIndex)[index.ExportIndex.Len()-1].EndRecord + 1
		}
	}
	fname := FileFromInt(startFile, b.Path)

	// append or create
	currFile, err := os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Fprintf(os.Stderr, fmt.Sprintf("error opening file %s: %s\n", fname, err.Error()))
		os.Exit(1)
	}
	currWrite := file.NewWriter(currFile)

	prev := new(index.PrevInfo)
	fileCount := startFile
	recCount := startRec
	projectDone := false

	ifile := IndexFile(b.Path)

	currIndex := new(index.Index)
	if !simpleWrite {
		index.ExportIndex.Add(currIndex)
	}

	for {
		rec := <-b.RecBuffer

		// end of current project, finsh current index and make new
		if rec == nil {
			if projectDone {
				// we already finished a project, 2nd nil means we are all done
				currWrite.Close()
				currFile.Close()
				if b.Debug {
					fmt.Fprintf(os.Stdout, "writer is all done\n")
				}
				b.Done <- true
				return
			}
			if b.Debug {
				fmt.Fprintf(os.Stdout, fmt.Sprintf("\nproject %s done writing\n", currIndex.Project))
			}
			projectDone = true
			if simpleWrite {
				continue
			}
			currIndex.Finalize(prev.M, prev.F, prev.R)
			index.ExportIndex.Save(ifile)

			nextIndex := new(index.Index)
			index.ExportIndex.Add(nextIndex)
			currIndex = nextIndex
			b.Done <- true
			continue
		}
		projectDone = false

		err := currWrite.Write(rec.R)
		if err != nil {
			// skip bad write
			fmt.Fprintf(os.Stderr, fmt.Sprintf("error in write: project=%s file=%d record=%d\n", fileCount, recCount))
			continue
		}
		if simpleWrite {
			continue
		}

		if b.Debug && (recCount%100 == 0) {
			fmt.Fprintf(os.Stdout, "+")
		}

		// update index
		if currIndex.Project == rec.P {
			// update existing
			if rec.M != prev.M {
				currIndex.Update(rec.M)
			}
		} else if currIndex.Project == "" {
			// empty index, start it
			currIndex.Init(rec.P, rec.M, fileCount, recCount)
		} else if currIndex.Project != rec.P {
			// we should not be in this state
			fmt.Fprintf(os.Stderr, fmt.Sprintf("error in record: project %s when expecting %s\n", rec.P, currIndex.Project))
			continue
		}
		prev.M = rec.M
		prev.F = fileCount
		prev.R = recCount

		info, _ := currFile.Stat()
		if info.Size() > b.Size {
			// need to switch to new file, reset counters
			currWrite.Close()
			currFile.Close()
			fileCount += 1
			recCount = 1
			fname = FileFromInt(fileCount, b.Path)
			currFile, err = os.OpenFile(fname, os.O_CREATE|os.O_WRONLY, 0666)
			if err != nil {
				fmt.Fprintf(os.Stderr, fmt.Sprintf("error opening file %s: %s\n", fname, err.Error()))
				os.Exit(1)
			}
			currWrite = file.NewWriter(currFile)
		} else {
			recCount += 1
		}
	}
	return
}
