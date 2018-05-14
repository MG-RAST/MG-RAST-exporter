package exporter

import (
	"bytes"
	"fmt"
	"github.com/MG-RAST/MG-RAST-exporter/mgrast-exporter/file"
	"github.com/MG-RAST/MG-RAST-exporter/mgrast-exporter/index"
	"github.com/MG-RAST/go-shock-client"
	"github.com/MG-RAST/golib/httpclient"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

var RESOURCE = "node"
var PAGE_SIZE = 50

type Record struct {
	R []byte
	P string
	M string
}

type Exporter struct {
	SC    shock.ShockClient
	RC    *httpclient.RestClient
	Path  string
	Stage string
	Size  int64
	Debug bool
	Query url.Values
}

func NewExporter(dir string, stage string, size int64, debug bool) *Exporter {
	return &Exporter{
		SC:    shock.ShockClient{},
		RC:    &httpclient.RestClient{},
		Path:  dir,
		Stage: stage,
		Size:  size,
		Debug: debug,
		Query: url.Values{},
	}
}

func (e *Exporter) Init(project string, shockhost string) (err error) {
	e.Query.Set("type", "metagenome")
	e.Query.Set("stage_name", e.Stage)
	e.Query.Set("direction", "asc")
	e.Query.Set("order", "project_id")
	if project != "" {
		e.Query.Set("project_id", project)
	}
	e.SC.Host = shockhost
	e.SC.Debug = e.Debug
	e.RC, err = e.SC.QueryPaginated(RESOURCE, e.Query, PAGE_SIZE, 0)
	return
}

func (e *Exporter) Index(force bool) (err error) {
	ifile := IndexFile(e.Path)
	if _, oerr := os.Stat(ifile); oerr == nil {
		if !force {
			err = fmt.Errorf("index file %s already exists, use --force to overwrite", ifile)
			return
		} else {
			os.Remove(ifile)
		}
	}
	err = index.ExportIndex.Init(ifile)
	if err != nil {
		return
	}
	files := e.exportFiles()
	if len(files) > 0 {
		err = index.ExportIndex.IndexAllFiles(files)
		if err != nil {
			return
		}
	}
	err = index.ExportIndex.Save(ifile)
	return
}

func (e *Exporter) Clean() (err error) {
	// retrieve index
	ifile := IndexFile(e.Path)
	err = index.ExportIndex.Init(ifile)
	if err != nil {
		return
	}

	// remove non-indexed
	var extra []string
	var indexFiles []string

	for _, fint := range index.ExportIndex.FileList(0) {
		indexFiles = append(indexFiles, FileFromInt(fint, e.Path))
	}
	for _, f := range e.exportFiles() {
		pos := SliceIndex(len(indexFiles), func(i int) bool { return indexFiles[i] == f })
		if pos == -1 {
			extra = append(extra, f)
		}
	}
	for _, f := range extra {
		fmt.Fprintf(os.Stdout, fmt.Sprintf("removing non-indexed file: %s\n", f))
		os.Remove(f)
	}

	// truncate last index end file to correct length
	lastIndex := index.ExportIndex.Get()
	err = e.truncateExportFile(lastIndex.EndFile, lastIndex.EndRecord)
	return
}

func (e *Exporter) Remove(count int) (err error) {
	// retrieve index
	ifile := IndexFile(e.Path)
	err = index.ExportIndex.Init(ifile)
	if err != nil {
		return
	}
	if index.ExportIndex.Len() == 0 {
		fmt.Fprintf(os.Stdout, "index is empty, nothing to remove\n")
		// do nothing
	} else if index.ExportIndex.Len() <= count {
		fmt.Fprintf(os.Stdout, "removing all indexes / export files\n")
		// delete all indexed export files and index
		for _, fint := range index.ExportIndex.FileList(0) {
			fname := FileFromInt(fint, e.Path)
			os.Remove(fname)
		}
		os.Remove(ifile)
	} else {
		fmt.Fprintf(os.Stdout, fmt.Sprintf("removing last %d index(es) / file(s)\n", count))
		newLastPos := index.ExportIndex.Len() - count - 1
		newLastIndex := (*index.ExportIndex)[newLastPos]
		filesRemove := index.ExportIndex.FileList(newLastPos + 1)
		lastFile := newLastIndex.EndFile
		if lastFile == 0 || newLastIndex.EndRecord == 0 || !newLastIndex.Completed {
			err = fmt.Errorf("export set in bad state, new last index (position=%d, project=%s) is incomplete", newLastPos, newLastIndex.Project)
			return
		}
		lastFilePos := SliceIndex(len(filesRemove), func(i int) bool { return filesRemove[i] == lastFile })
		if lastFilePos != -1 {
			filesRemove = append(filesRemove[:lastFilePos], filesRemove[lastFilePos+1:]...)
		}
		// delete all but new last export files
		for _, fint := range filesRemove {
			fname := FileFromInt(fint, e.Path)
			os.Remove(fname)
		}
		// delete indexes from end
		index.ExportIndex.RemoveFromEnd(count)
		index.ExportIndex.Save(ifile)

		err = e.truncateExportFile(lastFile, newLastIndex.EndRecord)
		if err != nil {
			return
		}
	}
	return
}

func (e *Exporter) Export() (err error) {
	// retrieve index
	ifile := IndexFile(e.Path)
	err = index.ExportIndex.Init(ifile)
	if err != nil {
		return
	}
	// validate index
	if ok, proj, pos := index.ExportIndex.IsComplete(); !ok {
		err = fmt.Errorf("export set in bad state: project %s (%d out of %d exports) is incomplete", proj, pos, index.ExportIndex.Len())
		return
	}
	if ok, missing := DirHasFiles(index.ExportIndex.FileList(0), e.Path); !ok {
		err = fmt.Errorf("export set in bad state: directory missing files\n\t%s\n", strings.Join(missing, "\n\t"))
		return
	}
	if ok, missing := index.ExportIndex.HasFiles(e.exportFiles()); !ok {
		err = fmt.Errorf("export set in bad state: index missing files\n\t%s\n", strings.Join(missing, "\n\t"))
		return
	}

	// start writer after index is good
	// exporter doesn't touch index after this, only writer
	RecordWriter.Init(e.Path, e.Size, e.Debug)
	go RecordWriter.WriterHandle(false, 0, 0)

	// export per metagenome
	prevProject := ""
	for {
		item, er := e.RC.Next()
		// non eof error
		if er != nil {
			if er != io.EOF {
				err = er
				return
			}
			break
		}

		node := item.Data.(map[string]interface{})
		attr, aok := node["attributes"].(map[string]interface{})
		nodeID, nok := node["id"].(string)
		projID, pok := attr["project_id"].(string)
		mgID, mok := attr["id"].(string)
		if !(aok && nok && pok && mok) {
			err = fmt.Errorf("Invalid shock node: %+v", node)
			return
		}

		// skip missing IDs
		if (projID == "") || (mgID == "") {
			continue
		}
		// skip first project if in index
		if (prevProject == "") && index.ExportIndex.Contains(projID) {
			fmt.Fprintf(os.Stdout, fmt.Sprintf("skipping: project=%s, metagenome=%s, node=%s\n", projID, mgID, nodeID))
			continue
		}
		// new project, not first
		if (prevProject != "") && (prevProject != projID) {
			// skip projects already exported
			if index.ExportIndex.Contains(projID) {
				fmt.Fprintf(os.Stdout, fmt.Sprintf("skipping: project=%s, metagenome=%s, node=%s\n", projID, mgID, nodeID))
				continue
			}
			// let writer know to finalize index for previous, then wait till done
			RecordWriter.RecBuffer <- nil
			_ = <-RecordWriter.Done
		}
		prevProject = projID

		fmt.Fprintf(os.Stdout, fmt.Sprintf("exporting: project=%s, metagenome=%s, node=%s\n", projID, mgID, nodeID))
		downloadUrl := fmt.Sprintf("%s/%s/%s?download", e.SC.Host, RESOURCE, nodeID)
		if e.Debug {
			fmt.Fprintf(os.Stdout, downloadUrl+"\n")
		}

		shockStream, serr := shock.FetchShockStream(downloadUrl, "")
		if serr != nil {
			err = serr
			return
		}

		sr := file.NewReader(shockStream, false)
		eof := false
		rnum := 0

		// process per record, push in buffer
		for {
			rnum += 1
			seq, er := sr.Read()
			if er != nil {
				if er != io.EOF {
					err = er
					return
				}
				eof = true
			}
			if seq == nil {
				if eof {
					break
				} else {
					continue
				}
			}

			// get record, send to buffer
			newHead := bytes.Join([][]byte{[]byte(projID), []byte(mgID), seq.ID}, []byte{'|'})
			seq.ID = newHead
			record := &Record{
				R: seq.Record(),
				P: projID,
				M: mgID,
			}

			RecordWriter.RecBuffer <- record

			if e.Debug && (rnum%100 == 0) {
				fmt.Fprintf(os.Stdout, ".")
			}
			if eof {
				break
			}
		} // done with file
		if e.Debug {
			fmt.Fprintf(os.Stdout, fmt.Sprintf("\nmetagenome %s done exporting\n", mgID))
		}
	} // done with metagenome list
	// let writer know to finalize index for last projet, then wait till done
	RecordWriter.RecBuffer <- nil
	_ = <-RecordWriter.Done

	// 2nd nil in a row means all done exporting, writer can end
	RecordWriter.RecBuffer <- nil
	_ = <-RecordWriter.Done
	return
}

func (e *Exporter) truncateExportFile(fint int, newRec int) (err error) {
	filePath := FileFromInt(fint, e.Path)
	tempFile := filePath + ".temp"
	err = os.Rename(filePath, tempFile)
	if err != nil {
		return
	}
	fmt.Fprintf(os.Stdout, fmt.Sprintf("truncating file: %s\n", filePath))

	// start writehandle
	RecordWriter.Init(e.Path, e.Size, e.Debug)
	go RecordWriter.WriterHandle(true, fint, 1)

	// open last file
	tempHandle, terr := os.Open(tempFile)
	if terr != nil {
		err = terr
		return
	}
	defer tempHandle.Close()
	tempReader := file.NewReader(tempHandle, true)

	// copy last records
	for rnum := 1; rnum <= newRec; rnum++ {
		seq, er := tempReader.Read()
		if er != nil {
			if er == io.EOF {
				err = fmt.Errorf("file %s in bad state, reached EOF before last record read: %d of %d records", filePath, rnum, newRec)
			} else {
				err = er
			}
			return
		}
		if seq == nil {
			err = fmt.Errorf("file %s in bad state, invalid record found: %d of %d records", filePath, rnum, newRec)
			return
		}
		record := &Record{
			R: seq.Record(),
			P: "",
			M: "",
		}
		RecordWriter.RecBuffer <- record
	}
	RecordWriter.RecBuffer <- nil
	RecordWriter.RecBuffer <- nil
	_ = <-RecordWriter.Done
	// delete old
	os.Remove(tempFile)
	return
}

func (e *Exporter) indexFile() string {
	return filepath.Join(e.Path, index.INDEX_FILE)
}

func (e *Exporter) exportFiles() (files []string) {
	files, _ = filepath.Glob(filepath.Join(e.Path, fmt.Sprintf("*%s", file.FILE_SUFFIX)))
	return
}

func DirHasFiles(files []int, path string) (ok bool, missing []string) {
	ok = true
	for _, i := range files {
		f := FileFromInt(i, path)
		if _, oerr := os.Stat(f); oerr != nil {
			ok = false
			missing = append(missing, f)
		}
	}
	return
}

func IndexFile(path string) string {
	return filepath.Join(path, index.INDEX_FILE)
}

func FileFromInt(num int, path string) string {
	return filepath.Join(path, fmt.Sprintf("%d%s", num, file.FILE_SUFFIX))
}

func SliceIndex(limit int, predicate func(i int) bool) int {
	for i := 0; i < limit; i++ {
		if predicate(i) {
			return i
		}
	}
	return -1
}
