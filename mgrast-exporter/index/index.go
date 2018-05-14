package index

import (
	"encoding/json"
	"github.com/MG-RAST/MG-RAST-exporter/mgrast-exporter/file"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var INDEX_FILE = "export.index"

var (
	ExportIndex = NewExportIndex()
)

func NewExportIndex() *Indexes {
	return &Indexes{}
}

type Indexes []*Index

type Index struct {
	Project     string   `json:"p"`
	Metagenomes []string `json:"m"`
	StartFile   int      `json:"sf"`
	StartRecord int      `json:"sr"`
	EndFile     int      `json:"ef"`
	EndRecord   int      `json:"er"`
	Completed   bool     `json:"c"`
}

type PrevInfo struct {
	M string
	F int
	R int
}

func (i *Index) Init(p string, m string, f int, r int) {
	i.Project = p
	i.Metagenomes = append(i.Metagenomes, m)
	i.StartFile = f
	i.StartRecord = r
}

func (i *Index) Update(mg string) {
	if mg == "" {
		return
	}
	add := true
	for _, m := range i.Metagenomes {
		if m == mg {
			add = false
		}
	}
	if add {
		i.Metagenomes = append(i.Metagenomes, mg)
	}
}

func (i *Index) CurrentMG() string {
	return i.Metagenomes[len(i.Metagenomes)-1]
}

func (i *Index) Finalize(mg string, f int, r int) {
	i.Update(mg)
	i.EndFile = f
	i.EndRecord = r
	i.Completed = true
}

func (idx *Indexes) Init(filepath string) (err error) {
	if _, oerr := os.Stat(filepath); oerr == nil {
		var jsonstream []byte
		jsonstream, err = ioutil.ReadFile(filepath)
		if err != nil {
			return
		}
		err = json.Unmarshal(jsonstream, idx)
	}
	return
}

func (idx *Indexes) Contains(p string) bool {
	for _, i := range *idx {
		if i.Project == p {
			return true
		}
	}
	return false
}

func (idx *Indexes) Get() *Index {
	return (*idx)[len(*idx)-1]
}

func (idx *Indexes) Save(filepath string) (err error) {
	if _, oerr := os.Stat(filepath); oerr == nil {
		// delete if exists
		os.Remove(filepath)
	}
	var jsonstream []byte
	jsonstream, err = json.Marshal(idx)
	if err != nil {
		return
	}
	err = ioutil.WriteFile(filepath, jsonstream, 0666)
	return
}

func (idx *Indexes) RemoveFromEnd(n int) {
	*idx = (*idx)[:len(*idx)-n]
}

func (idx *Indexes) Add(i *Index) {
	*idx = append(*idx, i)
}

func (idx *Indexes) Len() int {
	return len(*idx)
}

// get unique ordered list
func (idx *Indexes) FileList(from int) (files []int) {
	seen := make(map[int]bool)
	for n, i := range *idx {
		if n < from {
			continue
		}
		for n := i.StartFile; n <= i.EndFile; n++ {
			if _, ok := seen[n]; !ok {
				files = append(files, n)
				seen[n] = true
			}
		}
	}
	return
}

// validate
func (idx *Indexes) IsComplete() (ok bool, badProject string, badPos int) {
	for n, i := range *idx {
		if !i.Completed {
			badProject = i.Project
			badPos = n + 1
			return
		}
	}
	ok = true
	return
}

func (idx *Indexes) HasFiles(files []string) (ok bool, missing []string) {
	ok = true
	ifiles := idx.FileList(0)
	for _, f := range files {
		has := false
		fnum, _ := strconv.Atoi(strings.Split(filepath.Base(f), ".")[0])
		for _, ifile := range ifiles {
			if fnum == ifile {
				has = true
			}
		}
		if !has {
			ok = false
			missing = append(missing, f)
		}
	}
	return
}

func (idx *Indexes) IndexAllFiles(files []string) (err error) {
	prev := new(PrevInfo)
	currIndex := new(Index)
	idx.Add(currIndex)
	for _, f := range files {
		prev, currIndex, err = idx.indexFile(f, prev, currIndex)
		if err != nil {
			return
		}
	}
	// finalize last one
	currIndex.Finalize(prev.M, prev.F, prev.R)
	return
}

func (idx *Indexes) indexFile(f string, prev *PrevInfo, currIndex *Index) (next *PrevInfo, nextIndex *Index, err error) {
	var fnum int
	fnum, err = strconv.Atoi(strings.Split(filepath.Base(f), ".")[0])
	if err != nil {
		return
	}
	rnum := 0

	fh, err := os.Open(f)
	if err != nil {
		return
	}
	defer fh.Close()
	fr := file.NewReader(fh, true)

	eof := false
	for {
		rnum += 1
		seq, er := fr.Read()
		if er != nil {
			if er != io.EOF {
				err = er
				return
			}
			eof = true
		}
		if eof && (seq == nil) {
			break
		}
		var proj string
		var mg string
		proj, mg, err = file.ParseHeader(string(seq.ID[:]))
		if err != nil {
			return
		}
		if currIndex.Project == proj {
			// update existing
			if mg != prev.M {
				currIndex.Update(mg)
			}
		} else if currIndex.Project == "" {
			// empty index, start it
			currIndex.Init(proj, mg, fnum, rnum)
		} else {
			// new project, finsh current index and make new
			currIndex.Finalize(prev.M, prev.F, prev.R)
			nextIndex = new(Index)
			idx.Add(nextIndex)
			nextIndex.Init(proj, mg, fnum, rnum)
		}
		next.M = mg
		next.F = fnum
		next.R = rnum
		if nextIndex == nil {
			nextIndex = currIndex
		}
		if eof {
			break
		}
	}
	return
}
