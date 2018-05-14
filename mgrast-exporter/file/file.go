package file

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"strings"
)

var FILE_SUFFIX = ".fasta.gz"

type Seq struct {
	ID  []byte
	Seq []byte
}

func (s *Seq) Record() []byte {
	return append(append(append(append([]byte{'>'}, s.ID...), []byte{'\n'}...), bytes.ToUpper(s.Seq)...), []byte{'\n'}...)
}

type Reader struct {
	f io.Reader
	r *bufio.Reader
	c bool
}

func NewReader(f io.Reader, c bool) *Reader {
	return &Reader{
		f: f,
		r: nil,
		c: c,
	}
}

type Writer struct {
	f io.Writer
	w *gzip.Writer
}

func NewWriter(f io.Writer) *Writer {
	return &Writer{
		f: f,
		w: nil,
	}
}

func (self *Writer) Write(body []byte) (err error) {
	if self.w == nil {
		self.w = gzip.NewWriter(self.f)
	}
	_, err = self.w.Write(body)
	return
}

func (self *Writer) Close() {
	if self.w != nil {
		self.w.Close()
	}
}

func (self *Reader) Read() (seq *Seq, err error) {
	if self.r == nil {
		if self.c {
			gread, gerr := gzip.NewReader(self.f)
			if gerr != nil {
				err = gerr
				return
			}
			self.r = bufio.NewReader(gread)
		} else {
			self.r = bufio.NewReader(self.f)
		}
	}
	var prev, read, label, body []byte
	var eof bool
	for {
		read, err = self.r.ReadBytes('>')
		// non eof error
		if err != nil {
			if err == io.EOF {
				eof = true
			} else {
				return
			}
		}
		if len(prev) > 0 {
			read = append(prev, read...)
		}
		// only have '>'
		if len(read) == 1 {
			if eof {
				break
			} else {
				continue
			}
		}
		// found an embedded '>'
		if !bytes.Contains(read, []byte{'\n'}) {
			prev = read
			continue
		}
		// process lines
		read = bytes.TrimSpace(bytes.TrimRight(read, ">"))
		lines := bytes.Split(read, []byte{'\n'})
		label = lines[0]
		if len(lines) > 1 {
			body = bytes.Join(lines[1:], []byte{})
		}
		break
	}
	if len(label) > 0 {
		seq = &Seq{ID: label, Seq: body}
	} else {
		err = errors.New("Invalid fasta entry")
	}
	if eof {
		err = io.EOF
	}
	return
}

func ParseHeader(h string) (p string, m string, e error) {
	parts := strings.Split(h, "|")
	if len(parts) < 3 {
		e = errors.New("Invalid sequence header for index")
		return
	}
	p = parts[0]
	m = parts[1]
	if !strings.HasPrefix(p, "mgp") {
		e = errors.New("Invalid project ID in header")
		return
	}
	if !strings.HasPrefix(m, "mgm") {
		e = errors.New("Invalid metagenome ID in header")
		return
	}
	return
}
