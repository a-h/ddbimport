package linereader

import (
	"bufio"
	"io"
)

// New creates a new LineReader. A reader that keeps track of the line positions within the source.
func New(r io.Reader, startLine, startOffset int64, onNewLine func(line, offset int64)) *LineReader {
	return &LineReader{
		r:         bufio.NewReader(r),
		Line:      startLine,
		Offset:    startOffset,
		onNewLine: onNewLine,
	}
}

// LineReader keeps track of how many lines have been read.
type LineReader struct {
	r                     *bufio.Reader
	remainder             []byte
	bytesSinceLastNewLine int
	eof                   bool
	Line                  int64
	Offset                int64
	onNewLine             func(line, pos int64)
	d                     []byte
}

func (lr *LineReader) Read(p []byte) (n int, err error) {
	if p == nil {
		p = make([]byte, 4096)
	}
	if len(lr.remainder) > 0 {
		lr.d = lr.remainder
	} else {
		lr.d, err = lr.r.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				return
			}
			lr.eof = true
			err = nil
		}
	}
	n = len(lr.d)
	if len(p) < n {
		n = len(p)
	}
	copy(p, lr.d[0:n])
	lr.remainder = lr.d[n:]
	lr.bytesSinceLastNewLine += n
	if len(lr.remainder) == 0 && lr.bytesSinceLastNewLine != 0 {
		lr.Line++
		lr.Offset += int64(lr.bytesSinceLastNewLine)
		if lr.onNewLine != nil {
			lr.onNewLine(lr.Line, lr.Offset)
		}
		lr.bytesSinceLastNewLine = 0
	}
	if lr.eof {
		err = io.EOF
	}
	return
}
