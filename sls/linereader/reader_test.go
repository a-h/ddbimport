package linereader

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func BenchmarkLineReader(b *testing.B) {
	b.ReportAllocs()
	src, _ := generateLines(10, 0, 256*1024)
	for i := 0; i < b.N; i++ {
		lr := New(bytes.NewReader(src), 0, 0, nil)
		data := make([]byte, 4096)
		if _, err := lr.Read(data); err != nil && err != io.EOF {
			b.Errorf("failed to read: %v", err)
		}
		fmt.Println(lr.Line)
	}
}

func TestLineReader(t *testing.T) {
	for i := 0; i < 2; i++ {
		expectedLines := 10
		src, boundaries := generateLines(expectedLines, 0, 256*1024)

		var actualBoundaries []int64
		lr := New(bytes.NewReader(src), 0, 0, func(line, offset int64) {
			actualBoundaries = append(actualBoundaries, offset)
		})

		d := make([]byte, 4096)
		var total int64
		var n int
		var err error
		for {
			n, err = lr.Read(d)
			total += int64(n)
			if err != nil {
				break
			}
		}
		if lr.Line != int64(expectedLines) {
			t.Errorf("expected %d lines, got %d", len(boundaries), lr.Line)
		}
		if err != io.EOF {
			t.Errorf("expected EOF, got %v", err)
		}
		if diff := cmp.Diff(boundaries, actualBoundaries); diff != "" {
			t.Error(diff)
		}
	}
}

func generateLines(n, min, max int) ([]byte, []int64) {
	var buf bytes.Buffer
	var indices []int64
	var index int64
	for i := 0; i < n; i++ {
		line := generateLine(min, max)
		buf.Write(line)
		buf.WriteRune('\n')
		index += int64(len(line) + 1)
		indices = append(indices, index)
	}
	return buf.Bytes(), indices
}

const values = "abcdefghijklmonopqrstyuvwxyz01234567890"

func generateLine(min, max int) []byte {
	var buf bytes.Buffer
	length := int(rand.Int31n(int32(max+min)) - int32(min))
	for i := 0; i < length; i++ {
		r := values[rand.Int31n(int32(len(values)))]
		buf.WriteByte(r)
	}
	return buf.Bytes()
}
