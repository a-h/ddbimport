package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"

	"github.com/a-h/ddbimport/06-stepfns/linereader"
)

func main() {
	cpuF, err := os.Create("cpu.pprof")
	if err != nil {
		log.Fatalf("failed to start CPU profile: %v", err)
	}
	pprof.StartCPUProfile(cpuF)
	defer pprof.StopCPUProfile()

	memF, err := os.Create("memprofile.pprof")
	if err != nil {
		log.Fatalf("failed to start memory profile: %v", err)
	}

	src, err := os.Open("../data.csv")
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	lr := linereader.New(src, 0, 0, nil)
	for {
		_, err = lr.Read(nil)
		if err != nil {
			break
		}
	}
	if err != io.EOF {
		log.Fatalf("failed: %v", err)
	}
	//csvr := csv.NewReader(lr)
	//csvr.Comma = '\t'
	//for {
	//_, err := csvr.Read()
	//if err != nil && err != io.EOF {
	//log.Fatalf("error: %v", err)
	//return
	//}
	//if err == io.EOF {
	//break
	//}
	//}
	fmt.Printf("read %d lines (%d bytes)...\n", lr.Line, lr.Offset)
	pprof.WriteHeapProfile(memF)

	http.ListenAndServe("localhost:8080", nil)
}
