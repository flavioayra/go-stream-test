package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

/*
gwa-afl-prod
gwa-cvc-prod
gwa-sv-prod
orders-cvc-prod
precificador-afl-prod
precificador-cvc-prod
precificador-sv-prod
rfapi-afl-prod
rfapi-cvc-prod
rfapi-sv-prod
search-afl-prod
search-cvc-prod
search-int-prod
search-lojas-prod
search-lvl-prod
search-ra-prod
search-sv-prod


amazon-cvc-prod-logs/cvc-prod-logs/gwa-cvc-prod/2018/03/16

*/

var (
	stream = flag.String("stream", "logs", "logs")
	region = flag.String("region", "sa-east-1", "sa-east-1")
)

var bucket = "amazon-cvc-prod-logs"

func main() {

	flag.Parse()

	s := session.New(&aws.Config{Region: aws.String(*region)})
	kc := kinesis.New(s)

	streamName := aws.String(*stream)

	logFile, err := os.Open("/Users/flv/Projects/go/src/github.com/flavioayra/go-stream-test/data/gwaereo.log-T00h19m06Z-ip-10-229-6-231-i-08a46857106860285")
	if err != nil {
		panic(err)
	}
	defer logFile.Close()
	r := bufio.NewReader(logFile)

	errFile, err := os.Create("/Users/flv/Projects/go/src/github.com/flavioayra/go-stream-test/data/error.log")
	if err != nil {
		panic(err)
	}
	defer errFile.Close()
	p := bufio.NewWriter(errFile)
	for {
		line, err := r.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		_, err = kc.PutRecord(&kinesis.PutRecordInput{
			Data:         line,
			StreamName:   streamName,
			PartitionKey: aws.String("search-cvc-prod"),
		})
		if err != nil {
			_, err := p.Write(line)
			if err != nil {
				fmt.Println("Error writing", err)
			}
		}

	}
	p.Flush()

}
