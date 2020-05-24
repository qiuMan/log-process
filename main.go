package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

type iRead interface {
	Read(ch chan []byte)
}

type read struct {
	path string
}

func (r *read) Read(rc chan []byte) {
	file, err := os.Open(r.path)
	fmt.Println(err)
	if err != nil {
		log.Fatal(err)
	}

	file.Seek(0, 1)

	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			time.Sleep(time.Second * 3)
		} else if err != nil {
			fmt.Println(err)
		}

		rc <- line
	}

}

type iWrite interface {
	Write(ch chan *logMsg)
}

type write struct {
}

type logMsg struct {
	IP          string
	Path        string
	Method      string
	Status      string
	UpdateTime  string
	RequestTime string
	BytesSent   string
}

func (r *write) Write(wc chan *logMsg) {
	conn, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     "http://127.0.0.1:8086",
		Username: "admin",
		Password: "",
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(conn)

	for ch := range wc {
		if ch == nil {
			continue
		}

		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  "mydb",
			Precision: "s",
		})

		if err != nil {
			log.Fatal(err)
		}

		tags := map[string]string{"IP": ch.IP, "Path": ch.Path, "Method": ch.Method, "Status": ch.Status}
		fields := map[string]interface{}{
			"RequestTime": ch.RequestTime,
			"BytesSent":   ch.BytesSent,
		}

		pt, err := client.NewPoint("apache2_log", tags, fields, time.Now())
		if err != nil {
			log.Fatal(err)
		}
		bp.AddPoint(pt)

		if err := conn.Write(bp); err != nil {
			log.Fatal(err)
		}

		fmt.Printf("%v \n", ch)
	}
}

type process struct {
	rc    chan []byte
	wc    chan *logMsg
	read  iRead
	write iWrite
}

func (p *process) Process() {
	re := "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\""
	r := regexp.MustCompile(re)

	for ch := range p.rc {
		logStr := string(ch)
		ret := r.FindStringSubmatch(logStr)
		if len(ret) != 10 {
			continue
		}

		logMsgStru := &logMsg{
			IP:          ret[1],
			RequestTime: ret[4],
			Status:      ret[6],
			BytesSent:   ret[7],
		}

		reqSli := strings.Split(ret[5], " ")
		if len(reqSli) == 3 {
			logMsgStru.Method = reqSli[0]
			u, err := url.Parse(reqSli[1])
			if err == nil {
				logMsgStru.Path = u.Path
			}
		}

		p.wc <- logMsgStru
	}
}

func main() {
	p := &process{
		make(chan []byte),
		make(chan *logMsg),
		&read{
			path: "/home/qiuLong/apache2/log/access.log",
		},
		&write{},
	}

	go p.read.Read(p.rc)
	go p.Process()
	go p.write.Write(p.wc)

	for {

	}

}
