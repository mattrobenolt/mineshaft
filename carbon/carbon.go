package carbon

import (
	"github.com/mattrobenolt/go-cyanite/metric"
	"github.com/mattrobenolt/go-cyanite/store"

	"bufio"
	"log"
	"net"
	"strconv"
)

func recv(c net.Conn) {
	var (
		scanner   *bufio.Scanner
		point     = metric.New()
		more      bool
		value     float64
		err       error
		timestamp uint64
	)
	defer c.Close()
	defer metric.Release(point)

	scanner = bufio.NewScanner(c)
	scanner.Split(bufio.ScanWords)

	for {
		if more = scanner.Scan(); !more {
			// EOF
			return
		}
		point.Path = scanner.Text()
		if more = scanner.Scan(); !more {
			log.Println("carbon: unexpected eof")
			return
		}
		value, err = strconv.ParseFloat(scanner.Text(), 64)
		if err != nil {
			log.Println("carbon: Error parsing value", err, point.Path)
			return
		}
		point.Value = value
		if more = scanner.Scan(); !more {
			log.Println("carbon: unexpected eof")
			return
		}
		timestamp, err = strconv.ParseUint(scanner.Text(), 10, 32)
		if err != nil {
			log.Println("carbon: Error parsing timestamp", err, point.Path, point.Value)
			return
		}
		point.Timestamp = uint32(timestamp)
		store.Get().Set(point)
	}
}

func ListenAndServe(addr string) error {
	if store.Get() == nil {
		panic("carbon: store not set")
	}
	log.Println("Starting carbon on", addr)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go recv(conn)
	}
	panic("lol")
}
