package carbon

import (
	"github.com/mattrobenolt/mineshaft/metric"
	"github.com/mattrobenolt/mineshaft/store"

	"bufio"
	"log"
	"net"
	"strconv"
)

func recvAscii(c net.Conn, s *store.Store) {
	var (
		scanner   *bufio.Scanner
		point     = metric.New()
		more      bool
		value     float64
		err       error
		timestamp uint64
	)
	defer c.Close()
	defer point.Release()

	scanner = bufio.NewScanner(c)
	scanner.Split(bufio.ScanWords)

	for {
		if more = scanner.Scan(); !more {
			// EOF
			return
		}
		point.Path = scanner.Text()
		if more = scanner.Scan(); !more {
			log.Println("carbon/ascii: unexpected eof")
			return
		}
		value, err = strconv.ParseFloat(scanner.Text(), 64)
		if err != nil {
			log.Println("carbon/ascii: Error parsing value", err, point.Path)
			return
		}
		point.Value = value
		if more = scanner.Scan(); !more {
			log.Println("carbon/ascii: unexpected eof")
			return
		}
		timestamp, err = strconv.ParseUint(scanner.Text(), 10, 32)
		if err != nil {
			log.Println("carbon/ascii: Error parsing timestamp", err, point.Path, point.Value)
			return
		}
		point.Timestamp = uint32(timestamp)
		s.Set(point)
	}
}

func ListenAndServeAscii(addr string, s *store.Store) error {
	log.Println("carbon/ascii: listening on", addr)

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
		go recvAscii(conn, s)
	}
	panic("lol")
}
