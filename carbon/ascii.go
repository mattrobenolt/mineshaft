package carbon

import (
	"github.com/mattrobenolt/mineshaft/metric"
	"github.com/mattrobenolt/mineshaft/store"
	"github.com/mattrobenolt/semaphore"

	"bufio"
	"log"
	"net"
	"strconv"
	"sync"
)

func recvAscii(c net.Conn, s *store.Store) {
	var (
		scanner   *bufio.Scanner
		more      bool
		value     float64
		err       error
		timestamp uint64
		path      string
		wg        sync.WaitGroup
		sem       = semaphore.New(10)
	)
	defer c.Close()

	scanner = bufio.NewScanner(c)
	scanner.Split(bufio.ScanWords)

	for {
		if more = scanner.Scan(); !more {
			// EOF
			return
		}

		path = scanner.Text()
		if more = scanner.Scan(); !more {
			log.Println("carbon/ascii: unexpected eof")
			return
		}
		value, err = strconv.ParseFloat(scanner.Text(), 64)
		if err != nil {
			log.Println("carbon/ascii: Error parsing value", err, path)
			return
		}
		if more = scanner.Scan(); !more {
			log.Println("carbon/ascii: unexpected eof")
			return
		}
		timestamp, err = strconv.ParseUint(scanner.Text(), 10, 32)
		if err != nil {
			log.Println("carbon/ascii: Error parsing timestamp", err, path, value)
			return
		}

		wg.Add(1)
		sem.Wait()
		go func(path string, value float64, timestamp uint32) {
			p := metric.New()
			p.SetPath(path)
			p.SetValue(value)
			p.SetTimestamp(timestamp)
			s.Set(p)
			p.Release()
			wg.Done()
			sem.Signal()
		}(path, value, uint32(timestamp))
	}

	wg.Wait()
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
