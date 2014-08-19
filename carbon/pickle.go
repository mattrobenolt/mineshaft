package carbon

import (
	pickle "github.com/kisielk/og-rek"
	"github.com/mattrobenolt/mineshaft/metric"
	"github.com/mattrobenolt/mineshaft/store"
	"github.com/mattrobenolt/semaphore"

	"bufio"
	"encoding/binary"
	"io"
	"log"
	"net"
	"sync"
)

func recvPickle(c net.Conn, s *store.Store) {
	var (
		reader    = bufio.NewReader(c)
		lreader   = &io.LimitedReader{reader, 0}
		err       error
		data      interface{}
		length    uint32
		wg        sync.WaitGroup
		sem       = semaphore.New(10)
		path      string
		value     float64
		timestamp uint32
	)
	defer c.Close()

	for {
		err = binary.Read(reader, binary.BigEndian, &length)
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Println("carbon/pickle: error reading stream", err)
			return
		}

		lreader.N = int64(length)
		data, err = pickle.NewDecoder(lreader).Decode()
		if err != nil {
			log.Println("carbon/pickle: error decoding stream", err)
			return
		}

		for _, d := range data.([]interface{}) {
			path = d.([]interface{})[0].(string)
			timestamp = uint32(d.([]interface{})[1].([]interface{})[0].(int64))
			switch t := d.([]interface{})[1].([]interface{})[1].(type) {
			case int64:
				value = float64(d.([]interface{})[1].([]interface{})[1].(int64))
			case float64:
				value = d.([]interface{})[1].([]interface{})[1].(float64)
			default:
				log.Println("carbon/pickle: invalid type", t)
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
			}(path, value, timestamp)
		}
	}

	wg.Wait()
}

func ListenAndServePickle(addr string, s *store.Store) error {
	log.Println("carbon/pickle: listening on", addr)

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
		go recvPickle(conn, s)
	}
	panic("lol")
}
