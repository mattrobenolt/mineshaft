package carbon

import (
	"code.google.com/p/goprotobuf/proto"
	log "github.com/mattrobenolt/mineshaft/logging"
	"github.com/mattrobenolt/mineshaft/metric"
	"github.com/mattrobenolt/mineshaft/store"
	"github.com/mattrobenolt/semaphore"

	"bufio"
	"encoding/binary"
	"io"
	"net"
	"sync"
)

func recvProtobuf(c net.Conn, s *store.Store) {
	var (
		reader = bufio.NewReader(c)
		err    error
		length uint32
		wg     sync.WaitGroup
		sem    = semaphore.New(10)
		data   []byte
		n      int
	)
	defer c.Close()

	for {
		err = binary.Read(reader, binary.BigEndian, &length)
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Println("carbon/protobuf: error reading stream", err)
			return
		}

		data = make([]byte, length)
		n, err = reader.Read(data)
		if n < int(length) {
			log.Println("carbon/protobuf: incomplete read")
			return
		}
		if err != nil {
			log.Println("carbon/protobuf: error reading stream", err)
			return
		}

		p := metric.New()
		if err = proto.Unmarshal(data, p); err != nil {
			log.Println("carbon/protobuf: error Unmarshaling protobuf", err)
			p.Release()
			return
		}

		wg.Add(1)
		sem.Wait()
		go func(p *metric.Point) {
			s.Set(p)
			p.Release()
			wg.Done()
			sem.Signal()
		}(p)
	}

	wg.Wait()
}

func ListenAndServeProtobuf(addr string, s *store.Store) error {
	log.Println("carbon/protobuf: listening on", addr)

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
		go recvProtobuf(conn, s)
	}
	panic("lol")
}
