package carbon

import (
	pickle "github.com/kisielk/og-rek"
	"github.com/mattrobenolt/mineshaft/metric"
	"github.com/mattrobenolt/mineshaft/store"

	"log"
	"net"
)

func recvPickle(c net.Conn, s *store.Store) {
	point := metric.New()
	defer c.Close()
	defer point.Release()

	data, err := pickle.NewDecoder(c).Decode()
	if err != nil {
		log.Println("carbon/pickle: error decoding pickle stream", err)
		return
	}

	for _, d := range data.([]interface{}) {
		point.Path = d.([]interface{})[0].(string)
		point.Timestamp = uint32(d.([]interface{})[1].([]interface{})[0].(int64))
		switch t := d.([]interface{})[1].([]interface{})[1].(type) {
		case int64:
			point.Value = float64(d.([]interface{})[1].([]interface{})[1].(int64))
		case float64:
			point.Value = d.([]interface{})[1].([]interface{})[1].(float64)
		default:
			log.Println("carbon/pickle: invalid type", t)
		}
		s.Set(point)
	}
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
