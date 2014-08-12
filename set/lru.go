package set

import (
	"github.com/dustin/go-humanize"

	"bufio"
	"compress/gzip"
	"container/list"
	"io"
	"log"
	"os"
	"path"
	"time"
)

// Set is an LRU cache. It is not safe for concurrent access.
type Set struct {
	// MaxEntries is the maximum number of cache entries before
	// an item is evicted.
	MaxEntries int

	ll    *list.List
	cache map[string]*list.Element

	fp      *os.File
	freq    time.Duration
	changed bool
}

func New(maxEntries int) *Set {
	return &Set{
		MaxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[string]*list.Element),
	}
}

// NewPersistent creates a new Set backed by
// an io.ReadWriter that
func NewPersistent(maxEntries int, file string, freq time.Duration) (s *Set, err error) {
	s = New(maxEntries)
	if err = os.MkdirAll(path.Dir(file), 0777); err != nil {
		return nil, err
	}
	if s.fp, err = os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0666); err != nil {
		return nil, err
	}
	s.freq = freq
	s.load()
	// After an initial load, we're up to date
	// so don't trigger a new save
	s.changed = false
	go s.bgsave()
	return s, nil
}

func (s *Set) Close() {
	if s.fp != nil {
		s.fp.Close()
	}
}

// Add adds a value to the set.
// return value indicates if the Add was successful
// false if the key already existed in the Set
func (s *Set) Add(key string) bool {
	s.changed = true
	if ee, ok := s.cache[key]; ok {
		s.ll.MoveToFront(ee)
		return false
	}
	ele := s.ll.PushFront(key)
	s.cache[key] = ele
	if s.ll.Len() > s.MaxEntries {
		s.RemoveOldest()
	}
	return true
}

// Remove removes the provided key from the cache.
func (s *Set) Remove(key string) {
	if ele, hit := s.cache[key]; hit {
		s.removeElement(ele)
	}
}

// RemoveOldest removes the oldest item from the cache.
func (s *Set) RemoveOldest() {
	ele := s.ll.Back()
	if ele != nil {
		s.removeElement(ele)
	}
}

func (s *Set) removeElement(e *list.Element) {
	s.ll.Remove(e)
	delete(s.cache, e.Value.(string))
	s.changed = true
}

// Len returns the number of items in the set.
func (s *Set) Len() int {
	return s.ll.Len()
}

// Read the saved cache file from disk
// and explicitly ignore all errors because
// it's only a cache.
func (s *Set) load() {
	reader, err := gzip.NewReader(s.fp)
	if err == io.EOF {
		return
	}
	if err != nil {
		log.Println("index/lru:", err)
		return
	}
	defer reader.Close()
	scanner := bufio.NewScanner(reader)
	i := int64(0)
	for scanner.Scan() {
		s.Add(scanner.Text())
		i++
	}
	log.Println("index/lru: loaded", humanize.Comma(i), "keys from cache")
	return
}

// Flushes our key cache to disk as some gzipped blob
// This makes no attempt to be concurrency safe, or any guarantees
// that the keys written exactly reflect the linked list we're flushing.
// The list may get modified in the middle of a write.
// This is a cache, so we don't really care.
func (s *Set) bgsave() {
	writer := bufio.NewWriter(s.fp)
	gzipper := gzip.NewWriter(writer)
	i := int64(0)
	var e *list.Element
	for {
		time.Sleep(s.freq)
		if !s.changed || s.Len() == 0 {
			continue
		}
		s.changed = false
		s.fp.Truncate(0)
		s.fp.Seek(0, 0)
		for e = s.ll.Back(); e != nil; e = e.Prev() {
			i++
			gzipper.Write([]byte(e.Value.(string) + "\n"))
		}
		gzipper.Flush()
		writer.Flush()
		gzipper.Reset(writer)
		log.Println("index/lru: flushed", humanize.Comma(i), "keys to disk")
		i = 0
	}
}
