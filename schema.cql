CREATE KEYSPACE metrics WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

USE metrics;

-- Calculate average by data/count
-- Update by incrementing data by value and count by 1
CREATE TABLE avg (
  period int,
  rollup int,
  path text,
  time bigint,
  value double,
  PRIMARY KEY ((period, rollup, path), time)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=86400 AND
  index_interval=512 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='NONE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'sstable_size_in_mb': '160', 'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

-- Just simply update the data counter
CREATE TABLE sum (
  period int,
  rollup int,
  path text,
  time bigint,
  value double,
  PRIMARY KEY ((period, rollup, path), time)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=86400 AND
  index_interval=512 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='NONE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'sstable_size_in_mb': '160', 'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

-- Just keep updating data with the last value seen
-- They only differ on how we actually write the data
CREATE TABLE minmaxlast (
  period int,
  rollup int,
  path text,
  time bigint,
  value double,
  PRIMARY KEY ((period, rollup, path), time)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=86400 AND
  index_interval=512 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='NONE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'sstable_size_in_mb': '160', 'class': 'LeveledCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};
