CREATE KEYSPACE IF NOT EXISTS local_content_store WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

CREATE TABLE IF NOT EXISTS local_content_store.content_data (
    content_id text PRIMARY KEY,
    body blob,
    externallink text,
    last_updated_on timestamp,
    level1_concept list<text>,
    level1_name list<text>,
    level2_concept list<text>,
    level2_name list<text>,
    level3_concept list<text>,
    level3_name list<text>,
    ml_content_text text,
    ml_content_text_vector frozen<set<double>>,
    ml_keywords list<text>,
    oldbody blob,
    screenshots blob,
    source text,
    sourceurl text,
    stageicons blob,
    textbook_name list<text>
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';