CREATE KEYSPACE dialcodes WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '2'}  AND durable_writes = true;

CREATE TABLE dialcodes.dialcode_images (
    filename text PRIMARY KEY,
    channel text,
    config map<text, text>,
    created_on timestamp,
    dialcode text,
    publisher text,
    status int,
    url text
);

CREATE TABLE dialcodes.dialcode_batch (
    processid uuid PRIMARY KEY,
    channel text,
    config map<text, text>,
    created_on timestamp,
    dialcodes list<text>,
    publisher text,
    status int,
    url text
);