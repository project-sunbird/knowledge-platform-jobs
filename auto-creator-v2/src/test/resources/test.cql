CREATE KEYSPACE IF NOT EXISTS dev_hierarchy_store with replication = {'class':'SimpleStrategy','replication_factor':1};
CREATE TABLE IF NOT EXISTS dev_hierarchy_store.questionset_hierarchy (
  identifier text,
  hierarchy text,
  PRIMARY KEY (identifier)
);

CREATE KEYSPACE IF NOT EXISTS dev_question_store with replication = {'class':'SimpleStrategy','replication_factor':1};
CREATE TABLE IF NOT EXISTS dev_question_store.question_data (
  identifier text,
  body blob,
  editorstate text,
  answer blob,
  solutions text,
  instructions text,
  hints text,
  media text,
  responsedeclaration text,
  interactions text,
  PRIMARY KEY (identifier)
);