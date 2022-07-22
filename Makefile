spec_docs:
	generate-schema-doc --config-file conf/json_schema_for_humans.json source-debezium/app/src/main/resources/ source-debezium/docs
	generate-schema-doc --config-file conf/json_schema_for_humans.json sink-jdbc/app/src/main/resources/ sink-jdbc/docs

