spec_docs:
	generate-schema-doc --config-file conf/json_schema_for_humans.json source-debezium/app/src/main/resources/ docs/specs/
	generate-schema-doc --config-file conf/json_schema_for_humans.json sink-jdbc/app/src/main/resources/ docs/specs/
	generate-schema-doc --config-file conf/json_schema_for_humans.json sink-mongodb/app/src/main/resources/spec.json docs/specs/sink_mongodb_spec.md

