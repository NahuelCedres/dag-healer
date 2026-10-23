-- dag-healer store schema v1 (phase 5), SQLite: the frozen DDL created by init_db.
--
-- The sqlite_master statements of a v1 store, in creation order. Regenerate only while
-- dag_healer.store.models.SCHEMA_VERSION == 1 (HEALER_UPDATE_GOLDEN=1 pytest
-- tests/unit/test_store_v1_fixture.py); after that it is the baseline of the v2 migration.

CREATE TABLE healer_incidents (
	id VARCHAR(32) NOT NULL,
	dag_id VARCHAR(250) NOT NULL,
	run_id VARCHAR(250) NOT NULL,
	task_id VARCHAR(250) NOT NULL,
	map_index INTEGER NOT NULL,
	try_number INTEGER NOT NULL,
	status VARCHAR(32) NOT NULL,
	category VARCHAR(16),
	confidence FLOAT,
	source VARCHAR(16) NOT NULL,
	error_summary TEXT NOT NULL,
	diagnosis TEXT,
	violated_rule JSON,
	sample JSON,
	evidence JSON,
	test_path VARCHAR(1024),
	test_code TEXT,
	diff TEXT,
	pytest_output TEXT,
	pr_url VARCHAR(1024),
	pr_md_path VARCHAR(1024),
	signature VARCHAR(128),
	occurrences INTEGER NOT NULL,
	attempts INTEGER NOT NULL,
	tokens_used INTEGER NOT NULL,
	resolution VARCHAR(32),
	resolved_by VARCHAR(250),
	created_at DATETIME NOT NULL,
	updated_at DATETIME NOT NULL,
	claimed_at DATETIME,
	PRIMARY KEY (id),
	CONSTRAINT uq_healer_incidents_ti UNIQUE (dag_id, run_id, task_id, map_index, try_number)
);

CREATE INDEX ix_healer_incidents_dag_created ON healer_incidents (dag_id, created_at);

CREATE INDEX ix_healer_incidents_signature ON healer_incidents (dag_id, task_id, signature);

CREATE INDEX ix_healer_incidents_status ON healer_incidents (status);

CREATE TABLE healer_contracts (
	dag_id VARCHAR(250) NOT NULL,
	status VARCHAR(16) NOT NULL,
	approved_by VARCHAR(250),
	approved_at DATETIME,
	path VARCHAR(1024) NOT NULL,
	source_hash VARCHAR(128),
	updated_at DATETIME NOT NULL,
	PRIMARY KEY (dag_id)
);

CREATE TABLE healer_pending_events (
	id INTEGER NOT NULL,
	dag_id VARCHAR(250) NOT NULL,
	run_id VARCHAR(250) NOT NULL,
	task_id VARCHAR(250) NOT NULL,
	map_index INTEGER NOT NULL,
	try_number INTEGER NOT NULL,
	payload JSON,
	received_at DATETIME NOT NULL,
	status VARCHAR(16) NOT NULL,
	PRIMARY KEY (id)
);

CREATE INDEX ix_healer_pending_events_status ON healer_pending_events (status, id);

CREATE TABLE healer_schema_version (
	version INTEGER NOT NULL,
	applied_at DATETIME NOT NULL,
	PRIMARY KEY (version)
);

CREATE TABLE healer_incident_events (
	id INTEGER NOT NULL,
	incident_id VARCHAR(32) NOT NULL,
	at DATETIME NOT NULL,
	kind VARCHAR(32) NOT NULL,
	message TEXT NOT NULL,
	data JSON,
	actor VARCHAR(250) NOT NULL,
	PRIMARY KEY (id),
	FOREIGN KEY(incident_id) REFERENCES healer_incidents (id) ON DELETE CASCADE
);

CREATE INDEX ix_healer_incident_events_incident ON healer_incident_events (incident_id, id);

CREATE TABLE healer_alerts (
	id INTEGER NOT NULL,
	incident_id VARCHAR(32),
	channel VARCHAR(32) NOT NULL,
	recipients JSON,
	subject TEXT NOT NULL,
	status VARCHAR(16) NOT NULL,
	error TEXT,
	sent_at DATETIME NOT NULL,
	PRIMARY KEY (id),
	FOREIGN KEY(incident_id) REFERENCES healer_incidents (id) ON DELETE CASCADE
);

CREATE INDEX ix_healer_alerts_incident ON healer_alerts (incident_id);

CREATE TABLE healer_commands (
	id INTEGER NOT NULL,
	kind VARCHAR(32) NOT NULL,
	incident_id VARCHAR(32),
	dag_id VARCHAR(250) NOT NULL,
	payload JSON,
	actor VARCHAR(250) NOT NULL,
	status VARCHAR(16) NOT NULL,
	created_at DATETIME NOT NULL,
	applied_at DATETIME,
	error TEXT,
	PRIMARY KEY (id),
	FOREIGN KEY(incident_id) REFERENCES healer_incidents (id) ON DELETE CASCADE
);

CREATE INDEX ix_healer_commands_status ON healer_commands (status, id);
