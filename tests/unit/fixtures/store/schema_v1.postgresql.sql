-- dag-healer store schema v1 (phase 5), PostgreSQL: the frozen DDL of the demo stack.
--
-- Dumped from the running Docker stack (postgres:16, database "healer", schema version 1)
-- with `pg_dump --schema-only --no-owner --no-privileges healer` (pg_dump 16.15) on
-- 2026-10-19; only comments and the psql \restrict / \unrestrict lines were removed.
-- Used by the store v2 migration tests (section 11.4 of docs/architecture/orchestrators.md).
-- Never regenerate it from newer models: it is the v1 baseline.

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

CREATE TABLE public.healer_alerts (
    id integer NOT NULL,
    incident_id character varying(32),
    channel character varying(32) NOT NULL,
    recipients json,
    subject text NOT NULL,
    status character varying(16) NOT NULL,
    error text,
    sent_at timestamp with time zone NOT NULL
);

CREATE SEQUENCE public.healer_alerts_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.healer_alerts_id_seq OWNED BY public.healer_alerts.id;

CREATE TABLE public.healer_commands (
    id integer NOT NULL,
    kind character varying(32) NOT NULL,
    incident_id character varying(32),
    dag_id character varying(250) NOT NULL,
    payload json,
    actor character varying(250) NOT NULL,
    status character varying(16) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    applied_at timestamp with time zone,
    error text
);

CREATE SEQUENCE public.healer_commands_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.healer_commands_id_seq OWNED BY public.healer_commands.id;

CREATE TABLE public.healer_contracts (
    dag_id character varying(250) NOT NULL,
    status character varying(16) NOT NULL,
    approved_by character varying(250),
    approved_at timestamp with time zone,
    path character varying(1024) NOT NULL,
    source_hash character varying(128),
    updated_at timestamp with time zone NOT NULL
);

CREATE TABLE public.healer_incident_events (
    id integer NOT NULL,
    incident_id character varying(32) NOT NULL,
    at timestamp with time zone NOT NULL,
    kind character varying(32) NOT NULL,
    message text NOT NULL,
    data json,
    actor character varying(250) NOT NULL
);

CREATE SEQUENCE public.healer_incident_events_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.healer_incident_events_id_seq OWNED BY public.healer_incident_events.id;

CREATE TABLE public.healer_incidents (
    id character varying(32) NOT NULL,
    dag_id character varying(250) NOT NULL,
    run_id character varying(250) NOT NULL,
    task_id character varying(250) NOT NULL,
    map_index integer NOT NULL,
    try_number integer NOT NULL,
    status character varying(32) NOT NULL,
    category character varying(16),
    confidence double precision,
    source character varying(16) NOT NULL,
    error_summary text NOT NULL,
    diagnosis text,
    violated_rule json,
    sample json,
    evidence json,
    test_path character varying(1024),
    test_code text,
    diff text,
    pytest_output text,
    pr_url character varying(1024),
    pr_md_path character varying(1024),
    signature character varying(128),
    occurrences integer NOT NULL,
    attempts integer NOT NULL,
    tokens_used integer NOT NULL,
    resolution character varying(32),
    resolved_by character varying(250),
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    claimed_at timestamp with time zone
);

CREATE TABLE public.healer_pending_events (
    id integer NOT NULL,
    dag_id character varying(250) NOT NULL,
    run_id character varying(250) NOT NULL,
    task_id character varying(250) NOT NULL,
    map_index integer NOT NULL,
    try_number integer NOT NULL,
    payload json,
    received_at timestamp with time zone NOT NULL,
    status character varying(16) NOT NULL
);

CREATE SEQUENCE public.healer_pending_events_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.healer_pending_events_id_seq OWNED BY public.healer_pending_events.id;

CREATE TABLE public.healer_schema_version (
    version integer NOT NULL,
    applied_at timestamp with time zone NOT NULL
);

ALTER TABLE ONLY public.healer_alerts ALTER COLUMN id SET DEFAULT nextval('public.healer_alerts_id_seq'::regclass);

ALTER TABLE ONLY public.healer_commands ALTER COLUMN id SET DEFAULT nextval('public.healer_commands_id_seq'::regclass);

ALTER TABLE ONLY public.healer_incident_events ALTER COLUMN id SET DEFAULT nextval('public.healer_incident_events_id_seq'::regclass);

ALTER TABLE ONLY public.healer_pending_events ALTER COLUMN id SET DEFAULT nextval('public.healer_pending_events_id_seq'::regclass);

ALTER TABLE ONLY public.healer_alerts
    ADD CONSTRAINT healer_alerts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.healer_commands
    ADD CONSTRAINT healer_commands_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.healer_contracts
    ADD CONSTRAINT healer_contracts_pkey PRIMARY KEY (dag_id);

ALTER TABLE ONLY public.healer_incident_events
    ADD CONSTRAINT healer_incident_events_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.healer_incidents
    ADD CONSTRAINT healer_incidents_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.healer_pending_events
    ADD CONSTRAINT healer_pending_events_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.healer_schema_version
    ADD CONSTRAINT healer_schema_version_pkey PRIMARY KEY (version);

ALTER TABLE ONLY public.healer_incidents
    ADD CONSTRAINT uq_healer_incidents_ti UNIQUE (dag_id, run_id, task_id, map_index, try_number);

CREATE INDEX ix_healer_alerts_incident ON public.healer_alerts USING btree (incident_id);

CREATE INDEX ix_healer_commands_status ON public.healer_commands USING btree (status, id);

CREATE INDEX ix_healer_incident_events_incident ON public.healer_incident_events USING btree (incident_id, id);

CREATE INDEX ix_healer_incidents_dag_created ON public.healer_incidents USING btree (dag_id, created_at);

CREATE INDEX ix_healer_incidents_signature ON public.healer_incidents USING btree (dag_id, task_id, signature);

CREATE INDEX ix_healer_incidents_status ON public.healer_incidents USING btree (status);

CREATE INDEX ix_healer_pending_events_status ON public.healer_pending_events USING btree (status, id);

ALTER TABLE ONLY public.healer_alerts
    ADD CONSTRAINT healer_alerts_incident_id_fkey FOREIGN KEY (incident_id) REFERENCES public.healer_incidents(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.healer_commands
    ADD CONSTRAINT healer_commands_incident_id_fkey FOREIGN KEY (incident_id) REFERENCES public.healer_incidents(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.healer_incident_events
    ADD CONSTRAINT healer_incident_events_incident_id_fkey FOREIGN KEY (incident_id) REFERENCES public.healer_incidents(id) ON DELETE CASCADE;
