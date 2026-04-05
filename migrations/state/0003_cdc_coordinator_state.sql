create table if not exists cdc_commit_fragments (
    connection_id text not null,
    fragment_id text primary key,
    job_id text not null,
    sequence bigint not null,
    commit_lsn text not null,
    table_key text not null,
    status text not null,
    row_count bigint not null default 0,
    upserted_count bigint not null default 0,
    deleted_count bigint not null default 0,
    last_error text,
    created_at bigint not null,
    updated_at bigint not null
);

create index if not exists cdc_commit_fragments_connection_status_sequence_idx
    on cdc_commit_fragments (connection_id, status, sequence, created_at);

create index if not exists cdc_commit_fragments_connection_job_idx
    on cdc_commit_fragments (connection_id, job_id);

create index if not exists cdc_commit_fragments_connection_table_sequence_idx
    on cdc_commit_fragments (connection_id, table_key, sequence, created_at);

create table if not exists cdc_watermark_state (
    connection_id text primary key,
    next_sequence_to_ack bigint not null default 0,
    last_enqueued_sequence bigint,
    last_received_lsn text,
    last_flushed_lsn text,
    last_persisted_lsn text,
    updated_at bigint not null
);
