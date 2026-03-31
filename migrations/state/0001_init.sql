create table if not exists connection_state (
    connection_id text primary key,
    last_sync_started_at text,
    last_sync_finished_at text,
    last_sync_status text,
    last_error text,
    postgres_cdc_last_lsn text,
    postgres_cdc_slot_name text,
    updated_at bigint not null
);

create table if not exists table_checkpoints (
    connection_id text not null,
    source_kind text not null,
    entity_name text not null,
    checkpoint_json text not null,
    updated_at bigint not null,
    primary key (connection_id, source_kind, entity_name)
);

create table if not exists connection_locks (
    connection_id text primary key,
    owner_id text not null,
    acquired_at bigint not null,
    heartbeat_at bigint not null
);
