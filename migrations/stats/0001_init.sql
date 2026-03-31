create table if not exists runs (
    run_id text primary key,
    connection_id text not null,
    started_at text not null,
    finished_at text,
    status text,
    error text,
    rows_read bigint not null,
    rows_written bigint not null,
    rows_deleted bigint not null,
    rows_upserted bigint not null,
    extract_ms bigint not null,
    load_ms bigint not null,
    api_calls bigint not null,
    rate_limit_hits bigint not null
);

create table if not exists run_tables (
    id bigserial primary key,
    run_id text not null,
    connection_id text not null,
    table_name text not null,
    rows_read bigint not null,
    rows_written bigint not null,
    rows_deleted bigint not null,
    rows_upserted bigint not null,
    extract_ms bigint not null,
    load_ms bigint not null
);

create index if not exists idx_runs_connection on runs(connection_id);
create index if not exists idx_run_tables_connection on run_tables(connection_id);
create index if not exists idx_run_tables_table on run_tables(table_name);
