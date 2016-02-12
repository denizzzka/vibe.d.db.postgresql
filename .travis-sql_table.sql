CREATE TABLE IF NOT EXISTS pgator_rpc
(
  method text NOT NULL,
  sql_queries text[] NOT NULL,
  args text[] NOT NULL,
  set_username boolean NOT NULL,
  read_only boolean NOT NULL,
  commentary text,
  one_row_flags boolean[],

  CONSTRAINT pgator_rpc_pkey PRIMARY KEY (method)
);
