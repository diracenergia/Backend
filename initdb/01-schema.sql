-- Users
create table if not exists users(
  id bigserial primary key,
  name text not null,
  email text unique,
  created_at timestamptz default now()
);

-- Devices (ESP32) - local: store SHA256 (simpler than bcrypt). For prod, switch to bcrypt.
create table if not exists devices(
  id bigserial primary key,
  name text not null,
  api_key_sha256 text not null,
  owner_user_id bigint references users(id) on delete set null,
  created_at timestamptz default now(),
  last_seen_at timestamptz
);

-- Tanks
create table if not exists tanks(
  id bigserial primary key,
  user_id bigint references users(id) on delete cascade,
  name text not null,
  capacity_liters numeric,
  location_text text,
  location_lat numeric,
  location_lng numeric,
  created_at timestamptz default now()
);

-- Pumps
create table if not exists pumps(
  id bigserial primary key,
  user_id bigint references users(id) on delete cascade,
  name text not null,
  model text,
  max_flow_lpm numeric,
  created_at timestamptz default now()
);

-- Tank readings (history)
create table if not exists tank_readings(
  id bigserial primary key,
  tank_id bigint references tanks(id) on delete cascade,
  device_id bigint references devices(id) on delete set null,
  ts timestamptz not null default now(),
  level_percent numeric,
  volume_l numeric,
  temperature_c numeric,
  raw_json jsonb
);
create index if not exists idx_tank_readings_tank_ts on tank_readings(tank_id, ts desc);

-- Pump readings (history)
create table if not exists pump_readings(
  id bigserial primary key,
  pump_id bigint references pumps(id) on delete cascade,
  device_id bigint references devices(id) on delete set null,
  ts timestamptz not null default now(),
  is_on boolean,
  flow_lpm numeric,
  pressure_bar numeric,
  voltage_v numeric,
  current_a numeric,
  raw_json jsonb
);
create index if not exists idx_pump_readings_pump_ts on pump_readings(pump_id, ts desc);