-- Demo seed (safe for local)
insert into users (name, email) values ('Demo User', 'demo@example.com')
on conflict do nothing;

-- Get the created/first user id
with u as (
  select id from users order by id asc limit 1
)
insert into devices (name, api_key_sha256, owner_user_id)
select 'ESP32-Demo', '2def41e79737d19380ec582df2cfa751f5830f8ea4b5d52f31b4356e4cdca3bb', u.id from u
on conflict do nothing;

with u as (
  select id from users order by id asc limit 1
)
insert into tanks (user_id, name, capacity_liters, location_text)
select u.id, 'Tanque Principal', 1000, 'Sala de bombas' from u
on conflict do nothing;

with u as (
  select id from users order by id asc limit 1
)
insert into pumps (user_id, name, model, max_flow_lpm)
select u.id, 'Bomba 1', 'BP-100', 60 from u
on conflict do nothing;