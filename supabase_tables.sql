-- Correr en Supabase > SQL Editor

create table if not exists sops (
  id uuid primary key default gen_random_uuid(),
  cliente_id text not null,
  data jsonb not null default '{}',
  created_at timestamptz default now()
);

create table if not exists fundaciones (
  cliente_id text primary key,
  data jsonb not null default '{}',
  updated_at timestamptz default now()
);

create table if not exists contenido (
  id uuid primary key default gen_random_uuid(),
  cliente_id text not null,
  data jsonb not null default '{}',
  created_at timestamptz default now()
);

create table if not exists angulos (
  id uuid primary key default gen_random_uuid(),
  cliente_id text not null,
  data jsonb not null default '{}',
  created_at timestamptz default now()
);

create table if not exists referentes (
  id uuid primary key default gen_random_uuid(),
  cliente_id text not null,
  data jsonb not null default '{}',
  created_at timestamptz default now()
);

create table if not exists metricas (
  id uuid primary key default gen_random_uuid(),
  cliente_id text not null,
  data jsonb not null default '{}',
  created_at timestamptz default now()
);

create table if not exists ig_cuenta (
  cliente_id text primary key,
  data jsonb not null default '{}',
  updated_at timestamptz default now()
);

create table if not exists ig_reels (
  id uuid primary key default gen_random_uuid(),
  cliente_id text not null,
  data jsonb not null default '{}',
  created_at timestamptz default now()
);

create table if not exists ig_carruseles (
  id uuid primary key default gen_random_uuid(),
  cliente_id text not null,
  data jsonb not null default '{}',
  created_at timestamptz default now()
);

-- ══════════════════════════════════════════════════════
-- GHL + Calendar Integrations
-- ══════════════════════════════════════════════════════

-- Unified table for all calendar/CRM provider integrations (GHL, future providers)
create table if not exists calendar_integrations (
  id                   uuid primary key default gen_random_uuid(),
  negocio_id           text not null unique,
  provider             text not null,              -- 'ghl' | 'calendly' | ...
  access_token         text,
  refresh_token        text,
  token_expires_at     timestamptz,
  provider_user_id     text,                       -- GHL userId
  provider_location_id text,                       -- GHL locationId (sub-account)
  calendar_id          text,                       -- specific calendar within location
  webhook_id           text,                       -- webhook subscription ID on provider
  webhook_token        text,                       -- our URL token (?t=TOKEN)
  webhook_url          text,                       -- full callback URL registered
  metadata             jsonb default '{}',         -- provider-specific extras (e.g. locationName)
  connected_at         timestamptz,
  status               text default 'connected',   -- 'connected' | 'error' | 'disconnected'
  created_at           timestamptz default now(),
  updated_at           timestamptz default now()
);

-- New columns on calls table for provider-agnostic tracking
alter table calls add column if not exists provider_event_id text;   -- GHL appointment ID
alter table calls add column if not exists calendar_name     text;   -- calendar/event type name
