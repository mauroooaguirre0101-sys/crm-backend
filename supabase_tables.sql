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
