-- 在 Supabase SQL Editor 中执行一次。金额单位：元。
create table if not exists public.market_turnover_history (
    date date primary key,
    sh_amount numeric not null check (sh_amount > 0),
    sz_amount numeric not null check (sz_amount > 0),
    bj_amount numeric not null check (bj_amount > 0),
    total_amount numeric generated always as (sh_amount + sz_amount + bj_amount) stored,
    updated_at timestamptz not null default now()
);

alter table public.market_turnover_history enable row level security;
-- 服务器端 Streamlit 使用 service_role 密钥；不开放匿名写入。
grant all on table public.market_turnover_history to service_role;
