-- Migration 001: add FAILED to the orderstatus enum.
--
-- Applies to databases created by init_db()/create_all BEFORE the
-- 2026-10-12 reliability fixes (when OrderStatus had no FAILED value).
-- Fresh databases do not need this: create_all creates the enum with
-- all values.
--
-- Preserves all existing data: ALTER TYPE ... ADD VALUE only extends
-- the label set; no rows are rewritten.
--
-- Usage:
--   psql "$DATABASE_URL" -f docs/migrations/001_add_failed_to_orderstatus.sql
--
-- Idempotent: the DO block skips the ALTER when the label exists.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_enum e
        JOIN pg_type t ON t.oid = e.enumtypid
        WHERE t.typname = 'orderstatus'
          AND e.enumlabel = 'FAILED'
    ) THEN
        ALTER TYPE orderstatus ADD VALUE 'FAILED';
        RAISE NOTICE 'orderstatus: added FAILED label';
    ELSE
        RAISE NOTICE 'orderstatus: FAILED label already present, skipping';
    END IF;
END
$$;
