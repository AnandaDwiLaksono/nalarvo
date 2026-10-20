-- Forward-only: preserve already-applied M5 governance checksums.
-- NULL means no expiry configured; configured expiries are validated at decision time.
ALTER TABLE approval_requests ADD COLUMN expires_at TEXT;
