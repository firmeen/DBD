-- ============================================================================
-- DBD REMOTE CLIENT USER MANAGEMENT
-- ============================================================================
-- This file is intentionally FAIL-CLOSED.
--
-- Every CREATE/ALTER/GRANT/REVOKE/DROP example below is commented out.
-- Copy only the statements you intend to run, replace all placeholders first,
-- and never commit real passwords.
--
-- Recommended default privilege for normal DBD clients:
--   SELECT ON dbd.*
-- ============================================================================


-- ============================================================================
-- 1. DIRECT LAN CLIENT
-- ============================================================================
-- Use a specific approved client IP when the client reaches MySQL directly.
--
-- CREATE USER 'dbd-<team>'@'<CLIENT_IP>'
-- IDENTIFIED BY '<STRONG_TEAM_PASSWORD>';
--
-- GRANT SELECT
-- ON dbd.*
-- TO 'dbd-<team>'@'<CLIENT_IP>';


-- ============================================================================
-- 2. PYTHON REMOTE GATEWAY CLIENT
-- ============================================================================
-- dbd_gateway.py connects to MySQL through:
--   127.0.0.1:3307
--
-- Therefore MySQL matches a loopback account, not the original remote
-- workstation IP.
--
-- CREATE USER 'dbd-<team>'@'127.0.0.1'
-- IDENTIFIED BY '<STRONG_TEAM_PASSWORD>';
--
-- GRANT SELECT
-- ON dbd.*
-- TO 'dbd-<team>'@'127.0.0.1';
--
-- Optional connection cap:
--
-- ALTER USER 'dbd-<team>'@'127.0.0.1'
-- WITH MAX_USER_CONNECTIONS 10;


-- ============================================================================
-- 3. VERIFY ACCOUNTS
-- ============================================================================

SELECT User, Host, account_locked
FROM mysql.user
WHERE User LIKE 'dbd-%'
ORDER BY User, Host;

-- Direct LAN example:
-- SHOW GRANTS FOR 'dbd-<team>'@'<CLIENT_IP>';

-- Gateway example:
-- SHOW GRANTS FOR 'dbd-<team>'@'127.0.0.1';


-- ============================================================================
-- 4. VERIFY EFFECTIVE CONNECTION IDENTITY
-- ============================================================================
-- Run this after connecting as the client account:
--
-- SELECT
--     DATABASE(),
--     USER(),
--     CURRENT_USER(),
--     CONNECTION_ID(),
--     NOW();
--
-- Gateway mode should normally show CURRENT_USER() similar to:
--   dbd-<team>@127.0.0.1


-- ============================================================================
-- 5. PASSWORD ROTATION
-- ============================================================================

-- ALTER USER 'dbd-<team>'@'127.0.0.1'
-- IDENTIFIED BY '<NEW_STRONG_PASSWORD>';

-- ALTER USER 'dbd-<team>'@'<CLIENT_IP>'
-- IDENTIFIED BY '<NEW_STRONG_PASSWORD>';


-- ============================================================================
-- 6. REVOKE OR REMOVE ACCESS
-- ============================================================================

-- REVOKE SELECT
-- ON dbd.*
-- FROM 'dbd-<team>'@'127.0.0.1';

-- DROP USER IF EXISTS 'dbd-<team>'@'127.0.0.1';

-- REVOKE SELECT
-- ON dbd.*
-- FROM 'dbd-<team>'@'<CLIENT_IP>';

-- DROP USER IF EXISTS 'dbd-<team>'@'<CLIENT_IP>';


-- ============================================================================
-- 7. SECURITY NOTES
-- ============================================================================
-- Prefer:
--   'dbd-<team>'@'<specific-client-ip>'   for direct LAN
--   'dbd-<team>'@'127.0.0.1'             for Python gateway
--
-- Avoid:
--   'dbd-<team>'@'%'
--
-- Do not grant ALL PRIVILEGES or GRANT OPTION to ordinary client accounts.
-- Keep collector/runtime credentials separate from analyst/team credentials.
-- ============================================================================
