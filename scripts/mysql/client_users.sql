-- ============================================================================
-- DBD REMOTE CLIENT USER MANAGEMENT
-- ============================================================================
-- Naming convention:
--   dbd-<team>
--
-- MySQL account identity is:
--   '<username>'@'<client-ip>'
--
-- Example:
--   'dbd-tipdi'@'172.24.114.41'
--
-- Recommended default for remote clients:
--   SELECT only on dbd.*
--
-- CREATE USER / GRANT / REVOKE / DROP USER take effect immediately.
-- FLUSH PRIVILEGES is not required when using these account-management commands.
-- ============================================================================


-- ============================================================================
-- 1. CREATE A READ-ONLY CLIENT USER
-- ============================================================================
-- Edit TEAM, CLIENT_IP, and password before running.

CREATE USER 'dbd-tipdi'@'172.24.114.41'
IDENTIFIED BY 'CHANGE_ME_TEAM_PASSWORD';

GRANT SELECT
ON dbd.*
TO 'dbd-tipdi'@'172.24.114.41';


-- ============================================================================
-- 2. ADD ANOTHER MACHINE FOR THE SAME TEAM
-- ============================================================================
-- Same username and team password can be used for another approved IP.
--
-- Example:
--
-- CREATE USER 'dbd-tipdi'@'172.24.114.42'
-- IDENTIFIED BY 'CHANGE_ME_TEAM_PASSWORD';
--
-- GRANT SELECT
-- ON dbd.*
-- TO 'dbd-tipdi'@'172.24.114.42';


-- ============================================================================
-- 3. CREATE A USER FOR ANOTHER TEAM
-- ============================================================================
-- Naming pattern:
--   dbd-<team>
--
-- Example:
--
-- CREATE USER 'dbd-dataeng'@'172.24.115.10'
-- IDENTIFIED BY 'CHANGE_ME_DATAENG_PASSWORD';
--
-- GRANT SELECT
-- ON dbd.*
-- TO 'dbd-dataeng'@'172.24.115.10';


-- ============================================================================
-- 4. COMMON MYSQL PRIVILEGES
-- ============================================================================
--
-- SELECT
--   Read/query rows.
--   Recommended default for normal DBD clients.
--
-- INSERT
--   Add new rows.
--
-- UPDATE
--   Modify existing rows.
--
-- DELETE
--   Delete existing rows.
--
-- CREATE
--   Create databases/tables.
--
-- ALTER
--   Change table structure.
--
-- DROP
--   Delete databases/tables.
--
-- INDEX
--   Create/drop indexes.
--
-- CREATE VIEW
--   Create SQL views.
--
-- SHOW VIEW
--   View definitions of SQL views.
--
-- CREATE TEMPORARY TABLES
--   Create temporary tables for the session.
--
-- LOCK TABLES
--   Explicitly lock tables.
--
-- EXECUTE
--   Execute stored procedures/functions.
--
-- TRIGGER
--   Create/drop triggers.
--
-- EVENT
--   Create/alter/drop MySQL scheduled events.
--
-- REFERENCES
--   Used for foreign-key reference privileges in applicable operations.
--
-- ALL PRIVILEGES
--   Grants nearly all privileges available at the selected scope.
--   DO NOT grant this to normal remote clients.
--
-- GRANT OPTION
--   Allows the account to grant its own privileges to other accounts.
--   DO NOT grant this to normal remote clients.


-- ============================================================================
-- 5. READY-TO-COPY GRANT PATTERNS
-- ============================================================================

-- Read-only client (recommended default):
--
-- GRANT SELECT
-- ON dbd.*
-- TO 'dbd-tipdi'@'172.24.114.41';


-- Read/write application account:
--
-- GRANT SELECT, INSERT, UPDATE, DELETE
-- ON dbd.*
-- TO 'dbd-app'@'<CLIENT_IP>';


-- Read + temporary analytical work:
--
-- GRANT SELECT, CREATE TEMPORARY TABLES
-- ON dbd.*
-- TO 'dbd-analytics'@'<CLIENT_IP>';


-- Schema-management account (privileged; use only when explicitly required):
--
-- GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP
-- ON dbd.*
-- TO 'dbd-schema'@'<CLIENT_IP>';


-- DANGEROUS: full database privileges.
-- Do not use for ordinary remote users.
--
-- GRANT ALL PRIVILEGES
-- ON dbd.*
-- TO 'dbd-admin'@'<CLIENT_IP>';


-- ============================================================================
-- 6. VERIFY USERS AND PRIVILEGES
-- ============================================================================

-- List DBD team/client accounts:
SELECT User, Host
FROM mysql.user
WHERE User LIKE 'dbd-%'
ORDER BY User, Host;


-- Check one account:
SHOW GRANTS FOR 'dbd-tipdi'@'172.24.114.41';


-- ============================================================================
-- 7. CHANGE A TEAM PASSWORD
-- ============================================================================
-- Because each username@host is a separate MySQL account, update every
-- approved IP for that team when rotating a shared team password.
--
-- ALTER USER 'dbd-tipdi'@'172.24.114.41'
-- IDENTIFIED BY 'NEW_TEAM_PASSWORD';
--
-- ALTER USER 'dbd-tipdi'@'172.24.114.42'
-- IDENTIFIED BY 'NEW_TEAM_PASSWORD';


-- ============================================================================
-- 8. REVOKE PRIVILEGES
-- ============================================================================

-- Remove SELECT while keeping the account:
--
-- REVOKE SELECT
-- ON dbd.*
-- FROM 'dbd-tipdi'@'172.24.114.41';


-- ============================================================================
-- 9. REMOVE A CLIENT MACHINE
-- ============================================================================

-- Remove only the account for the specified source IP:
--
-- DROP USER 'dbd-tipdi'@'172.24.114.41';


-- ============================================================================
-- 10. SECURITY NOTES
-- ============================================================================
-- Prefer:
--   'dbd-<team>'@'<specific-client-ip>'
--
-- Avoid:
--   'dbd-<team>'@'%'
--
-- '%' permits that username to authenticate from any source host that can
-- reach the MySQL server, subject to password/authentication requirements.
--
-- Keep collector/internal credentials such as dbd-data separate from remote
-- client/team credentials.
-- ============================================================================
