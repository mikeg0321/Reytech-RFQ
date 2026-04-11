"""
migrations.py — Lightweight schema migration framework for SQLite.
Sprint 5.2 (M3): Versioned migrations with rollback tracking.

Usage:
    from src.core.migrations import run_migrations
    run_migrations()  # Called at app startup
"""
import logging
from datetime import datetime

log = logging.getLogger("reytech.migrations")


def _get_db():
    from src.core.db import get_db
    return get_db()


def _ensure_migration_table(conn):
    """Create migration tracking table."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TEXT NOT NULL,
            checksum TEXT
        )
    """)


def _get_current_version(conn) -> int:
    """Get the highest applied migration version."""
    try:
        row = conn.execute(
            "SELECT MAX(version) as v FROM schema_migrations"
        ).fetchone()
        return row["v"] or 0 if row else 0
    except Exception:
        return 0


# ── Migration Definitions ────────────────────────────────────────────────────
# Each migration is (version, name, up_sql)
# Add new migrations at the end. NEVER modify existing migrations.

MIGRATIONS = [
    (1, "add_order_status_log", """
        CREATE TABLE IF NOT EXISTS order_status_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL,
            old_status TEXT,
            new_status TEXT NOT NULL,
            changed_at TEXT NOT NULL,
            changed_by TEXT DEFAULT 'system',
            notes TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_osl_order ON order_status_log(order_id);
        CREATE INDEX IF NOT EXISTS idx_osl_time ON order_status_log(changed_at);
    """),

    (2, "add_processed_emails", """
        CREATE TABLE IF NOT EXISTS processed_emails (
            uid TEXT PRIMARY KEY,
            inbox TEXT DEFAULT 'sales',
            processed_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_pe_inbox ON processed_emails(inbox);
    """),

    (3, "add_email_classifications", """
        CREATE TABLE IF NOT EXISTS email_classifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email_uid TEXT,
            subject TEXT,
            sender TEXT,
            classification TEXT,
            confidence REAL,
            scores TEXT,
            classified_at TEXT,
            needs_review INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_ec_review ON email_classifications(needs_review);
    """),

    (4, "add_backup_log", """
        CREATE TABLE IF NOT EXISTS backup_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            backup_at TEXT NOT NULL,
            file_path TEXT,
            size_bytes INTEGER,
            duration_sec REAL,
            status TEXT DEFAULT 'ok'
        );
    """),

    (5, "add_scheduler_heartbeats", """
        CREATE TABLE IF NOT EXISTS scheduler_heartbeats (
            job_name TEXT PRIMARY KEY,
            last_heartbeat TEXT,
            interval_sec INTEGER,
            status TEXT DEFAULT 'ok'
        );
    """),

    (6, "add_pdf_generation_log", """
        CREATE TABLE IF NOT EXISTS pdf_generation_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            template_type TEXT NOT NULL,
            template_version TEXT NOT NULL,
            document_id TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            generator TEXT,
            file_path TEXT,
            metadata TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_pdflog_doc ON pdf_generation_log(document_id);
        CREATE INDEX IF NOT EXISTS idx_pdflog_type ON pdf_generation_log(template_type);
    """),

    (7, "add_price_audit", """
        CREATE TABLE IF NOT EXISTS price_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            rfq_id TEXT,
            item_description TEXT NOT NULL,
            part_number TEXT,
            field_changed TEXT NOT NULL,
            old_value REAL,
            new_value REAL,
            source TEXT NOT NULL,
            actor TEXT DEFAULT 'system',
            notes TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_paudit_rfq ON price_audit(rfq_id);
        CREATE INDEX IF NOT EXISTS idx_paudit_desc ON price_audit(item_description);
        CREATE INDEX IF NOT EXISTS idx_paudit_ts ON price_audit(ts);
    """),

    (8, "add_price_checks_ship_to",
     "SELECT 1;"  # ship_to now in CREATE TABLE; this is a no-op for new DBs
     ),

    (9, "create_scprs_intelligence_tables", """
        CREATE TABLE IF NOT EXISTS scprs_awards (
            id TEXT PRIMARY KEY,
            po_number TEXT,
            agency TEXT,
            agency_code TEXT,
            vendor_name TEXT,
            vendor_code TEXT,
            award_date TEXT,
            fiscal_year TEXT,
            total_value REAL,
            item_count INTEGER,
            source TEXT DEFAULT 'scprs',
            tenant_id TEXT DEFAULT 'reytech',
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS scprs_results (
            id TEXT PRIMARY KEY,
            search_query TEXT,
            agency TEXT,
            result_json TEXT,
            pulled_at TEXT,
            tenant_id TEXT DEFAULT 'reytech'
        );

        CREATE TABLE IF NOT EXISTS vendor_intel (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor_name TEXT,
            vendor_code TEXT,
            agency TEXT,
            category TEXT,
            win_count INTEGER DEFAULT 0,
            total_value REAL DEFAULT 0,
            avg_price REAL,
            items_won TEXT,
            first_seen TEXT,
            last_seen TEXT,
            tenant_id TEXT DEFAULT 'reytech',
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS buyer_intel (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            buyer_name TEXT,
            buyer_email TEXT,
            agency TEXT,
            agency_code TEXT,
            items_purchased TEXT,
            categories TEXT,
            total_spend REAL DEFAULT 0,
            rfq_count INTEGER DEFAULT 0,
            last_purchase TEXT,
            contact_status TEXT DEFAULT 'unknown',
            tenant_id TEXT DEFAULT 'reytech',
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS competitors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor_name TEXT UNIQUE,
            vendor_code TEXT,
            primary_agencies TEXT,
            primary_categories TEXT,
            win_rate REAL,
            avg_margin_vs_reytech REAL,
            last_win TEXT,
            weakness_notes TEXT,
            tenant_id TEXT DEFAULT 'reytech',
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS won_quotes_kb (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_description TEXT,
            nsn TEXT,
            mfg_number TEXT,
            agency TEXT,
            winning_price REAL,
            winning_vendor TEXT,
            reytech_won INTEGER DEFAULT 0,
            reytech_price REAL,
            price_delta REAL,
            award_date TEXT,
            po_number TEXT,
            tenant_id TEXT DEFAULT 'reytech',
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_vendor_intel_name ON vendor_intel(vendor_name);
        CREATE INDEX IF NOT EXISTS idx_buyer_intel_email ON buyer_intel(buyer_email);
        CREATE INDEX IF NOT EXISTS idx_buyer_intel_agency ON buyer_intel(agency);
        CREATE INDEX IF NOT EXISTS idx_competitors_name ON competitors(vendor_name);
        CREATE INDEX IF NOT EXISTS idx_won_kb_desc ON won_quotes_kb(item_description);
        CREATE INDEX IF NOT EXISTS idx_won_kb_agency ON won_quotes_kb(agency);
        CREATE INDEX IF NOT EXISTS idx_scprs_awards_agency ON scprs_awards(agency);
    """),

    (10, "multi_state_multi_source_schema", """
        -- Add state + source_system columns to existing tables (safe: no-op if exists)
        -- Using separate statements because ALTER TABLE ADD COLUMN IF NOT EXISTS
        -- is not supported in SQLite — we rely on duplicate column error being caught

        CREATE TABLE IF NOT EXISTS procurement_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT UNIQUE,
            state TEXT,
            jurisdiction TEXT,
            base_url TEXT,
            auth_required INTEGER DEFAULT 0,
            scraper_class TEXT,
            last_pulled TEXT,
            pull_frequency_days INTEGER DEFAULT 7,
            status TEXT DEFAULT 'active',
            notes TEXT,
            tenant_id TEXT DEFAULT 'reytech'
        );

        CREATE TABLE IF NOT EXISTS agency_registry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agency_name TEXT,
            agency_code TEXT,
            state TEXT,
            jurisdiction TEXT,
            category TEXT,
            procurement_url TEXT,
            annual_spend_est REAL,
            reytech_customer INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1,
            tenant_id TEXT DEFAULT 'reytech',
            UNIQUE(agency_name, state)
        );

        CREATE TABLE IF NOT EXISTS harvest_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_system TEXT,
            state TEXT,
            agency TEXT,
            fiscal_year TEXT,
            pos_found INTEGER DEFAULT 0,
            lines_found INTEGER DEFAULT 0,
            reytech_wins INTEGER DEFAULT 0,
            errors TEXT,
            duration_seconds REAL,
            started_at TEXT,
            completed_at TEXT,
            tenant_id TEXT DEFAULT 'reytech'
        );

        CREATE INDEX IF NOT EXISTS idx_harvest_log_agency ON harvest_log(agency);
        CREATE INDEX IF NOT EXISTS idx_harvest_log_source ON harvest_log(source_system);
        CREATE INDEX IF NOT EXISTS idx_agency_reg_state ON agency_registry(state);
        CREATE INDEX IF NOT EXISTS idx_proc_sources_state ON procurement_sources(state);

        -- Seed California SCPRS as procurement source
        INSERT OR IGNORE INTO procurement_sources
            (source_name, state, jurisdiction, base_url, auth_required,
             scraper_class, pull_frequency_days, status)
        VALUES
            ('scprs', 'CA', 'state',
             'https://caleprocure.ca.gov/pages/SCPRSSearch/scprs-search.aspx',
             0, 'FiscalSession', 7, 'active');

        INSERT OR IGNORE INTO procurement_sources
            (source_name, state, jurisdiction, base_url, auth_required,
             scraper_class, pull_frequency_days, status)
        VALUES
            ('usaspending', 'federal', 'federal',
             'https://api.usaspending.gov/api/v2',
             0, 'USASpendingAgent', 7, 'planned');
    """),

    (11, "connector_registry", """
        CREATE TABLE IF NOT EXISTS connectors (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            jurisdiction_level TEXT,
            state TEXT,
            base_url TEXT,
            connector_class TEXT,
            auth_type TEXT DEFAULT 'none',
            pull_frequency_hours INTEGER DEFAULT 168,
            last_pulled_at TEXT,
            last_health_grade TEXT,
            status TEXT DEFAULT 'active',
            priority INTEGER DEFAULT 5,
            record_count INTEGER DEFAULT 0,
            notes TEXT,
            tenant_id TEXT DEFAULT 'reytech',
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_connectors_status ON connectors(status);

        INSERT OR IGNORE INTO connectors (id, name, jurisdiction_level, state, base_url, connector_class, auth_type, pull_frequency_hours, status, priority, notes) VALUES
            ('ca_scprs', 'California SCPRS', 'state', 'CA', 'https://caleprocure.ca.gov', 'src.agents.connectors.ca_scprs.CASCPRSConnector', 'session', 168, 'active', 1, NULL),
            ('federal_usaspending', 'USASpending.gov', 'federal', 'federal', 'https://api.usaspending.gov/api/v2', 'src.agents.connectors.federal_usaspending.USASpendingConnector', 'none', 168, 'active', 2, 'Filtered to CA place-of-performance + Reytech NAICS codes'),
            ('federal_sam', 'SAM.gov Opportunities', 'federal', 'federal', 'https://api.sam.gov', NULL, 'apikey', 168, 'scaffolded', 3, 'Needs SAM.gov API key to activate'),
            ('tx_esbd', 'Texas ESBD', 'state', 'TX', 'https://www.txsmartbuy.com/esbd', NULL, 'none', 168, 'scaffolded', 5, 'Activate when Reytech expands to TX'),
            ('fl_mfmp', 'Florida MFMP', 'state', 'FL', 'https://vendor.myfloridamarketplace.com', NULL, 'none', 168, 'scaffolded', 5, 'Activate when Reytech expands to FL'),
            ('ny_ogs', 'New York OGS', 'state', 'NY', 'https://www.ogs.ny.gov/procurement', NULL, 'none', 168, 'scaffolded', 5, NULL),
            ('wa_webs', 'Washington WEBS', 'state', 'WA', 'https://pr-webs-customer.des.wa.gov', NULL, 'none', 168, 'scaffolded', 5, NULL),
            ('az_spo', 'Arizona SPO', 'state', 'AZ', 'https://spo.az.gov/procurement', NULL, 'none', 168, 'scaffolded', 5, NULL),
            ('ca_demandstar', 'DemandStar CA Local', 'county', 'CA', 'https://network.demandstar.com', NULL, 'none', 168, 'scaffolded', 4, 'Covers 500+ CA local agencies'),
            ('ca_bonfire', 'Bonfire CA Local', 'county', 'CA', 'https://gobonfire.com', NULL, 'none', 168, 'scaffolded', 4, 'Hospital and school districts');
    """),

    (12, "tenant_profiles", """
        CREATE TABLE IF NOT EXISTS tenant_profiles (
            tenant_id TEXT PRIMARY KEY,
            legal_name TEXT NOT NULL,
            dba_names TEXT,
            entity_number TEXT,
            entity_type TEXT,
            state_of_formation TEXT,
            formation_date TEXT,
            status TEXT DEFAULT 'active',
            website TEXT,
            phone TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            vendor_search_names TEXT,
            vendor_codes TEXT,
            certifications TEXT,
            naics_codes TEXT,
            statement_of_info_due TEXT,
            licenses_json TEXT,
            notify_phone TEXT,
            notify_email TEXT,
            base_url TEXT,
            api_key_hash TEXT,
            approval_threshold REAL DEFAULT 5000,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        INSERT OR IGNORE INTO tenant_profiles (
            tenant_id, legal_name, dba_names, entity_number,
            entity_type, state_of_formation, formation_date,
            website, phone, address, city, state, zip,
            vendor_search_names, certifications, naics_codes,
            statement_of_info_due
        ) VALUES (
            'reytech',
            'REYTECH INC',
            '["Reytech Inc.", "Rey Tech Inc", "Reytech"]',
            '3799353',
            'S-Corp', 'CA', '2015-06-18',
            'https://www.reytechinc.com',
            '949-229-1575',
            '30 Carnoustie Way', 'Trabuco Canyon', 'CA', '92679',
            '["REYTECH INC","reytech inc.","reytech inc","reytech","rey tech inc","rey tech"]',
            '[{"type":"MB","number":"2002605","state":"CA","expiry":null,"active":true},{"type":"SB","number":"2002605","state":"CA","expiry":null,"active":true},{"type":"SB-PW","number":"2002605","state":"CA","expiry":null,"active":true},{"type":"DVBE","number":"2002605","state":"CA","expiry":null,"active":true,"notes":"Service-Disabled Veteran Business Enterprise"},{"type":"SDVOB","number":"221449","state":"NY","expiry":null,"active":true,"notes":"NY Service-Disabled Veteran-Owned Business"},{"type":"DBE","number":"44511","jurisdiction":"DOT","expiry":null,"active":true,"notes":"Disadvantaged Business Enterprise"}]',
            '["339112","339113","423450","423490","339920"]',
            '2024-06-30'
        );
    """),
    # Migration 13: handled programmatically (ALTER TABLE doesn't support IF NOT EXISTS)
    # Migration 14: intelligence layer tables + columns (programmatic below)
    (14, "intelligence_layer_tables", """
        CREATE TABLE IF NOT EXISTS parsed_documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            original_path TEXT,
            doc_type TEXT DEFAULT 'unknown',
            parsed_markdown TEXT,
            parsed_tables_json TEXT,
            metadata_json TEXT,
            linked_rfq_id TEXT,
            linked_pc_id TEXT,
            uploaded_by TEXT DEFAULT 'system',
            uploaded_at TEXT DEFAULT (datetime('now')),
            parse_duration_ms INTEGER,
            page_count INTEGER,
            status TEXT DEFAULT 'parsed'
        );
        CREATE INDEX IF NOT EXISTS idx_parsed_docs_rfq ON parsed_documents(linked_rfq_id);
        CREATE INDEX IF NOT EXISTS idx_parsed_docs_type ON parsed_documents(doc_type);

        CREATE TABLE IF NOT EXISTS nl_query_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_text TEXT NOT NULL,
            generated_sql TEXT,
            result_count INTEGER,
            duration_ms INTEGER,
            success INTEGER DEFAULT 1,
            error TEXT,
            queried_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS compliance_matrices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rfq_id TEXT NOT NULL,
            source_doc_id INTEGER,
            requirements_json TEXT,
            met_count INTEGER DEFAULT 0,
            total_count INTEGER DEFAULT 0,
            score REAL DEFAULT 0,
            extracted_at TEXT DEFAULT (datetime('now')),
            extraction_method TEXT DEFAULT 'claude',
            source_pdf_path TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_compliance_rfq ON compliance_matrices(rfq_id);
    """),
    (15, "platform_upgrade_tables", """
        CREATE TABLE IF NOT EXISTS bid_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT, pc_id TEXT NOT NULL,
            catalog_coverage REAL DEFAULT 0, win_history_score REAL DEFAULT 0,
            margin_potential REAL DEFAULT 0, complexity_score REAL DEFAULT 0,
            total_score REAL DEFAULT 0, recommendation TEXT DEFAULT 'review',
            scored_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_bid_scores_pc ON bid_scores(pc_id);
        CREATE TABLE IF NOT EXISTS agency_compliance_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT, agency_key TEXT NOT NULL,
            requirement_text TEXT NOT NULL, category TEXT DEFAULT 'other',
            severity TEXT DEFAULT 'required', form_id TEXT DEFAULT '',
            active INTEGER DEFAULT 1, created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_act_agency ON agency_compliance_templates(agency_key);
    """),
]


def _run_migration_15(conn):
    """Platform upgrade — quote expiry + contact address columns."""
    def _add_col(table, col, coltype):
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
        except Exception:
            pass
    _add_col("quotes", "expires_at", "TEXT DEFAULT ''")
    _add_col("quotes", "expiry_notified", "INTEGER DEFAULT 0")
    _add_col("contacts", "address", "TEXT DEFAULT ''")
    _add_col("contacts", "city", "TEXT DEFAULT ''")
    _add_col("contacts", "state", "TEXT DEFAULT 'CA'")
    _add_col("contacts", "zip", "TEXT DEFAULT ''")
    _add_col("contacts", "ship_to_default", "TEXT DEFAULT ''")


def _run_migration_14(conn):
    """Intelligence layer — add UNSPSC + country_of_origin columns to catalog tables."""
    def _add_col(table, col, coltype):
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
        except Exception:
            pass  # Column already exists

    for table in ("products", "product_catalog"):
        _add_col(table, "unspsc_code", "TEXT DEFAULT ''")
        _add_col(table, "unspsc_description", "TEXT DEFAULT ''")
        _add_col(table, "country_of_origin", "TEXT DEFAULT ''")
        _add_col(table, "taa_compliant", "INTEGER DEFAULT -1")


def _run_migration_13(conn):
    """Award intelligence enhancements — idempotent column additions."""
    # Add columns only if they don't exist
    def _add_col(table, col, coltype):
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
        except Exception:
            pass  # Column already exists

    _add_col("competitor_intel", "loss_reason_class", "TEXT DEFAULT ''")
    _add_col("competitor_intel", "our_cost", "REAL DEFAULT 0")
    _add_col("competitor_intel", "our_margin_pct", "REAL DEFAULT 0")
    _add_col("competitor_intel", "margin_too_high", "INTEGER DEFAULT 0")
    _add_col("competitor_intel", "items_detail", "TEXT")
    _add_col("competitor_intel", "category", "TEXT DEFAULT ''")
    _add_col("award_check_queue", "phase", "TEXT DEFAULT 'daily'")
    _add_col("award_check_queue", "check_count", "INTEGER DEFAULT 0")
    _add_col("award_check_queue", "last_checked", "TEXT DEFAULT ''")
    _add_col("award_check_queue", "next_check", "TEXT DEFAULT ''")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS loss_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT, detected_at TEXT NOT NULL,
            pattern_type TEXT NOT NULL, category TEXT DEFAULT '', agency TEXT DEFAULT '',
            competitor TEXT DEFAULT '', description TEXT NOT NULL, severity TEXT DEFAULT 'info',
            recommendation TEXT DEFAULT '', data_json TEXT DEFAULT '{}',
            acknowledged INTEGER DEFAULT 0, acknowledged_at TEXT DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_lp_type ON loss_patterns(pattern_type);
        CREATE INDEX IF NOT EXISTS idx_lp_severity ON loss_patterns(severity);
        CREATE INDEX IF NOT EXISTS idx_lp_unack ON loss_patterns(acknowledged);
        CREATE INDEX IF NOT EXISTS idx_ci_loss_class ON competitor_intel(loss_reason_class);
        CREATE INDEX IF NOT EXISTS idx_ci_margin ON competitor_intel(margin_too_high);
    """)


def run_migrations():
    """Apply any pending migrations. Safe to call on every startup."""
    try:
        with _get_db() as conn:
            _ensure_migration_table(conn)
            current = _get_current_version(conn)

            applied = 0
            for version, name, sql in MIGRATIONS:
                if version <= current:
                    continue

                try:
                    conn.executescript(sql)
                    conn.execute(
                        "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?,?,?)",
                        (version, name, datetime.now().isoformat())
                    )
                    applied += 1
                    log.info("Migration %d applied: %s", version, name)
                except Exception as e:
                    log.error("Migration %d (%s) FAILED: %s", version, name, e)
                    raise

            # Programmatic migrations (handle ALTER TABLE idempotently)
            if current < 13:
                try:
                    _run_migration_13(conn)
                    conn.execute(
                        "INSERT OR REPLACE INTO schema_migrations (version, name, applied_at) VALUES (?,?,?)",
                        (13, "award_intelligence_enhancements", datetime.now().isoformat())
                    )
                    applied += 1
                    log.info("Migration 13 applied: award_intelligence_enhancements (programmatic)")
                except Exception as e:
                    log.warning("Migration 13 partial: %s (non-fatal)", e)

            if current < 14:
                try:
                    _run_migration_14(conn)
                    conn.execute(
                        "INSERT OR REPLACE INTO schema_migrations (version, name, applied_at) VALUES (?,?,?)",
                        (14, "intelligence_layer_columns", datetime.now().isoformat())
                    )
                    applied += 1
                    log.info("Migration 14 applied: intelligence_layer_columns (programmatic)")
                except Exception as e:
                    log.warning("Migration 14 partial: %s (non-fatal)", e)

            if current < 15:
                try:
                    _run_migration_15(conn)
                    conn.execute(
                        "INSERT OR REPLACE INTO schema_migrations (version, name, applied_at) VALUES (?,?,?)",
                        (15, "platform_upgrade_columns", datetime.now().isoformat())
                    )
                    applied += 1
                    log.info("Migration 15 applied: platform_upgrade_columns (programmatic)")
                except Exception as e:
                    log.warning("Migration 15 partial: %s (non-fatal)", e)

            if applied:
                log.info("Applied %d migration(s). Schema at version %d",
                         applied, version)
            else:
                log.debug("Schema up to date at version %d", current)

            return {"ok": True, "version": max(v for v, _, _ in MIGRATIONS),
                    "applied": applied}

    except Exception as e:
        log.error("Migration runner failed: %s", e)
        return {"ok": False, "error": str(e)}


def get_migration_status() -> dict:
    """Return current schema version and applied migrations."""
    try:
        with _get_db() as conn:
            _ensure_migration_table(conn)
            rows = conn.execute(
                "SELECT version, name, applied_at FROM schema_migrations ORDER BY version"
            ).fetchall()
            current = _get_current_version(conn)
            latest = max(v for v, _, _ in MIGRATIONS) if MIGRATIONS else 0
            pending = [{"version": v, "name": n}
                       for v, n, _ in MIGRATIONS if v > current]
            return {
                "current_version": current,
                "latest_available": latest,
                "applied": [dict(r) for r in rows],
                "pending": pending,
                "up_to_date": current >= latest,
            }
    except Exception as e:
        return {"error": str(e)}
