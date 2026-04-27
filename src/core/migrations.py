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

    (16, "performance_indexes_and_cleanup", """
        -- Performance indexes on high-query tables
        CREATE INDEX IF NOT EXISTS idx_quotes_quote_number ON quotes(quote_number);
        CREATE INDEX IF NOT EXISTS idx_quotes_agency ON quotes(agency);
        CREATE INDEX IF NOT EXISTS idx_quotes_status ON quotes(status);
        CREATE INDEX IF NOT EXISTS idx_quotes_created ON quotes(created_at);
        CREATE INDEX IF NOT EXISTS idx_pc_status ON price_checks(status);
        CREATE INDEX IF NOT EXISTS idx_pc_agency ON price_checks(agency);
        CREATE INDEX IF NOT EXISTS idx_rfqs_status ON rfqs(status);
        CREATE INDEX IF NOT EXISTS idx_rfqs_received ON rfqs(received_at);
        CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
        CREATE INDEX IF NOT EXISTS idx_orders_po ON orders(po_number);
        CREATE INDEX IF NOT EXISTS idx_scprs_po_lines_desc ON scprs_po_lines(description);
        CREATE INDEX IF NOT EXISTS idx_scprs_po_lines_po ON scprs_po_lines(po_number);
        CREATE INDEX IF NOT EXISTS idx_supplier_costs_supplier ON supplier_costs(supplier);
        CREATE INDEX IF NOT EXISTS idx_supplier_costs_desc ON supplier_costs(description);
        CREATE INDEX IF NOT EXISTS idx_won_quotes_desc ON won_quotes(description);
        CREATE INDEX IF NOT EXISTS idx_email_sent_log_sent ON email_sent_log(sent_at);
        CREATE INDEX IF NOT EXISTS idx_parse_gaps_rfq ON parse_gaps(rfq_id);

        -- Drop unused tables
        DROP TABLE IF EXISTS contract_violations;
        DROP TABLE IF EXISTS sent_quote_tracker;
        DROP TABLE IF EXISTS intel_pulls;
        DROP TABLE IF EXISTS rfq_store;
    """),

    (17, "fk_validation_triggers", """
        -- Referential integrity triggers (SQLite cannot add FKs via ALTER TABLE)

        -- order_audit_log.order_id must reference orders.id
        CREATE TRIGGER IF NOT EXISTS fk_order_audit_log_order
        BEFORE INSERT ON order_audit_log FOR EACH ROW BEGIN
          SELECT RAISE(ABORT, 'FK violation: order_audit_log.order_id not in orders')
          WHERE NEW.order_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM orders WHERE id = NEW.order_id);
        END;

        -- competitor_intel: pc_id should reference price_checks.id
        CREATE TRIGGER IF NOT EXISTS fk_competitor_intel_pc
        BEFORE INSERT ON competitor_intel FOR EACH ROW BEGIN
          SELECT RAISE(ABORT, 'FK violation: competitor_intel.pc_id not in price_checks')
          WHERE NEW.pc_id IS NOT NULL AND NEW.pc_id != '' AND NOT EXISTS (SELECT 1 FROM price_checks WHERE id = NEW.pc_id);
        END;

        -- match_feedback: pc_id should reference price_checks.id
        CREATE TRIGGER IF NOT EXISTS fk_match_feedback_pc
        BEFORE INSERT ON match_feedback FOR EACH ROW BEGIN
          SELECT RAISE(ABORT, 'FK violation: match_feedback.pc_id not in price_checks')
          WHERE NEW.pc_id IS NOT NULL AND NEW.pc_id != '' AND NOT EXISTS (SELECT 1 FROM price_checks WHERE id = NEW.pc_id);
        END;

        -- sent_documents: pc_id should reference price_checks.id
        CREATE TRIGGER IF NOT EXISTS fk_sent_documents_pc
        BEFORE INSERT ON sent_documents FOR EACH ROW BEGIN
          SELECT RAISE(ABORT, 'FK violation: sent_documents.pc_id not in price_checks')
          WHERE NEW.pc_id IS NOT NULL AND NEW.pc_id != '' AND NOT EXISTS (SELECT 1 FROM price_checks WHERE id = NEW.pc_id);
        END;

        -- recommendation_audit: pc_id should reference price_checks.id
        CREATE TRIGGER IF NOT EXISTS fk_recommendation_audit_pc
        BEFORE INSERT ON recommendation_audit FOR EACH ROW BEGIN
          SELECT RAISE(ABORT, 'FK violation: recommendation_audit.pc_id not in price_checks')
          WHERE NEW.pc_id IS NOT NULL AND NEW.pc_id != '' AND NOT EXISTS (SELECT 1 FROM price_checks WHERE id = NEW.pc_id);
        END;

        -- vendor_orders: quote_number should reference quotes.quote_number
        CREATE TRIGGER IF NOT EXISTS fk_vendor_orders_quote
        BEFORE INSERT ON vendor_orders FOR EACH ROW BEGIN
          SELECT RAISE(ABORT, 'FK violation: vendor_orders.quote_number not in quotes')
          WHERE NEW.quote_number IS NOT NULL AND NEW.quote_number != '' AND NOT EXISTS (SELECT 1 FROM quotes WHERE quote_number = NEW.quote_number);
        END;
    """),

    (18, "api_usage_tracking", """
        CREATE TABLE IF NOT EXISTS api_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            service TEXT NOT NULL,
            agent TEXT DEFAULT '',
            pc_id TEXT DEFAULT '',
            call_date TEXT NOT NULL DEFAULT (date('now')),
            called_at TEXT NOT NULL DEFAULT (datetime('now')),
            request_tokens INTEGER DEFAULT 0,
            response_tokens INTEGER DEFAULT 0,
            estimated_cost REAL DEFAULT 0,
            response_time_ms INTEGER DEFAULT 0,
            error TEXT DEFAULT '',
            model TEXT DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_api_usage_service ON api_usage(service);
        CREATE INDEX IF NOT EXISTS idx_api_usage_date ON api_usage(call_date);
        CREATE INDEX IF NOT EXISTS idx_api_usage_agent ON api_usage(agent);

        CREATE TABLE IF NOT EXISTS api_quotas (
            service TEXT PRIMARY KEY,
            daily_limit_dollars REAL DEFAULT 1.0,
            monthly_limit_dollars REAL DEFAULT 20.0,
            per_pc_limit INTEGER DEFAULT 10,
            enabled INTEGER DEFAULT 1,
            updated_at TEXT DEFAULT (datetime('now'))
        );

        INSERT OR IGNORE INTO api_quotas (service, daily_limit_dollars, monthly_limit_dollars, per_pc_limit)
        VALUES ('grok', 1.00, 20.00, 8);
        INSERT OR IGNORE INTO api_quotas (service, daily_limit_dollars, monthly_limit_dollars, per_pc_limit)
        VALUES ('claude', 0.50, 10.00, 4);
    """),

    (19, "workflow_runs_table", """
        CREATE TABLE IF NOT EXISTS workflow_runs (
            id TEXT PRIMARY KEY,
            task_type TEXT NOT NULL,
            status TEXT DEFAULT 'running',
            phase TEXT DEFAULT '',
            progress TEXT DEFAULT '',
            running INTEGER DEFAULT 1,
            items_done INTEGER DEFAULT 0,
            items_total INTEGER DEFAULT 0,
            results_count INTEGER DEFAULT 0,
            errors_json TEXT DEFAULT '[]',
            started_at TEXT DEFAULT (datetime('now')),
            finished_at TEXT DEFAULT '',
            last_updated TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_wf_type ON workflow_runs(task_type);
        CREATE INDEX IF NOT EXISTS idx_wf_running ON workflow_runs(running);
    """),

    (20, "json_validation_triggers", """
        -- BEFORE INSERT triggers: reject invalid JSON on critical columns

        CREATE TRIGGER IF NOT EXISTS validate_quotes_items_insert
        BEFORE INSERT ON quotes FOR EACH ROW
        WHEN NEW.items_detail IS NOT NULL AND NEW.items_detail != ''
        BEGIN
          SELECT RAISE(ABORT, 'Invalid JSON in quotes.items_detail')
          WHERE NOT json_valid(NEW.items_detail);
        END;

        CREATE TRIGGER IF NOT EXISTS validate_pc_items_insert
        BEFORE INSERT ON price_checks FOR EACH ROW
        WHEN NEW.items IS NOT NULL AND NEW.items != ''
        BEGIN
          SELECT RAISE(ABORT, 'Invalid JSON in price_checks.items')
          WHERE NOT json_valid(NEW.items);
        END;

        CREATE TRIGGER IF NOT EXISTS validate_rfq_items_insert
        BEFORE INSERT ON rfqs FOR EACH ROW
        WHEN NEW.items IS NOT NULL AND NEW.items != ''
        BEGIN
          SELECT RAISE(ABORT, 'Invalid JSON in rfqs.items')
          WHERE NOT json_valid(NEW.items);
        END;

        CREATE TRIGGER IF NOT EXISTS validate_order_items_insert
        BEFORE INSERT ON orders FOR EACH ROW
        WHEN NEW.items IS NOT NULL AND NEW.items != ''
        BEGIN
          SELECT RAISE(ABORT, 'Invalid JSON in orders.items')
          WHERE NOT json_valid(NEW.items);
        END;

        CREATE TRIGGER IF NOT EXISTS validate_vo_items_insert
        BEFORE INSERT ON vendor_orders FOR EACH ROW
        WHEN NEW.items_json IS NOT NULL AND NEW.items_json != ''
        BEGIN
          SELECT RAISE(ABORT, 'Invalid JSON in vendor_orders.items_json')
          WHERE NOT json_valid(NEW.items_json);
        END;

        CREATE TRIGGER IF NOT EXISTS validate_wf_errors_insert
        BEFORE INSERT ON workflow_runs FOR EACH ROW
        WHEN NEW.errors_json IS NOT NULL AND NEW.errors_json != ''
        BEGIN
          SELECT RAISE(ABORT, 'Invalid JSON in workflow_runs.errors_json')
          WHERE NOT json_valid(NEW.errors_json);
        END;

        -- BEFORE UPDATE triggers: same validation on updates

        CREATE TRIGGER IF NOT EXISTS validate_quotes_items_update
        BEFORE UPDATE ON quotes FOR EACH ROW
        WHEN NEW.items_detail IS NOT NULL AND NEW.items_detail != ''
        BEGIN
          SELECT RAISE(ABORT, 'Invalid JSON in quotes.items_detail')
          WHERE NOT json_valid(NEW.items_detail);
        END;

        CREATE TRIGGER IF NOT EXISTS validate_pc_items_update
        BEFORE UPDATE ON price_checks FOR EACH ROW
        WHEN NEW.items IS NOT NULL AND NEW.items != ''
        BEGIN
          SELECT RAISE(ABORT, 'Invalid JSON in price_checks.items')
          WHERE NOT json_valid(NEW.items);
        END;

        CREATE TRIGGER IF NOT EXISTS validate_rfq_items_update
        BEFORE UPDATE ON rfqs FOR EACH ROW
        WHEN NEW.items IS NOT NULL AND NEW.items != ''
        BEGIN
          SELECT RAISE(ABORT, 'Invalid JSON in rfqs.items')
          WHERE NOT json_valid(NEW.items);
        END;

        CREATE TRIGGER IF NOT EXISTS validate_order_items_update
        BEFORE UPDATE ON orders FOR EACH ROW
        WHEN NEW.items IS NOT NULL AND NEW.items != ''
        BEGIN
          SELECT RAISE(ABORT, 'Invalid JSON in orders.items')
          WHERE NOT json_valid(NEW.items);
        END;

        CREATE TRIGGER IF NOT EXISTS validate_vo_items_update
        BEFORE UPDATE ON vendor_orders FOR EACH ROW
        WHEN NEW.items_json IS NOT NULL AND NEW.items_json != ''
        BEGIN
          SELECT RAISE(ABORT, 'Invalid JSON in vendor_orders.items_json')
          WHERE NOT json_valid(NEW.items_json);
        END;

        CREATE TRIGGER IF NOT EXISTS validate_wf_errors_update
        BEFORE UPDATE ON workflow_runs FOR EACH ROW
        WHEN NEW.errors_json IS NOT NULL AND NEW.errors_json != ''
        BEGIN
          SELECT RAISE(ABORT, 'Invalid JSON in workflow_runs.errors_json')
          WHERE NOT json_valid(NEW.errors_json);
        END;
    """),

    (21, "quote_audit_log", """
        CREATE TABLE IF NOT EXISTS quote_audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quote_doc_id TEXT NOT NULL,
            doc_type TEXT NOT NULL,
            agency_key TEXT DEFAULT '',
            stage_from TEXT DEFAULT '',
            stage_to TEXT NOT NULL,
            outcome TEXT NOT NULL,
            reasons_json TEXT DEFAULT '[]',
            actor TEXT DEFAULT 'system',
            at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_qal_doc ON quote_audit_log(quote_doc_id);
        CREATE INDEX IF NOT EXISTS idx_qal_agency ON quote_audit_log(agency_key);
        CREATE INDEX IF NOT EXISTS idx_qal_outcome ON quote_audit_log(outcome);
        CREATE INDEX IF NOT EXISTS idx_qal_at ON quote_audit_log(at);

        CREATE TRIGGER IF NOT EXISTS validate_qal_reasons_insert
        BEFORE INSERT ON quote_audit_log FOR EACH ROW
        WHEN NEW.reasons_json IS NOT NULL AND NEW.reasons_json != ''
        BEGIN
          SELECT RAISE(ABORT, 'Invalid JSON in quote_audit_log.reasons_json')
          WHERE NOT json_valid(NEW.reasons_json);
        END;
    """),

    # Programmatic — see _run_migration_22.
    (22, "scprs_po_lines_dedup_and_unique_index", "SELECT 1;"),

    # Programmatic — see _run_migration_23.
    (23, "scprs_tables_add_is_test", "SELECT 1;"),

    (24, "agency_vendor_registry", """
        CREATE TABLE IF NOT EXISTS agency_vendor_registry (
            dept_code                  TEXT PRIMARY KEY,
            status                     TEXT NOT NULL DEFAULT 'unknown',
            confirmed_at               TEXT DEFAULT '',
            expires_at                 TEXT DEFAULT '',
            portal_url                 TEXT DEFAULT '',
            procurement_officer_name   TEXT DEFAULT '',
            procurement_officer_email  TEXT DEFAULT '',
            procurement_officer_phone  TEXT DEFAULT '',
            vendor_id_at_agency        TEXT DEFAULT '',
            categories_json            TEXT DEFAULT '[]',
            notes                      TEXT DEFAULT '',
            source                     TEXT DEFAULT 'operator',
            updated_by                 TEXT DEFAULT '',
            is_test                    INTEGER NOT NULL DEFAULT 0,
            created_at                 TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at                 TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_avr_status ON agency_vendor_registry(status);
        CREATE INDEX IF NOT EXISTS idx_avr_is_test ON agency_vendor_registry(is_test);
    """),

    (25, "outreach_credit_shown", """
        CREATE TABLE IF NOT EXISTS outreach_credit_shown (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            prospect_dept_code TEXT NOT NULL,
            credit_po_number   TEXT NOT NULL,
            credit_dept_code   TEXT,
            credit_category    TEXT,
            match_type         TEXT,
            shown_at           TEXT NOT NULL DEFAULT (datetime('now')),
            is_test            INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_credit_shown_prospect ON outreach_credit_shown(prospect_dept_code);
        CREATE INDEX IF NOT EXISTS idx_credit_shown_po ON outreach_credit_shown(credit_po_number);
        CREATE INDEX IF NOT EXISTS idx_credit_shown_at ON outreach_credit_shown(shown_at);
    """),

    (26, "reytech_certifications", """
        CREATE TABLE IF NOT EXISTS reytech_certifications (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            cert_type    TEXT NOT NULL UNIQUE,
            cert_number  TEXT DEFAULT '',
            issue_date   TEXT DEFAULT '',
            expires_at   TEXT DEFAULT '',
            renewal_url  TEXT DEFAULT '',
            notes        TEXT DEFAULT '',
            is_active    INTEGER NOT NULL DEFAULT 1,
            is_test      INTEGER NOT NULL DEFAULT 0,
            created_at   TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at   TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_cert_active ON reytech_certifications(is_active);
        CREATE INDEX IF NOT EXISTS idx_cert_expires ON reytech_certifications(expires_at);
    """),

    (27, "bid_memory", """
        CREATE TABLE IF NOT EXISTS bid_memory (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            rfq_id             TEXT UNIQUE,
            received_at        TEXT DEFAULT '',
            dept_code          TEXT NOT NULL,
            dept_name          TEXT DEFAULT '',
            category           TEXT DEFAULT '',
            summary_description TEXT DEFAULT '',
            our_status         TEXT DEFAULT 'received',
            our_bid_amount     REAL DEFAULT 0,
            our_bid_per_unit   REAL DEFAULT 0,
            outcome            TEXT DEFAULT 'pending',
            winning_supplier   TEXT DEFAULT '',
            winning_price      REAL DEFAULT 0,
            award_date         TEXT DEFAULT '',
            contract_end_date  TEXT DEFAULT '',
            notes              TEXT DEFAULT '',
            source             TEXT DEFAULT 'operator',
            updated_by         TEXT DEFAULT '',
            is_test            INTEGER NOT NULL DEFAULT 0,
            created_at         TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at         TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_bidmem_dept ON bid_memory(dept_code);
        CREATE INDEX IF NOT EXISTS idx_bidmem_outcome ON bid_memory(outcome);
        CREATE INDEX IF NOT EXISTS idx_bidmem_received ON bid_memory(received_at);
        CREATE INDEX IF NOT EXISTS idx_bidmem_contract_end ON bid_memory(contract_end_date);
    """),

    # Programmatic — see _run_migration_28.
    (28, "registration_gap_agent_tables", "SELECT 1;"),

    (29, "drop_award_check_queue_vestigial", """
        -- Remove the orphan award_check_queue table. Written by
        -- post_send_pipeline.on_quote_sent() since 2026-03-16 but never read —
        -- the consumer Mike intended (adaptive queue-driven checker) was
        -- superseded 2 weeks later by award_tracker.py's direct-iteration
        -- design that calls scprs_schedule.should_check_record per-row at
        -- read-time. Same schedule logic, different consumer pattern, queue
        -- write left orphan. See DATA_ARCHITECTURE_MAP §7 S7.
        --
        -- Idempotent: prod has 32 orphan rows from 2026-03-23 through
        -- 2026-04-24 which this drops. Fresh installs no-op. The legacy
        -- ADD COLUMN calls for phase/check_count/last_checked/next_check
        -- in _run_migration_13 already swallow "no such table" via try/except,
        -- so they become silent no-ops on previously-migrated DBs after this.
        DROP TABLE IF EXISTS award_check_queue;
    """),

    (30, "supplier_skus_cross_reference", """
        -- Phase 1.7 of PLAN_ONCE_AND_FOR_ALL.md (2026-04-25). Generic
        -- supplier-SKU → MFG# cross-reference. Powers reverse lookup when
        -- a buyer quotes a supplier SKU (e.g. McKesson item #1041721) and
        -- we need the manufacturer part number (e.g. Mueller #64179) to
        -- match our catalog and enrich pricing.
        --
        -- Source-of-truth notes:
        --   - This table is enrichment data, NOT cost. McKesson costs are
        --     not exposed in the source CSV. The pricing oracle still
        --     looks up cost from product_catalog / web scraping.
        --   - One row per (supplier, supplier_sku) pair. Duplicates blocked
        --     by UNIQUE INDEX so re-importing the same CSV is idempotent.
        CREATE TABLE IF NOT EXISTS supplier_skus (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            supplier TEXT NOT NULL,
            supplier_sku TEXT NOT NULL,
            mfg_number TEXT,
            description TEXT,
            imported_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_supplier_skus_unique
            ON supplier_skus(supplier, supplier_sku);
        CREATE INDEX IF NOT EXISTS idx_supplier_skus_mfg
            ON supplier_skus(mfg_number);
    """),

    (32, "cost_alerts", """
        -- Phase 4.3 of PLAN_ONCE_AND_FOR_ALL.md (2026-04-26).
        -- Catalog-cost change detector. Each row = one detected delta
        -- between the prior and most-recent unit_price for an item from
        -- a non-reference source (amazon/grainger/manual). Operator
        -- triages: dismiss (false alarm), apply (update catalog).
        --
        -- Fed by scripts/scan_cost_alerts.py + POST /api/admin/scan-cost-alerts.
        -- Surfaced on /health/quoting via GET /api/admin/cost-alerts.
        CREATE TABLE IF NOT EXISTS cost_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mfg_number TEXT,
            description TEXT,
            source TEXT,
            prior_price REAL,
            new_price REAL,
            delta_pct REAL,
            prior_found_at TEXT,
            new_found_at TEXT,
            detected_at TEXT NOT NULL DEFAULT (datetime('now')),
            status TEXT NOT NULL DEFAULT 'pending',
            agency TEXT,
            quote_number TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_cost_alerts_status
            ON cost_alerts(status);
        CREATE INDEX IF NOT EXISTS idx_cost_alerts_mfg
            ON cost_alerts(mfg_number);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_cost_alerts_dedup
            ON cost_alerts(mfg_number, source, new_found_at);
    """),

    (31, "scprs_reytech_wins", """
        -- Phase 0.7d (2026-04-25): Mike's SCPRS HTML export of all
        -- Reytech-won POs since 2022. Imported via scripts/import_scprs_
        -- reytech_wins.py. Joins against quotes table to mark which
        -- QuoteWerks-imported quotes actually won.
        --
        -- One row per won SCPRS PO. items_json stores the per-line
        -- item descriptions extracted from the export. Re-imports key
        -- on po_number so refreshed exports overwrite stale rows.
        CREATE TABLE IF NOT EXISTS scprs_reytech_wins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_number TEXT NOT NULL,
            business_unit TEXT,
            dept_name TEXT,
            associated_po TEXT,
            start_date TEXT,
            end_date TEXT,
            grand_total REAL,
            items_json TEXT,
            imported_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_scprs_reytech_wins_po
            ON scprs_reytech_wins(po_number);
        CREATE INDEX IF NOT EXISTS idx_scprs_reytech_wins_dept
            ON scprs_reytech_wins(dept_name);
    """),

    (32, "buyer_template_candidates", """
        -- Phase 1.6 PR3c (2026-04-26): every incoming attached PDF gets
        -- fingerprinted and registered here. When a fingerprint is unknown
        -- (no matching FormProfile yet), the operator can promote it to a
        -- buyer-specific YAML profile via /settings/forms (PR3f, future).
        --
        -- Status lifecycle: candidate → promoted | ignored.
        -- Dedup key: (fingerprint, agency_key) — same blank can apply to
        -- multiple agencies, but we only register once per agency.
        CREATE TABLE IF NOT EXISTS buyer_template_candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fingerprint TEXT NOT NULL,
            agency_key TEXT NOT NULL DEFAULT '',
            form_type_guess TEXT NOT NULL DEFAULT '',
            sample_filename TEXT NOT NULL DEFAULT '',
            sample_quote_id TEXT NOT NULL DEFAULT '',
            sample_quote_type TEXT NOT NULL DEFAULT '',
            field_count INTEGER NOT NULL DEFAULT 0,
            page_count INTEGER NOT NULL DEFAULT 0,
            first_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
            last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
            seen_count INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL DEFAULT 'candidate',
            promoted_profile_id TEXT NOT NULL DEFAULT '',
            notes TEXT NOT NULL DEFAULT ''
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_buyer_template_candidates_dedup
            ON buyer_template_candidates(fingerprint, agency_key);
        CREATE INDEX IF NOT EXISTS idx_buyer_template_candidates_status
            ON buyer_template_candidates(status);
        CREATE INDEX IF NOT EXISTS idx_buyer_template_candidates_form_type
            ON buyer_template_candidates(form_type_guess);
    """),

    (33, "intel_acceptance_log", """
        -- Phase 4.7.3 of PLAN_PRICING_ENGINE_INTEGRATION.md (2026-04-27).
        -- Telemetry for the category-intel suggested_alternative swap link.
        -- Each row = one accept-or-reject decision Mike made on a danger
        -- bucket suggestion. After 30 days of data, the damping factor
        -- (currently hand-tuned at 0.5) can be learned from rejection
        -- rate per bucket.
        --
        -- accepted=1 → Mike clicked the swap link (took the lower price)
        -- accepted=0 → Mike priced something OTHER than the suggestion
        --              (tracked passively via post-quote autosave diff)
        CREATE TABLE IF NOT EXISTS intel_acceptance_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description TEXT NOT NULL,
            agency TEXT,
            category TEXT NOT NULL,
            flavor TEXT NOT NULL,
            engine_markup_pct REAL,
            engine_price REAL,
            suggested_markup_pct REAL,
            suggested_price REAL,
            final_price REAL,
            accepted INTEGER NOT NULL DEFAULT 0,
            quote_number TEXT,
            pcid TEXT,
            recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_intel_accept_category
            ON intel_acceptance_log(category);
        CREATE INDEX IF NOT EXISTS idx_intel_accept_recorded
            ON intel_acceptance_log(recorded_at);
        CREATE INDEX IF NOT EXISTS idx_intel_accept_agency
            ON intel_acceptance_log(agency);
    """),

    (34, "operator_quote_sent_telemetry", """
        -- Plan §4.1 of PLAN_ONCE_AND_FOR_ALL.md (2026-04-27).
        -- KPI is "1 quote sent in <90 seconds." Until now we had no
        -- way to MEASURE that — quote send fired without a timer.
        --
        -- One row per Mark Sent click. quote_id is the canonical
        -- pcid/rfq_id (whichever flavor of quote was sent). started_at
        -- is when the operator opened the detail page (or first edit
        -- if we can't get that — falls back to created_at).
        -- time_to_send_seconds = sent_at - started_at, computed at
        -- ingest time so analytics aren't paying the cost.
        --
        -- agency_key + item_count let us slice by buyer × complexity:
        -- "median time-to-send for 1-item CCHCS quotes this week."
        CREATE TABLE IF NOT EXISTS operator_quote_sent (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quote_id TEXT NOT NULL,
            quote_type TEXT NOT NULL DEFAULT 'pc',
            sent_at TEXT NOT NULL DEFAULT (datetime('now')),
            started_at TEXT,
            time_to_send_seconds INTEGER,
            item_count INTEGER DEFAULT 0,
            agency_key TEXT DEFAULT '',
            quote_total REAL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_operator_quote_sent_at
            ON operator_quote_sent(sent_at);
        CREATE INDEX IF NOT EXISTS idx_operator_quote_sent_agency
            ON operator_quote_sent(agency_key);
        CREATE INDEX IF NOT EXISTS idx_operator_quote_sent_quote
            ON operator_quote_sent(quote_id);
    """),
]


def _run_migration_28(conn):
    """V2-PR-7: registration-gap detector + Gmail bulk-seed agent.

    Two new tables:
      - agency_domain_aliases: domain → dept_code map for inbound RFQ
        sender resolution
      - agency_pending_aliases: queue of unmapped sender domains the
        agent encountered (compounds operator-reviewable backlog)

    Plus two columns on existing agency_vendor_registry:
      - is_provisional: agent-seeded rows start at 1, operator confirm
        flips to 0
      - evidence_message_ids: JSON array of Gmail message IDs that
        triggered the auto-seed (auditable / re-runnable)

    Idempotent ALTER TABLE ADD COLUMN with try/except per existing pattern.
    """
    def _add_col(table, col, coltype):
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
        except Exception as _e:
            log.debug("suppressed (column likely exists): %s", _e)

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS agency_domain_aliases (
            domain      TEXT PRIMARY KEY,
            dept_code   TEXT NOT NULL,
            dept_name   TEXT DEFAULT '',
            confidence  TEXT NOT NULL DEFAULT 'high',
            is_active   INTEGER NOT NULL DEFAULT 1,
            notes       TEXT DEFAULT '',
            created_at  TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_alias_dept ON agency_domain_aliases(dept_code);

        CREATE TABLE IF NOT EXISTS agency_pending_aliases (
            domain      TEXT PRIMARY KEY,
            seen_count  INTEGER NOT NULL DEFAULT 1,
            first_seen  TEXT NOT NULL DEFAULT (datetime('now')),
            last_seen   TEXT NOT NULL DEFAULT (datetime('now')),
            example_subject TEXT DEFAULT ''
        );
    """)

    # Add columns to agency_vendor_registry only if that table exists
    # (it does once migration 24 has fired — should be true everywhere).
    has_avr = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='agency_vendor_registry'"
    ).fetchone()
    if has_avr:
        _add_col("agency_vendor_registry", "is_provisional",
                 "INTEGER NOT NULL DEFAULT 0")
        _add_col("agency_vendor_registry", "evidence_message_ids",
                 "TEXT DEFAULT '[]'")

    # Pre-seed canonical CA agency domains. Conservative list — only
    # domains that map cleanly to a single dept_code at high confidence.
    # Bare @ca.gov and @state.ca.gov intentionally OMITTED (too ambiguous).
    seeds = [
        ("cdcr.ca.gov",        "5225", "CDCR / Corrections"),
        ("cchcs.ca.gov",       "4700", "CCHCS / Correctional Health"),
        ("dsh.ca.gov",         "4440", "DSH / State Hospitals"),
        ("calvet.ca.gov",      "7800", "CalVet"),
        ("calfire.ca.gov",     "3840", "CalFire"),
        ("fire.ca.gov",        "3840", "CalFire"),
        ("cdph.ca.gov",        "4265", "CDPH / Public Health"),
        ("dot.ca.gov",         "2660", "CalTrans"),
        ("calrecycle.ca.gov",  "6440", "CalRecycle"),
        ("dgs.ca.gov",         "1760", "DGS"),
        ("water.ca.gov",       "3100", "Water Resources"),
        ("dss.ca.gov",         "5180", "Social Services"),
        ("dmh.ca.gov",         "4150", "Mental Health"),
        ("chp.ca.gov",         "2720", "CHP"),
        ("va.gov",             "7120", "Veterans Affairs (federal)"),
    ]
    try:
        conn.executemany(
            "INSERT OR IGNORE INTO agency_domain_aliases "
            "(domain, dept_code, dept_name, confidence) "
            "VALUES (?, ?, ?, 'high')",
            seeds,
        )
    except Exception as _e:
        log.debug("alias seed suppressed: %s", _e)

    log.info("Migration 28: registration_gap_agent tables ensured + "
             "agency_vendor_registry columns + %d alias seeds", len(seeds))


def _run_migration_23(conn):
    """
    Add is_test column to SCPRS tables so test data can never feed real
    aggregates or auto-close-lost decisions.

    Same shape as the CR-5 / AN-P0 / RE-AUDIT-5 work in earlier audits:
    every operationally significant table needs an is_test flag, and every
    read site that feeds operator-visible decisions must filter is_test=0.

    Idempotent — ADD COLUMN failures (column already exists) are swallowed.
    """
    def _add_col(table, col, coltype):
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
        except Exception as _e:
            log.debug("suppressed (column likely exists): %s", _e)

    has_master = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='scprs_po_master'"
    ).fetchone()
    has_lines = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='scprs_po_lines'"
    ).fetchone()
    if not (has_master or has_lines):
        log.info("Migration 23: SCPRS tables not present yet — skipping (fresh install)")
        return

    if has_master:
        _add_col("scprs_po_master", "is_test", "INTEGER NOT NULL DEFAULT 0")
        # Speeds up the is_test=0 filter on the most-read aggregator paths.
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_po_is_test ON scprs_po_master(is_test)"
        )
    if has_lines:
        _add_col("scprs_po_lines", "is_test", "INTEGER NOT NULL DEFAULT 0")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lines_is_test ON scprs_po_lines(is_test)"
        )

    log.info("Migration 23: is_test column ensured on scprs_po_master + scprs_po_lines")


def _run_migration_22(conn):
    """
    Dedup scprs_po_lines on (po_id, line_num) and add the UNIQUE INDEX that
    makes INSERT OR REPLACE actually upsert.

    Why: scprs_universal_pull.run_universal_pull stores line items with
    `INSERT OR REPLACE INTO scprs_po_lines ... VALUES (...)` against a table
    that had only `id` as PRIMARY KEY — no uniqueness on (po_id, line_num).
    Result: every re-pull of the same PO duplicated all of its lines,
    silently inflating gap_spend / win_back_spend / by_agency totals.

    Idempotent — safe to re-run on already-deduped DBs.
    """
    # Skip if the table doesn't exist yet (fresh install — _ensure_schema
    # will create the table with the UNIQUE INDEX in one shot).
    has_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='scprs_po_lines'"
    ).fetchone()
    if not has_table:
        log.info("Migration 22: scprs_po_lines does not exist yet — skipping (fresh install)")
        return

    # Count duplicates before delete (informational).
    dup_groups = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT po_id, line_num, COUNT(*) c
            FROM scprs_po_lines
            GROUP BY po_id, line_num
            HAVING c > 1
        )
    """).fetchone()[0]

    if dup_groups:
        # Keep the highest id per (po_id, line_num) — most recently INSERTed
        # row for each logical position.
        cur = conn.execute("""
            DELETE FROM scprs_po_lines
            WHERE id NOT IN (
                SELECT MAX(id) FROM scprs_po_lines GROUP BY po_id, line_num
            )
        """)
        log.info("Migration 22: deduped scprs_po_lines — %d duplicate groups, %d rows removed",
                 dup_groups, cur.rowcount)
    else:
        log.info("Migration 22: scprs_po_lines has no duplicates")

    # Create the UNIQUE INDEX (idempotent via IF NOT EXISTS).
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_scprs_po_lines_po_linenum
            ON scprs_po_lines(po_id, line_num)
    """)


def _run_migration_15(conn):
    """Platform upgrade — quote expiry + contact address columns."""
    def _add_col(table, col, coltype):
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
        except Exception as _e:
            log.debug("suppressed: %s", _e)
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
        except Exception as _e:
            log.debug("suppressed: %s", _e)  # Column already exists

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
        except Exception as _e:
            log.debug("suppressed: %s", _e)  # Column already exists

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


def _heal_workflow_runs_schema(conn):
    """Backfill missing columns on legacy workflow_runs before migrations 19+.

    `src/core/db.py` boot DDL creates a LEGACY workflow_runs (id/started_at/
    finished_at/type/status/run_at/score/grade/passed/failed/warned/...). That
    DDL runs before migrations. Migration 19 then does
    `CREATE TABLE IF NOT EXISTS workflow_runs (...task_type NOT NULL, ...)`,
    which is a no-op because the table exists, then tries
    `CREATE INDEX ... ON workflow_runs(task_type)` which raises because the
    column doesn't exist. That abort cascades — migrations 20, 21, ... never
    apply. Migration 21 is what creates `quote_audit_log`, so /quoting/status
    stays empty and orchestrator audit inserts fail silently.

    This helper is idempotent: if a column already exists, ALTER TABLE ADD
    COLUMN raises and we swallow it. Runs unconditionally on every boot so
    prod DBs that already advanced past this point stay unaffected.
    """
    def _add_col(col, coltype):
        try:
            conn.execute(f"ALTER TABLE workflow_runs ADD COLUMN {col} {coltype}")
        except Exception as _e:
            log.debug("workflow_runs column %s: %s", col, _e)

    try:
        conn.execute("SELECT 1 FROM workflow_runs LIMIT 0")
    except Exception:
        return  # Table doesn't exist yet — migration 19's CREATE TABLE handles it.

    _add_col("task_type",     "TEXT DEFAULT ''")
    _add_col("phase",         "TEXT DEFAULT ''")
    _add_col("progress",      "TEXT DEFAULT ''")
    _add_col("running",       "INTEGER DEFAULT 0")
    _add_col("items_done",    "INTEGER DEFAULT 0")
    _add_col("items_total",   "INTEGER DEFAULT 0")
    _add_col("results_count", "INTEGER DEFAULT 0")
    _add_col("errors_json",   "TEXT DEFAULT '[]'")
    _add_col("last_updated",  "TEXT DEFAULT ''")


def run_migrations():
    """Apply any pending migrations. Safe to call on every startup."""
    try:
        with _get_db() as conn:
            _ensure_migration_table(conn)
            current = _get_current_version(conn)

            # Heal legacy workflow_runs BEFORE the migration loop so that
            # migrations 19 (indexes) and 20 (triggers referencing errors_json)
            # don't abort the loop. Idempotent on already-healed DBs.
            _heal_workflow_runs_schema(conn)

            applied = 0
            for version, name, sql in MIGRATIONS:
                if version <= current:
                    continue

                try:
                    conn.executescript(sql)
                    # OR IGNORE so duplicate version numbers in MIGRATIONS list
                    # (legacy: v32 has both cost_alerts and buyer_template_candidates)
                    # don't abort the loop on PK conflict. CREATE TABLE IF NOT EXISTS
                    # already guarantees DDL idempotence.
                    conn.execute(
                        "INSERT OR IGNORE INTO schema_migrations (version, name, applied_at) VALUES (?,?,?)",
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

            if current < 22:
                try:
                    _run_migration_22(conn)
                    conn.execute(
                        "INSERT OR REPLACE INTO schema_migrations (version, name, applied_at) VALUES (?,?,?)",
                        (22, "scprs_po_lines_dedup_and_unique_index", datetime.now().isoformat())
                    )
                    applied += 1
                    log.info("Migration 22 applied: scprs_po_lines_dedup_and_unique_index (programmatic)")
                except Exception as e:
                    # error (not warning) so future Railway alert hooks catch
                    # a silent dedup failure — inflated totals would otherwise
                    # persist invisibly.
                    log.error("Migration 22 partial: %s (non-fatal)", e)

            if current < 23:
                try:
                    _run_migration_23(conn)
                    conn.execute(
                        "INSERT OR REPLACE INTO schema_migrations (version, name, applied_at) VALUES (?,?,?)",
                        (23, "scprs_tables_add_is_test", datetime.now().isoformat())
                    )
                    applied += 1
                    log.info("Migration 23 applied: scprs_tables_add_is_test (programmatic)")
                except Exception as e:
                    log.error("Migration 23 partial: %s (non-fatal)", e)

            if current < 28:
                try:
                    _run_migration_28(conn)
                    conn.execute(
                        "INSERT OR REPLACE INTO schema_migrations (version, name, applied_at) VALUES (?,?,?)",
                        (28, "registration_gap_agent_tables", datetime.now().isoformat())
                    )
                    applied += 1
                    log.info("Migration 28 applied: registration_gap_agent_tables (programmatic)")
                except Exception as e:
                    log.error("Migration 28 partial: %s (non-fatal)", e)

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
