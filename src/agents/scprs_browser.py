"""
SCPRS Browser Scraper — Playwright-based detail page extraction.
Uses headless Chromium to execute PeopleSoft's JS modals and
extract line-item data that HTTP scraping cannot reach.
"""

import logging
import re
import asyncio

log = logging.getLogger("scprs.browser")

SCPRS_SEARCH_URL = (
    "https://suppliers.fiscal.ca.gov/psc/psfpd1/"
    "SUPPLIER/ERP/c/ZZ_PO.ZZ_SCPRS1_CMP.GBL"
)

DETAIL_FIELDS = {
    "ZZ_SCPR_PDL_DVW_DESCR254_MIXED": "description",
    "ZZ_SCPR_PDL_DVW_INV_ITEM_ID": "item_id",
    "ZZ_SCPR_PDL_DVW_CRDMEM_ACCT_NBR": "line_number",
    "ZZ_SCPR_PDL_DVW_PV_UNSPSC_CODE": "unspsc",
    "ZZ_SCPR_PDL_DVW_DESCR": "uom",
    "ZZ_SCPR_PDL_DVW_QUANTITY": "quantity",
    "ZZ_SCPR_PDL_DVW_UNIT_PRICE": "unit_price",
    "ZZ_SCPR_PDL_DVW_LINE_TOTAL": "line_total",
    "ZZ_SCPR_PDL_DVW_DESCR1": "line_status",
}

HEADER_FIELDS = {
    "ZZ_SCPR_SBP_WRK_BUSINESS_UNIT": "dept_code",
    "ZZ_SCPR_SBP_WRK_DESCR": "dept_name",
    "ZZ_SCPR_SBP_WRK_CRDMEM_ACCT_NBR": "po_number",
    "ZZ_SCPR_SBP_WRK_STATUS1": "status",
    "ZZ_SCPR_SBP_WRK_START_DATE": "start_date",
    "ZZ_SCPR_SBP_WRK_END_DATE": "end_date",
    "ZZ_SCPR_SBP_WRK_NAME1": "supplier",
    "ZZ_SCPR_SBP_WRK_ZZ_COMMENT1": "acq_type",
    "ZZ_SCPR_SBP_WRK_ZZ_ACQ_MTHD": "acq_method",
    "ZZ_SCPR_SBP_WRK_MERCH_AMT_TTL": "merch_amount",
    "ZZ_SCPR_SBP_WRK_ADJ_AMT_TTL": "freight_tax",
    "ZZ_SCPR_SBP_WRK_AWARDED_AMT": "grand_total",
    "ZZ_SCPR_SBP_WRK_BUYER_DESCR": "buyer_name",
    "ZZ_SCPR_SBP_WRK_EMAILID": "buyer_email",
    "ZZ_SCPR_SBP_WRK_PHONE": "buyer_phone",
}


def _parse_dollar(text):
    if not text:
        return None
    try:
        return float(re.sub(r'[^\d.]', '', text))
    except Exception:
        return None


async def _scrape_detail_async(supplier_name="reytech",
                                from_date="",
                                max_rows=200):
    """
    Launch headless browser, search SCPRS, click each PO link,
    wait for detail modal, extract line items.
    """
    from playwright.async_api import async_playwright

    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-popup-blocking",
                "--disable-features=BlockInsecurePrivateNetworkRequests",
            ]
        )
        context = await browser.new_context(
            java_script_enabled=True,
            bypass_csp=True,
        )
        page = await context.new_page()
        page.set_default_timeout(30000)

        try:
            # Step 1: Load search page (double-load for PeopleSoft)
            log.info("Browser: loading SCPRS search page")
            await page.goto(SCPRS_SEARCH_URL + "?&",
                            wait_until="networkidle")
            content = await page.content()
            log.info("Browser: load1 %db ICSID=%s",
                     len(content), "ICSID" in content)

            # PeopleSoft often needs a second load
            if "ICSID" not in content or len(content) < 10000:
                await page.goto(SCPRS_SEARCH_URL + "?&",
                                wait_until="networkidle")
                content = await page.content()
                log.info("Browser: load2 %db ICSID=%s",
                         len(content), "ICSID" in content)

            # Step 2: Fill search form
            log.info("Browser: filling search form")
            name_field = page.locator("#ZZ_SCPRS_SP_WRK_NAME1")
            name_count = await name_field.count()
            log.info("Browser: name field count=%d", name_count)
            if name_count > 0:
                await name_field.fill(supplier_name)

            date_field = page.locator("#ZZ_SCPRS_SP_WRK_FROM_DATE")
            date_count = await date_field.count()
            log.info("Browser: date field count=%d", date_count)
            if date_count > 0:
                await date_field.fill(from_date)

            # Click search button
            search_btn = page.locator("#ZZ_SCPRS_SP_WRK_BUTTON")
            btn_count = await search_btn.count()
            log.info("Browser: search button count=%d", btn_count)
            if btn_count == 0:
                log.error("Browser: no search button found")
                await context.close()
                await browser.close()
                return results

            await search_btn.click()
            log.info("Browser: search clicked, waiting for results...")

            # PeopleSoft search is AJAX — wait for results grid
            try:
                await page.wait_for_selector(
                    "[id^='ZZ_SCPR_RSLT_VW']",
                    timeout=15000
                )
                log.info("Browser: results grid appeared")
            except Exception:
                # Fallback: wait for processing to complete
                await page.wait_for_timeout(3000)
                log.info("Browser: waited 3s fallback")

            # Also wait for network to settle after AJAX
            await page.wait_for_load_state("networkidle")

            # Check for results
            content = await page.content()
            log.info("Browser: post-search %db", len(content))
            count_match = re.search(
                r'(\d+)\s+to\s+(\d+)\s+of\s+(\d+)', content
            )
            if not count_match:
                title = re.search(r'<title>([^<]*)</title>', content)
                has_form = "ZZ_SCPRS_SP_WRK" in content
                log.warning("Browser: no results. title=%s form=%s size=%d",
                            title.group(1)[:50] if title else "?",
                            has_form, len(content))
                await context.close()
                await browser.close()
                return results

            total = int(count_match.group(3))
            log.info("Browser: %d results found", total)

            # Step 3: Click first result to open modal, then click each PO
            link_id = "ZZ_SCPR_RSLT_VW$hmodal$0"
            link = page.locator(f"[id='{link_id}']")
            if await link.count() == 0:
                log.warning("Browser: no modal link found")
                await context.close()
                await browser.close()
                return results

            log.info("Browser: clicking %s to open modal", link_id)
            await link.click()
            await page.wait_for_timeout(5000)

            frames = page.frames
            if len(frames) < 2:
                log.warning("Browser: no modal frame")
                await context.close()
                await browser.close()
                return results

            modal_frame = frames[1]

            # Find ALL PO links in modal
            po_links = await modal_frame.locator("a:has-text('4500')").all()
            other_links = await modal_frame.locator("a:has-text('0000')").all()
            all_po_elements = po_links + other_links

            log.info("Browser: found %d PO links in modal (%d 4500-prefix, %d 0000-prefix)",
                     len(all_po_elements), len(po_links), len(other_links))

            await page.screenshot(path="/data/scprs_step1_modal_0.png", full_page=True)

            # Process each PO
            processed = 0
            failed = 0
            for po_idx, po_el in enumerate(all_po_elements):
                if processed >= max_rows:
                    break

                try:
                    po_text = (await po_el.text_content() or "").strip()
                    if not po_text or len(po_text) < 7:
                        continue

                    log.info("Browser: [%d/%d] clicking PO %s",
                             po_idx + 1, len(all_po_elements), po_text)

                    # Click PO — opens new window
                    detail_page = None
                    try:
                        async with page.context.expect_page(timeout=15000) as new_page_info:
                            await po_el.click()
                        detail_page = await new_page_info.value
                        await detail_page.wait_for_load_state("networkidle")
                    except Exception as e:
                        log.warning("Browser: PO %s no window: %s", po_text, str(e)[:40])
                        failed += 1
                        if failed > 5:
                            log.error("Browser: too many failures, stopping")
                            break
                        continue

                    dp_content = await detail_page.content()
                    has_pdl = "ZZ_SCPR_PDL_DVW" in dp_content
                    log.info("Browser: PO %s window %db PDL=%s",
                             po_text, len(dp_content), has_pdl)

                    if has_pdl:
                        detail = _parse_browser_detail(dp_content)
                        if detail and detail.get("line_items"):
                            detail["source"] = "scprs_browser"
                            detail["_po_text"] = po_text
                            results.append(detail)
                            processed += 1
                            log.info("Browser: PO=%s %d lines, $%s buyer=%s [%d done]",
                                     detail["header"].get("po_number", po_text),
                                     len(detail["line_items"]),
                                     detail["header"].get("grand_total", "?"),
                                     detail["header"].get("buyer_name", "?"),
                                     processed)

                            # Save screenshot + HTML for every PO
                            import os
                            os.makedirs("/data/po_records", exist_ok=True)
                            try:
                                await detail_page.screenshot(
                                    path=f"/data/po_records/{po_text}.png",
                                    full_page=True
                                )
                                with open(f"/data/po_records/{po_text}.html", "w", encoding="utf-8") as _f:
                                    _f.write(dp_content)
                                detail["screenshot_path"] = f"/data/po_records/{po_text}.png"
                            except Exception:
                                pass
                    else:
                        log.warning("Browser: PO %s no line items in %db",
                                    po_text, len(dp_content))
                        failed += 1

                    # Close detail window
                    await detail_page.close()

                    # Small delay to not hammer the server
                    await page.wait_for_timeout(500)

                except Exception as e:
                    log.warning("Browser: PO[%d] error: %s", po_idx, str(e)[:80])
                    failed += 1
                    # Close any extra windows
                    for extra in page.context.pages[1:]:
                        try:
                            await extra.close()
                        except Exception:
                            pass
                    if failed > 10:
                        log.error("Browser: too many failures (%d), stopping", failed)
                        break
                    continue

            log.info("Browser: COMPLETE — %d POs extracted, %d failed out of %d total",
                     processed, failed, len(all_po_elements))

            # Save final state
            try:
                await page.screenshot(path="/data/scprs_final.png", full_page=True)
            except Exception:
                pass

        except Exception as e:
            log.error("Browser scrape failed: %s", e)
        finally:
            await context.close()
            await browser.close()

    return results


def _parse_browser_detail(html):
    """Parse detail page HTML from browser for line items."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    def get_text(element_id):
        el = soup.find(id=element_id)
        if el:
            t = el.get_text(strip=True)
            return t if t and t != "\xa0" else ""
        return ""

    header = {}
    for field_id, key in HEADER_FIELDS.items():
        header[key] = get_text(field_id)

    line_items = []
    for row in range(200):
        desc = get_text(f"ZZ_SCPR_PDL_DVW_DESCR254_MIXED${row}")
        if not desc:
            break

        item = {"line_num": row + 1}
        for field_prefix, key in DETAIL_FIELDS.items():
            item[key] = get_text(f"{field_prefix}${row}")

        item["unit_price_num"] = _parse_dollar(item.get("unit_price", ""))
        item["line_total_num"] = _parse_dollar(item.get("line_total", ""))
        try:
            item["quantity_num"] = float(
                item.get("quantity", "0").replace(",", "")
            )
        except Exception:
            item["quantity_num"] = 0

        line_items.append(item)

    return {"header": header, "line_items": line_items}


def scrape_details(supplier_name="reytech", from_date="",
                   max_rows=500):
    """Synchronous wrapper for async browser scraping."""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results = loop.run_until_complete(
            _scrape_detail_async(supplier_name, from_date, max_rows)
        )
        loop.close()
        return results
    except Exception as e:
        log.error("scrape_details failed: %s", e)
        return []


def scrape_po_detail(po_number):
    """Scrape detail for a single PO number."""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results = loop.run_until_complete(
            _scrape_single_po(po_number)
        )
        loop.close()
        return results
    except Exception as e:
        log.error("scrape_po_detail failed: %s", e)
        return None


async def _scrape_single_po(po_number):
    """Search for a specific PO and extract its detail."""
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-popup-blocking",
                "--disable-features=BlockInsecurePrivateNetworkRequests",
            ]
        )
        context = await browser.new_context(
            java_script_enabled=True,
            bypass_csp=True,
        )
        page = await context.new_page()
        page.set_default_timeout(30000)

        try:
            await page.goto(SCPRS_SEARCH_URL + "?&",
                            wait_until="networkidle")
            if "ICSID" not in await page.content():
                await page.goto(SCPRS_SEARCH_URL + "?&",
                                wait_until="networkidle")

            # Search by PO number
            po_field = page.locator("#ZZ_SCPRS_SP_WRK_CRDMEM_ACCT_NBR")
            if await po_field.count() > 0:
                await po_field.fill(po_number)

            search_btn = page.locator("#ZZ_SCPRS_SP_WRK_BUTTON")
            await search_btn.click()
            await page.wait_for_load_state("networkidle")

            # Click first result
            link = page.locator("[id='ZZ_SCPR_RSLT_VW$hmodal$0']")
            if await link.count() > 0:
                await link.click()
                try:
                    await page.wait_for_selector(
                        "[id^='ZZ_SCPR_PDL_DVW'], "
                        "[id^='ZZ_SCPR_SBP_WRK']",
                        timeout=10000
                    )
                except Exception:
                    pass

                html = await page.content()
                detail = _parse_browser_detail(html)
                if detail:
                    detail["source"] = "scprs_browser"
                return detail

        except Exception as e:
            log.error("Browser PO %s failed: %s", po_number, e)
        finally:
            await context.close()
            await browser.close()

    return None


# ── 3-Layer Storage ─────────────────────────────────────────────

def _store_results(batch, seen_pos):
    """Store into 3 layers: raw FI$Cal DB, Reytech won quotes, catalog."""
    stored_pos = 0
    stored_lines = 0
    won_quotes = 0
    catalog_items = 0

    try:
        import sqlite3
        from src.core.db import DB_PATH
        db = sqlite3.connect(DB_PATH, timeout=30)

        for r in batch:
            header = r.get("header", {})
            po = header.get("po_number", "")
            if not po:
                continue
            seen_pos.add(po)
            screenshot_path = r.get("screenshot_path", "")

            # LAYER 1: Raw FI$Cal
            try:
                db.execute("""
                    INSERT OR REPLACE INTO scprs_po_master
                    (po_number, dept_code, dept_name, status, start_date,
                     end_date, supplier, supplier_id, acq_type, acq_method,
                     merch_amount, grand_total, buyer_name, buyer_email,
                     buyer_phone, source_system, screenshot_path, scraped_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
                """, (
                    po, header.get("dept_code", ""), header.get("dept_name", ""),
                    header.get("status", ""), header.get("start_date", ""),
                    header.get("end_date", ""), header.get("supplier", ""),
                    header.get("supplier_id", ""), header.get("acq_type", ""),
                    header.get("acq_method", ""), header.get("merch_amount", ""),
                    header.get("grand_total", ""), header.get("buyer_name", ""),
                    header.get("buyer_email", ""), header.get("buyer_phone", ""),
                    "scprs_browser", screenshot_path,
                ))
                stored_pos += 1
            except Exception as e:
                log.warning("Store PO master %s: %s", po, str(e)[:60])

            for line in r.get("line_items", []):
                try:
                    db.execute("""
                        INSERT OR IGNORE INTO scprs_po_lines
                        (po_number, line_num, item_id, description,
                         unspsc, uom, quantity, unit_price, line_total,
                         line_status, category)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        po, line.get("line_number", ""), line.get("item_id", ""),
                        (line.get("description", "") or "")[:500],
                        line.get("unspsc", ""), line.get("uom", ""),
                        line.get("quantity_num", 0) or 0,
                        line.get("unit_price_num", 0) or 0,
                        line.get("line_total_num", 0) or 0,
                        line.get("line_status", ""), "other",
                    ))
                    stored_lines += 1
                except Exception as e:
                    log.warning("Store line %s: %s", po, str(e)[:60])

            # LAYER 2: Won Quotes — Reytech only
            supplier = (header.get("supplier", "") or "").upper()
            if "REYTECH" in supplier:
                try:
                    from src.knowledge.won_quotes_db import ingest_scprs_result
                    for line in r.get("line_items", []):
                        up = line.get("unit_price_num")
                        if up and up > 0:
                            ingest_scprs_result(
                                po_number=po,
                                item_number=line.get("item_id", ""),
                                description=line.get("description", ""),
                                unit_price=up,
                                quantity=line.get("quantity_num", 1) or 1,
                                supplier=header.get("supplier", ""),
                                department=header.get("dept_name", ""),
                                award_date=header.get("start_date", ""),
                                source="scprs_browser_won",
                            )
                            won_quotes += 1
                except Exception as e:
                    log.warning("Won quotes %s: %s", po, str(e)[:60])

            # LAYER 3: Product Catalog
            for line in r.get("line_items", []):
                up = line.get("unit_price_num")
                desc = line.get("description", "")
                if up and up > 0 and desc:
                    try:
                        db.execute("""
                            INSERT INTO scprs_catalog
                            (description, unspsc, last_unit_price, last_quantity,
                             last_uom, last_supplier, last_department,
                             last_po_number, last_date, times_seen, updated_at)
                            VALUES (?,?,?,?,?,?,?,?,?,1,datetime('now'))
                            ON CONFLICT(description) DO UPDATE SET
                                last_unit_price = excluded.last_unit_price,
                                last_quantity = excluded.last_quantity,
                                last_supplier = excluded.last_supplier,
                                last_department = excluded.last_department,
                                last_po_number = excluded.last_po_number,
                                last_date = excluded.last_date,
                                times_seen = scprs_catalog.times_seen + 1,
                                updated_at = datetime('now')
                        """, (
                            desc[:500], line.get("unspsc", ""), up,
                            line.get("quantity_num", 1), line.get("uom", ""),
                            header.get("supplier", ""), header.get("dept_name", ""),
                            po, header.get("start_date", ""),
                        ))
                        catalog_items += 1
                    except Exception:
                        pass

        db.commit()
        db.close()
    except Exception as e:
        log.error("Store results DB error: %s", e)

    log.info("Stored: %d POs, %d lines -> DB | %d -> Won Quotes | %d -> Catalog",
             stored_pos, stored_lines, won_quotes, catalog_items)

    # Refresh buyer profiles after storing new data
    try:
        from src.agents.buyer_intelligence import refresh_buyer_profiles
        refresh_buyer_profiles()
    except Exception as e:
        log.warning("Buyer refresh failed: %s", str(e)[:60])

    return stored_lines


# ── Exhaustive Scrape ───────────────────────────────────────────

def schedule_full_fiscal_scrape(target_hour_pst=2):
    """Schedule exhaustive FI$Cal scrape at target hour PST."""
    import threading
    from datetime import datetime, timezone, timedelta
    PST = timezone(timedelta(hours=-8))

    def _wait_and_run():
        import time as _time
        while True:
            now = datetime.now(PST)
            target = now.replace(hour=target_hour_pst, minute=0, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            wait_seconds = (target - now).total_seconds()
            log.info("FISCAL SCRAPE: next run %s PST (in %.1f hours)",
                     target.strftime("%Y-%m-%d %H:%M"), wait_seconds / 3600)
            _time.sleep(wait_seconds)
            try:
                _run_exhaustive_scrape()
            except Exception as e:
                log.error("FISCAL SCRAPE failed: %s", e)
            _time.sleep(60)

    t = threading.Thread(target=_wait_and_run, daemon=True, name="fiscal-exhaustive")
    t.start()


def _run_exhaustive_scrape():
    """Every PO in FI$Cal since 2019. Monthly windows. Auto-retry."""
    import time as _time
    from datetime import datetime, timedelta

    log.info("=" * 60)
    log.info("FISCAL EXHAUSTIVE SCRAPE — STARTING")
    log.info("=" * 60)

    seen_pos = set()
    total_pos = 0
    total_lines = 0
    total_ingested = 0
    total_errors = 0

    # Monthly windows from Jan 2019 to now
    date_ranges = []
    current = datetime(2019, 1, 1)
    now = datetime.now()
    while current < now:
        next_month = (current + timedelta(days=32)).replace(day=1)
        if next_month > now:
            next_month = now
        date_ranges.append((current.strftime("%m/%d/%Y"), next_month.strftime("%m/%d/%Y")))
        current = next_month

    date_ranges.reverse()
    log.info("FISCAL: %d monthly windows", len(date_ranges))

    for idx, (from_d, to_d) in enumerate(date_ranges):
        log.info("FISCAL [%d/%d]: %s - %s (seen %d POs)",
                 idx + 1, len(date_ranges), from_d, to_d, len(seen_pos))
        try:
            batch = _scrape_with_retry(
                search_params={"supplier_name": "", "from_date": from_d,
                               "to_date": to_d, "description": ""},
                seen_pos=seen_pos, max_rows=500,
            )
            ingested = _store_results(batch, seen_pos)
            batch_lines = sum(len(r.get("line_items", [])) for r in batch)
            total_pos += len(batch)
            total_lines += batch_lines
            total_ingested += ingested

            log.info("FISCAL [%d/%d]: %d POs, %d lines [TOTAL: %d POs, %d unique]",
                     idx + 1, len(date_ranges), len(batch), batch_lines,
                     total_pos, len(seen_pos))

            _time.sleep(5)
        except Exception as e:
            total_errors += 1
            log.error("FISCAL [%d/%d]: FAILED: %s", idx + 1, len(date_ranges), e)
            _time.sleep(15)

    log.info("=" * 60)
    log.info("FISCAL EXHAUSTIVE SCRAPE COMPLETE")
    log.info("  POs extracted:    %d", total_pos)
    log.info("  Line items:       %d", total_lines)
    log.info("  Unique POs:       %d", len(seen_pos))
    log.info("  Items ingested:   %d", total_ingested)
    log.info("  Errors (skipped): %d", total_errors)
    log.info("=" * 60)

    # Post-scrape: re-enrich all existing quotes with fresh data
    try:
        from src.agents.quote_reprocessor import reprocess_all_quotes
        reprocess_all_quotes()
    except Exception as e:
        log.warning("Post-scrape quote reprocessing failed: %s", e)

    # Enrich catalog with product identifiers
    try:
        from src.agents.item_enricher import enrich_catalog
        enriched = enrich_catalog()
        log.info("Catalog enrichment: %d items processed", enriched)
    except Exception as e:
        log.warning("Catalog enrichment failed: %s", e)


async def _scrape_full_async(search_params, seen_pos, max_rows=500):
    """Full async scrape with search_params dict (supports to_date, description)."""
    from playwright.async_api import async_playwright
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-popup-blocking"]
        )
        context = await browser.new_context(java_script_enabled=True, bypass_csp=True)
        page = await context.new_page()
        page.set_default_timeout(30000)
        try:
            await page.goto(SCPRS_SEARCH_URL + "?&", wait_until="networkidle")
            content = await page.content()
            if "ICSID" not in content:
                await page.goto(SCPRS_SEARCH_URL + "?&", wait_until="networkidle")

            supplier = search_params.get("supplier_name", "")
            from_date = search_params.get("from_date", "")
            to_date = search_params.get("to_date", "")
            description = search_params.get("description", "")

            if supplier:
                f = page.locator("#ZZ_SCPRS_SP_WRK_NAME1")
                if await f.count() > 0:
                    await f.fill(supplier)
            if from_date:
                f = page.locator("#ZZ_SCPRS_SP_WRK_FROM_DATE")
                if await f.count() > 0:
                    await f.fill(from_date)
            if to_date:
                f = page.locator("#ZZ_SCPRS_SP_WRK_TO_DATE")
                if await f.count() > 0:
                    await f.fill(to_date)
            if description:
                f = page.locator("#ZZ_SCPRS_SP_WRK_DESCR254")
                if await f.count() > 0:
                    await f.fill(description)

            search_btn = page.locator("#ZZ_SCPRS_SP_WRK_BUTTON")
            if await search_btn.count() == 0:
                await context.close()
                await browser.close()
                return results
            await search_btn.click()

            try:
                await page.wait_for_selector("[id^='ZZ_SCPR_RSLT_VW']", timeout=15000)
            except Exception:
                await page.wait_for_timeout(3000)
            await page.wait_for_load_state("networkidle")

            content = await page.content()
            count_match = re.search(r'(\d+)\s+to\s+(\d+)\s+of\s+(\d+)', content)
            if not count_match:
                await context.close()
                await browser.close()
                return results

            total = int(count_match.group(3))
            log.info("Browser full: %d results found", total)

            link = page.locator("[id='ZZ_SCPR_RSLT_VW$hmodal$0']")
            if await link.count() == 0:
                await context.close()
                await browser.close()
                return results

            await link.click()
            await page.wait_for_timeout(5000)

            frames = page.frames
            if len(frames) < 2:
                await context.close()
                await browser.close()
                return results

            modal_frame = frames[1]
            failed = 0

            po_links = await modal_frame.locator("a:has-text('4500')").all()
            other_links = await modal_frame.locator("a:has-text('0000')").all()
            all_po_elements = po_links + other_links
            log.info("Browser full: %d PO links in modal", len(all_po_elements))

            for po_el in all_po_elements:
                if len(results) >= max_rows:
                    break
                try:
                    po_text = (await po_el.text_content() or "").strip()
                    if not po_text or len(po_text) < 7 or po_text in seen_pos:
                        continue

                    detail_page = None
                    try:
                        async with page.context.expect_page(timeout=15000) as info:
                            await po_el.click()
                        detail_page = await info.value
                        await detail_page.wait_for_load_state("networkidle")
                    except Exception:
                        failed += 1
                        if failed > 10:
                            break
                        continue

                    dp_content = await detail_page.content()
                    if "ZZ_SCPR_PDL_DVW" in dp_content:
                        import os
                        os.makedirs("/data/po_records", exist_ok=True)
                        try:
                            await detail_page.screenshot(path=f"/data/po_records/{po_text}.png", full_page=True)
                            with open(f"/data/po_records/{po_text}.html", "w", encoding="utf-8") as fw:
                                fw.write(dp_content)
                        except Exception:
                            pass
                        detail = _parse_browser_detail(dp_content)
                        if detail and detail.get("line_items"):
                            detail["source"] = "scprs_browser"
                            detail["screenshot_path"] = f"/data/po_records/{po_text}.png"
                            results.append(detail)
                            seen_pos.add(po_text)
                            log.info("Browser full: PO=%s %d lines [%d done]",
                                     detail["header"].get("po_number", po_text),
                                     len(detail["line_items"]), len(results))
                    else:
                        failed += 1

                    await detail_page.close()
                    await page.wait_for_timeout(500)
                except Exception:
                    failed += 1
                    for extra in page.context.pages[1:]:
                        try:
                            await extra.close()
                        except Exception:
                            pass
                    if failed > 10:
                        break

        except Exception as e:
            log.error("Browser full scrape error: %s", e)
        finally:
            await context.close()
            await browser.close()
    return results


def _scrape_with_retry(search_params, seen_pos, max_rows=500, max_retries=3):
    """Scrape with retry logic."""
    import time as _time
    for attempt in range(1, max_retries + 1):
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            results = loop.run_until_complete(
                _scrape_full_async(
                    search_params=search_params,
                    seen_pos=seen_pos,
                    max_rows=max_rows,
                )
            )
            loop.close()
            return results
        except Exception as e:
            log.warning("Scrape attempt %d/%d failed: %s", attempt, max_retries, e)
            if attempt < max_retries:
                _time.sleep(10 * attempt)
            else:
                raise
    return []
