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
                                from_date="01/01/2024",
                                max_rows=5):
    """
    Launch headless browser, search SCPRS, click each PO link,
    wait for detail modal, extract line items.
    """
    from playwright.async_api import async_playwright

    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        page = await browser.new_page()
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
                await browser.close()
                return results

            total = int(count_match.group(3))
            log.info("Browser: %d results found", total)

            # Step 3: Click each PO link
            rows_to_check = min(total, max_rows)
            for row_idx in range(rows_to_check):
                try:
                    link_id = f"ZZ_SCPR_RSLT_VW$hmodal${row_idx}"
                    link = page.locator(f"[id='{link_id}']")

                    if await link.count() == 0:
                        log.warning("Browser: link %s not found", link_id)
                        continue

                    log.info("Browser: clicking %s", link_id)
                    await link.click()

                    # Wait for detail content to appear
                    try:
                        await page.wait_for_selector(
                            "[id^='ZZ_SCPR_PDL_DVW'], "
                            "[id^='ZZ_SCPR_SBP_WRK']",
                            timeout=10000
                        )
                    except Exception:
                        log.warning("Browser: detail didn't load for row %d", row_idx)
                        close_btn = page.locator(
                            "[id*='CLOSE'], [id*='close'], "
                            "[id*='Cancel'], .ps_closebox"
                        )
                        if await close_btn.count() > 0:
                            await close_btn.first.click()
                            await page.wait_for_load_state("networkidle")
                        continue

                    detail_html = await page.content()
                    detail = _parse_browser_detail(detail_html)

                    if detail and detail.get("line_items"):
                        log.info(
                            "Browser: PO=%s %d lines, buyer=%s",
                            detail["header"].get("po_number", "?"),
                            len(detail["line_items"]),
                            detail["header"].get("buyer_name", "?")
                        )
                        detail["source"] = "scprs_browser"
                        results.append(detail)
                    else:
                        log.info("Browser: row %d no line items", row_idx)

                    # Close modal / go back to results
                    close_btn = page.locator(
                        "[id*='CLOSE'], [id*='close'], "
                        ".ps_closebox, [id*='Return']"
                    )
                    if await close_btn.count() > 0:
                        await close_btn.first.click()
                        await page.wait_for_load_state("networkidle")
                    else:
                        await page.go_back()
                        await page.wait_for_load_state("networkidle")

                except Exception as e:
                    log.warning("Browser: row %d error: %s", row_idx, e)
                    continue

        except Exception as e:
            log.error("Browser scrape failed: %s", e)
        finally:
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


def scrape_details(supplier_name="reytech", from_date="01/01/2024",
                   max_rows=5):
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
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        page = await browser.new_page()
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
            await browser.close()

    return None
