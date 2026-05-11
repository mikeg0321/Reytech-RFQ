"""Sanity-check the new v_revenue_year_2026 logic against current data.

Run this on prod to confirm migration 42 produces the expected ~$108K
real-2026 number instead of the inflated $1.74M from the bulk import.
"""
from src.core.db import get_db


SQL = """
WITH order_year AS (
    SELECT id, total AS amount, created_at, po_date, quote_number,
           po_number, status, agency, is_test,
           COALESCE(
               NULLIF(substr(po_date, 1, 4), ''),
               CASE
                   WHEN quote_number GLOB 'R[0-9][0-9][QO]*'
                       THEN '20' || substr(quote_number, 2, 2)
                   ELSE NULL
               END,
               substr(created_at, 1, 4)
           ) AS real_year
    FROM orders
    WHERE COALESCE(is_test, 0) = 0
      AND COALESCE(po_number, '') NOT IN ('TEST', 'test')
      AND COALESCE(quote_number, '') NOT IN ('TEST', 'test')
),
order_dedup AS (
    SELECT id, amount, created_at AS dated_at, quote_number, po_number,
           status, agency
    FROM order_year
    WHERE real_year = '2026'
      AND id IN (
          SELECT MIN(id) FROM order_year
          WHERE real_year = '2026'
          GROUP BY COALESCE(NULLIF(po_number, ''), id),
                   COALESCE(agency, ''), ROUND(COALESCE(amount, 0), 2)
      )
)
SELECT id, ROUND(amount, 2) AS amt, dated_at, po_number, status, agency,
       quote_number
FROM order_dedup
ORDER BY amt DESC
"""


def main():
    with get_db() as conn:
        rows = conn.execute(SQL).fetchall()
        print("=== orders counted as 2026 after migration 42 ===")
        total = 0.0
        for r in rows:
            d = dict(r)
            total += d["amt"]
            print(d)
        print()
        print("ROW COUNT: %d" % len(rows))
        print("TOTAL 2026 order revenue: $%s" % f"{total:,.2f}")


if __name__ == "__main__":
    main()
