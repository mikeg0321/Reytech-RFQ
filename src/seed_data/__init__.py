"""Seed data bundled with the application image.

These files live at /app/src/seed_data/ inside the Railway container and are
NEVER shadowed by the volume mount (which is at /app/data/).

Migration reads from here on first boot to populate the SQLite database even
when the volume is brand-new and empty.
"""
