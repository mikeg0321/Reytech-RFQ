#!/usr/bin/env python3
"""
mcp_server.py — Claude MCP tool server for Reytech RFQ.

Registers 5 tools that call the Reytech RFQ API endpoints.
Run: python mcp_server.py (or via Claude Desktop config)

Requires: API_KEY env var set, app running at REYTECH_URL.
"""
import os
import json
import logging
import httpx
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

log = logging.getLogger("reytech.mcp")

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY = os.environ.get("API_KEY", "")
REYTECH_URL = os.environ.get("REYTECH_URL",
    os.environ.get("BASE_URL", "http://localhost:5000"))

server = Server("reytech-rfq")


def _headers():
    return {"X-API-Key": API_KEY, "Content-Type": "application/json"}


def _get(path: str) -> dict:
    """GET request to Reytech API."""
    resp = httpx.get(f"{REYTECH_URL}{path}", headers=_headers(), timeout=15)
    resp.raise_for_status()
    return resp.json()


def _post(path: str, data: dict = None) -> dict:
    """POST request to Reytech API."""
    resp = httpx.post(f"{REYTECH_URL}{path}", headers=_headers(),
                      json=data or {}, timeout=15)
    resp.raise_for_status()
    return resp.json()


@server.list_tools()
async def list_tools():
    return [
        Tool(
            name="get_rfq",
            description="Get a single RFQ by ID including all line items",
            inputSchema={
                "type": "object",
                "properties": {
                    "rfq_id": {"type": "string", "description": "The RFQ ID (8-char hex)"}
                },
                "required": ["rfq_id"]
            }
        ),
        Tool(
            name="get_pipeline",
            description="Get current queue depths and agent status across the full RFQ pipeline",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="trigger_pricing",
            description="Trigger automated pricing on an existing RFQ",
            inputSchema={
                "type": "object",
                "properties": {
                    "rfq_id": {"type": "string", "description": "The RFQ ID to price"},
                    "force": {"type": "boolean", "description": "Force re-price even if already priced", "default": False}
                },
                "required": ["rfq_id"]
            }
        ),
        Tool(
            name="get_health",
            description="Get full system health — DB status, agent last run times, queue depths. Call this first to confirm the app is healthy before taking any action.",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="create_rfq",
            description="Create a new RFQ manually with line items",
            inputSchema={
                "type": "object",
                "properties": {
                    "solicitation_number": {"type": "string"},
                    "agency": {"type": "string"},
                    "requestor_name": {"type": "string"},
                    "requestor_email": {"type": "string"},
                    "due_date": {"type": "string", "description": "ISO date (YYYY-MM-DD)"},
                    "ship_to": {"type": "string"},
                    "line_items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "qty": {"type": "integer"},
                                "uom": {"type": "string"},
                                "description": {"type": "string"},
                                "unit_price": {"type": "number"}
                            }
                        }
                    }
                },
                "required": ["solicitation_number", "agency"]
            }
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict):
    try:
        if name == "get_rfq":
            result = _get(f"/api/v1/rfq/{arguments['rfq_id']}")
        elif name == "get_pipeline":
            result = _get("/api/v1/pipeline")
        elif name == "trigger_pricing":
            result = _post(f"/api/v1/rfq/{arguments['rfq_id']}/price",
                          {"force": arguments.get("force", False)})
        elif name == "get_health":
            result = _get("/api/v1/health")
        elif name == "create_rfq":
            payload = {
                "solicitation_number": arguments.get("solicitation_number", ""),
                "agency": arguments.get("agency", ""),
                "requestor_name": arguments.get("requestor_name", ""),
                "requestor_email": arguments.get("requestor_email", ""),
                "due_date": arguments.get("due_date", ""),
                "ship_to": arguments.get("ship_to", ""),
                "items": arguments.get("line_items", []),
            }
            result = _post("/api/v1/rfq/create", payload)
        else:
            return [TextContent(type="text", text=json.dumps({"error": f"Unknown tool: {name}"}))]

        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
    except httpx.HTTPStatusError as e:
        return [TextContent(type="text", text=json.dumps({
            "error": f"HTTP {e.response.status_code}",
            "detail": e.response.text[:500]
        }))]
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({
            "error": str(e)
        }))]


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
