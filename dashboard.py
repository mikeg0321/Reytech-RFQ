@app.route("/api/debug/paths")
@auth_required
def api_debug_paths():
    """Debug: show actual filesystem paths."""
    import won_quotes_db
    results = {
        "BASE_DIR": BASE_DIR,
        "DATA_DIR": DATA_DIR,
        "wq_DATA_DIR": won_quotes_db.DATA_DIR,
        "wq_FILE": won_quotes_db.WON_QUOTES_FILE,
        "cwd": os.getcwd(),
    }
    for p in ["/app/data", "/app", DATA_DIR, won_quotes_db.DATA_DIR]:
        k = p.replace("/", "_").strip("_")
        results[f"{k}_exists"] = os.path.exists(p)
        if os.path.isdir(p):
            try: results[f"{k}_contents"] = os.listdir(p)
            except: results[f"{k}_contents"] = "denied"
    return jsonify(results)
