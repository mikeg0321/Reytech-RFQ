# Reytech RFQ Dashboard — Deployment Guide

Deploy your bid automation dashboard at `bid.reytechinc.com` (password-protected, hidden from public).

---

## Overview

Your Wix site stays exactly as-is. The dashboard runs on **Railway** ($5/mo), a cloud platform that hosts Python apps. You'll point a subdomain (`bid.reytechinc.com`) to it. Nobody can find or access it without the password.

**What you'll need:**
- A GitHub account (free)
- A Railway account (free to sign up, $5/mo hobby plan)
- Your Gmail App Password
- Access to your domain's DNS settings (wherever you registered reytechinc.com)

**Total time: ~20 minutes**

---

## Step 1: Get a Gmail App Password

This lets the dashboard read your inbox for RFQ emails and send bid responses.

1. Go to https://myaccount.google.com/security
2. Make sure **2-Step Verification** is ON (required for app passwords)
3. Go to https://myaccount.google.com/apppasswords
4. Select app: **Mail**, device: **Other** → type "Reytech Dashboard"
5. Click **Generate**
6. Copy the 16-character password (like `abcd efgh ijkl mnop`)
7. Save it somewhere — you'll need it in Step 4

**Note:** If using rfq@reytechinc.com through Google Workspace, do this on that account. If it's a forwarding alias, use the actual Gmail account that receives the mail.

---

## Step 2: Push Code to GitHub

1. Go to https://github.com/new
2. Repository name: `reytech-rfq` (set to **Private**)
3. Click **Create repository**
4. On your computer, open Terminal (Mac) or Command Prompt (Windows):

```bash
# Navigate to where you downloaded the reytech_system folder
cd path/to/reytech_system

# Initialize git and push
git init
git add .
git commit -m "Reytech RFQ Dashboard v2"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/reytech-rfq.git
git push -u origin main
```

**Don't have git?** Download from https://git-scm.com/ or use GitHub Desktop (https://desktop.github.com/) — it has a GUI for all of this.

---

## Step 3: Deploy on Railway

1. Go to https://railway.app and sign up (use GitHub login — easiest)
2. Click **New Project** → **Deploy from GitHub repo**
3. Select your `reytech-rfq` repository
4. Railway will auto-detect Python and start building

**Wait for the build to finish** (usually 1-2 minutes). You'll see a green checkmark.

---

## Step 4: Set Environment Variables

This is where your passwords go. They're encrypted and never visible in the code.

1. In Railway, click on your deployment
2. Go to **Variables** tab
3. Click **New Variable** and add these one by one:

| Variable | Value |
|----------|-------|
| `DASH_USER` | `reytech` (or whatever username you want) |
| `DASH_PASS` | Pick a strong password for the dashboard login |
| `GMAIL_ADDRESS` | `rfq@reytechinc.com` |
| `GMAIL_PASSWORD` | The 16-char app password from Step 1 |
| `SECRET_KEY` | Any random string (like `reytech-bid-2026-xyz`) |

4. Railway will auto-redeploy with the new variables

---

## Step 5: Add Persistent Storage

So your price database and RFQ data survive redeployments:

1. In your Railway project, click **+ New** → **Volume**
2. Mount path: `/app/data`
3. Size: 1 GB (more than enough)
4. Click **Create**

---

## Step 6: Get Your URL

1. In Railway, go to **Settings** → **Networking**
2. Click **Generate Domain** — you'll get something like `reytech-rfq-production.up.railway.app`
3. Test it! Open that URL in your browser
4. You should see a login prompt → enter your DASH_USER and DASH_PASS
5. The dashboard should load with the green "Polling" indicator

---

## Step 7: Connect Your Custom Domain (bid.reytechinc.com)

1. In Railway **Settings** → **Networking** → **Custom Domain**
2. Type: `bid.reytechinc.com` and click **Add**
3. Railway will show you a **CNAME record** to add

Now go to your domain registrar (wherever you bought reytechinc.com — GoDaddy, Namecheap, Google Domains, Cloudflare, etc.):

4. Go to **DNS Settings**
5. Add a new record:
   - **Type:** CNAME
   - **Name/Host:** `bid`
   - **Value/Points to:** (the value Railway gave you, like `reytech-rfq-production.up.railway.app`)
   - **TTL:** Auto or 300
6. Save

**Wait 5-30 minutes** for DNS to propagate, then test: https://bid.reytechinc.com

---

## Step 8: Verify Everything Works

1. Open https://bid.reytechinc.com
2. Login with your username/password
3. Check the green polling dot in the header (means email checking is active)
4. **Test upload:** Download the 3 PDFs from a real RFQ email, drag onto the dashboard
5. **Test email polling:** Forward an old RFQ email to rfq@reytechinc.com, wait 2 minutes, refresh the dashboard — it should appear automatically

---

## Daily Workflow

1. RFQ email arrives → dashboard auto-detects it (📧 badge)
2. Open https://bid.reytechinc.com on your phone or computer
3. Click the new RFQ → see line items pre-parsed
4. Enter your supplier costs
5. Hit a Quick Markup button (+25%) or SCPRS Undercut (-1%)
6. Click **⚡ Generate Bid Package**
7. Review the draft email → click **📤 Send** or **📋 Open in Mail App**
8. Done. Bid submitted.

---

## Troubleshooting

**"Email not configured" / yellow dot:**
- Double check GMAIL_PASSWORD is set in Railway Variables
- Make sure it's the App Password (16 chars), not your regular Gmail password
- Verify 2-Step Verification is enabled on the Gmail account

**Login not working:**
- Check DASH_USER and DASH_PASS in Railway Variables
- Clear browser cache or try incognito mode

**RFQ not parsing correctly:**
- Make sure all 3 PDFs are from the same RFQ (matching solicitation numbers)
- The 704B is the key file — the parser needs it

**"SCPRS prices not found":**
- Normal for first few bids — prices build up over time in your local DB
- Enter SCPRS prices manually → they get cached for future lookups
- Use the 🔍 button to try Cal eProcure search

**Data lost after redeploy:**
- Make sure you added the persistent Volume (Step 5)
- Mount path must be `/app/data`

---

## Costs

| Service | Cost |
|---------|------|
| Railway Hobby Plan | $5/mo |
| Custom domain | Already have it |
| Gmail | Free (or included in Workspace) |
| **Total** | **$5/mo** |

---

## Security

- Dashboard is **password-protected** (HTTP Basic Auth over HTTPS)
- Not linked from your Wix site — no one knows it exists
- Passwords stored as encrypted **environment variables** (not in code)
- GitHub repo is **private**
- Railway provides **HTTPS by default** (SSL certificate automatic)
- Gmail App Password has limited scope (can only send/read email)
