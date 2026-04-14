# tompickup.co.uk — Claude Code Context

## Overview
Personal website and portfolio for Tom Pickup. Central hub linking all projects.

**Stack**: Python | Flask/FastAPI
**Hosting**: Cloudflare
**Automation**: 2 GitHub Actions (deploy, data-etl)
**Branch**: main

## Key Patterns
- Python web application
- Data ETL pipeline for automated content
- Template-based rendering
- API integrations for dynamic data

## Commands

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python app.py                    # Run locally
```

## Rules
- Never commit .env or secrets
- British English throughout
- Keep the site fast and lightweight
- SEO: proper meta tags on every page
- Mobile-first responsive design
- All data processing in ETL pipeline, not in request handlers

## Related Projects
Links to all 13 tompickup23 GitHub repos — this is the portfolio hub.
