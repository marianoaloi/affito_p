# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python-based real estate listing scraper and synchronization system. It fetches rental property listings from the Immobiliare.it API for Udine and Trieste regions in Italy, and synchronizes them with a MongoDB Atlas database using X.509 certificate authentication.

## Architecture

### Core Components

**fetch_and_save.py** - Main orchestrator that:
- Fetches listings from two regions (Udine and Trieste) using `ListingFetcher`
- Connects to MongoDB Atlas using X.509 certificate authentication
- Calls `compare_and_sync()` to intelligently update the database
- Handles connection lifecycle and error scenarios

**listing_fetcher.py** - API client that:
- Handles paginated API requests to Immobiliare.it
- Implements rate limiting (1-4 second delays between requests)
- Extracts `realEstate.id` from API responses and assigns as MongoDB `_id`
- Adds `mLastUpdate` timestamp to each listing

### Data Synchronization Logic

The `compare_and_sync()` function implements soft-delete synchronization:
- **New listings**: Upserted with `$set` operation and `$unset` to clear any `deleted` flag
- **Existing listings**: Updated in place, `deleted` flag removed if present
- **Missing listings**: Marked with `deleted: true` (soft delete) rather than hard deleted
- **Create dates**: New documents get `mCreateDate` set via a second bulk write operation after upserts

Key behavior: The `mLastUpdate` field is removed before upserting (line fetch_and_save.py:43) to prevent timestamp pollution in the database.

## Running the Code

### Setup
```bash
# Activate virtual environment
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

# Install dependencies (no requirements.txt present, main deps are):
pip install pymongo requests
```

### Execution
```bash
# Run the main scraper
python fetch_and_save.py

# Run the fetcher standalone (example mode)
python listing_fetcher.py
```

### Testing
```bash
# Run all tests
python -m unittest discover -s __tests__ -p "*.test.py"

# Run specific test file
python -m unittest __tests__.fetch_and_save.test
```

## Configuration

All configuration is hardcoded in `fetch_and_save.py` (lines 11-15):
- `URL` and `URL_TS`: Immobiliare.it API endpoints for Udine and Trieste
- `MONGO_URI`: MongoDB Atlas connection string
- `DATABASE_NAME`: "udine"
- `COLLECTION_NAME`: "affito"
- Certificate path: `X509-cert-2864290664025085959.pem` (must be in project root)

## Important Implementation Details

### MongoDB Authentication
Uses X.509 certificate authentication (lines fetch_and_save.py:88-91). The certificate file must exist at the project root.

### API Rate Limiting
The fetcher uses random delays between 1-4 seconds (listing_fetcher.py:29) to avoid overwhelming the server. Do not reduce these delays.

### Soft Delete Pattern
Documents are never hard-deleted from MongoDB. Instead, they're marked with `deleted: true`. This preserves historical data and allows for potential recovery. Queries should filter `{"deleted": {"$exists": False}}` to exclude deleted documents.

### Bulk Operations
The sync uses two bulk write operations:
1. Upserts for all listings with `$set` and `$unset` operations
2. `$set mCreateDate` only for newly inserted documents (identified via `result.upserted_ids`)

This two-phase approach ensures `mCreateDate` is only set once per listing, never updated.
