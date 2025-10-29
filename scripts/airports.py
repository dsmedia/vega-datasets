"""
Generate airports.csv from BTS Aviation Facilities data.

This script retrieves airport data from the Bureau of Transportation Statistics (BTS)
National Transportation Atlas Database (NTAD). The Aviation Facilities dataset provides
information about airport locations and attributes for airports in the United States and
its territories.

The output includes major commercial, regional, and municipal airports with their IATA codes,
names, locations, and geographic coordinates.

Data Source
-----------
The dataset is available from:
- BTS Geospatial Portal: https://geodata.bts.gov/datasets/usdot::aviation-facilities
- Data.gov: https://catalog.data.gov/dataset/airports-5e97a

Expected Schema
---------------
The current airports.csv contains the following fields:
- iata: IATA airport code (e.g., "JFK", "LAX")
- name: Airport name
- city: City where airport is located
- state: State/territory abbreviation
- country: Country code (typically "USA")
- latitude: Latitude coordinate (decimal degrees)
- longitude: Longitude coordinate (decimal degrees)

Notes
-----
The BTS Aviation Facilities dataset may contain additional fields that are not included
in the output. The script filters and transforms the source data to match the expected
schema above.

As the original data source may have evolved since the airports.csv file was created,
field mappings may need adjustment to achieve an exact match.
"""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.error import HTTPError
from urllib.request import urlopen

if TYPE_CHECKING:
    from typing import Any

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Constants
REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "data"
OUTPUT_FILE = DATA_DIR / "airports.csv"

# BTS Aviation Facilities data source - ArcGIS REST API
API_BASE_URL = (
    "https://services.arcgis.com/xOi1kZaI0eWDREZv/ArcGIS/rest/services/"
    "NTAD_Aviation_Facilities/FeatureServer/0/query"
)
# Filter for airports only (SITE_TYPE_CODE='A')
AIRPORT_FILTER = "SITE_TYPE_CODE='A'"
# Pagination settings
PAGE_SIZE = 2000

# Field mappings from source data to output schema
# Based on actual BTS NTAD Aviation Facilities API response fields
FIELD_MAPPINGS = {
    "iata": "ARPT_ID",  # FAA Location ID (e.g., "00M", "JFK") - called "iata" for compatibility
    "name": "ARPT_NAME",  # Airport facility name
    "city": "CITY",  # City name
    "state": "STATE_CODE",  # State abbreviation (2-letter code)
    "country": "COUNTRY_CODE",  # Country code (will be converted from "US" to "USA")
    "latitude": "LAT_DECIMAL",  # Latitude in decimal degrees
    "longitude": "LONG_DECIMAL",  # Longitude in decimal degrees
}

# Expected output columns
OUTPUT_COLUMNS = ["iata", "name", "city", "state", "country", "latitude", "longitude"]


def fetch_airport_data() -> list[dict[str, Any]]:
    """
    Fetch airport data from BTS Aviation Facilities dataset via ArcGIS REST API.

    Retrieves all airport records (SITE_TYPE_CODE='A') from the BTS NTAD Aviation
    Facilities service using pagination to handle the large dataset.

    Returns
    -------
    list[dict[str, Any]]
        List of airport feature records in GeoJSON format. Each feature contains
        'properties' (attributes) and 'geometry' (coordinates).

    Raises
    ------
    HTTPError
        If the data source is unavailable or returns an error.
    json.JSONDecodeError
        If the API response is not valid JSON.

    Notes
    -----
    The API is paginated with PAGE_SIZE records per request. This function:
    1. First queries for total record count
    2. Iterates through pages using resultOffset
    3. Collects all features into a single list
    4. Returns GeoJSON features with properties and geometry
    """
    logger.info("Fetching airport data from BTS Aviation Facilities API")

    all_features = []

    # Step 1: Get total record count
    from urllib.parse import urlencode

    count_params = urlencode({
        "where": AIRPORT_FILTER,
        "returnCountOnly": "true",
        "f": "json",
    })
    count_url = f"{API_BASE_URL}?{count_params}"

    logger.info("Querying total record count...")
    with urlopen(count_url, timeout=30) as response:
        count_data = json.loads(response.read().decode("utf-8"))
        total_records = count_data.get("count")

    if total_records is None:
        msg = "Could not determine total record count from API"
        raise RuntimeError(msg)

    logger.info("Total airport records to download: %d", total_records)

    # Step 2: Paginate through all records
    for offset in range(0, total_records, PAGE_SIZE):
        query_params = urlencode({
            "where": AIRPORT_FILTER,
            "outFields": "*",
            "f": "geojson",
            "resultOffset": str(offset),
            "resultRecordCount": str(PAGE_SIZE),
        })
        query_url = f"{API_BASE_URL}?{query_params}"

        logger.info(
            "Downloading records %d to %d of %d...",
            offset,
            min(offset + PAGE_SIZE, total_records),
            total_records,
        )

        with urlopen(query_url, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))
            page_features = data.get("features", [])

            if not page_features:
                logger.warning("No features returned at offset %d, stopping pagination", offset)
                break

            all_features.extend(page_features)

    logger.info("Download complete. Total features retrieved: %d", len(all_features))
    return all_features


def transform_record(record: dict[str, Any]) -> dict[str, str]:
    """
    Transform a GeoJSON feature record to the expected output schema.

    Parameters
    ----------
    record : dict[str, Any]
        GeoJSON feature from the BTS API with 'properties' and 'geometry' keys.

    Returns
    -------
    dict[str, str]
        Transformed record with standardized field names matching OUTPUT_COLUMNS.

    Notes
    -----
    - Extracts attributes from 'properties' key in GeoJSON feature
    - Extracts coordinates from 'geometry.coordinates' (GeoJSON format: [lon, lat])
    - Falls back to coordinate fields in properties if geometry is not available
    - Defaults country to "USA" if not present (BTS data is US-focused)
    - Returns empty strings for missing fields
    """
    # GeoJSON structure: {"type": "Feature", "properties": {...}, "geometry": {...}}
    properties = record.get("properties", {})
    geometry = record.get("geometry", {})

    # Extract fields using mappings
    transformed = {}
    for output_field, source_field in FIELD_MAPPINGS.items():
        value = properties.get(source_field, "")

        # Handle special cases
        if output_field == "country":
            # Convert country code from "US" to "USA" for consistency
            if value == "US":
                value = "USA"
            elif not value:
                value = "USA"  # Default to USA for BTS data

        # Handle latitude/longitude from GeoJSON geometry
        # GeoJSON Point format: {"type": "Point", "coordinates": [longitude, latitude]}
        if output_field in ["latitude", "longitude"] and not value:
            if geometry and geometry.get("type") == "Point":
                coordinates = geometry.get("coordinates", [])
                if len(coordinates) >= 2:
                    if output_field == "latitude":
                        value = coordinates[1]  # GeoJSON: [lon, lat]
                    else:  # longitude
                        value = coordinates[0]

        transformed[output_field] = str(value) if value else ""

    return transformed


def filter_valid_airports(records: list[dict[str, str]]) -> list[dict[str, str]]:
    """
    Filter records to include only valid airports with required fields.

    Parameters
    ----------
    records : list[dict[str, str]]
        Transformed airport records.

    Returns
    -------
    list[dict[str, str]]
        Filtered list containing only airports with IATA codes and valid coordinates.

    Notes
    -----
    Filters out records that:
    - Have missing or empty IATA codes
    - Have invalid or missing coordinates
    - Are not passenger airports (if applicable)
    """
    valid_records = []
    for record in records:
        # Check for required fields
        if not record.get("iata"):
            continue
        if not record.get("latitude") or not record.get("longitude"):
            continue

        # Optionally validate coordinate ranges
        try:
            lat = float(record["latitude"])
            lon = float(record["longitude"])
            if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                continue
        except (ValueError, TypeError):
            continue

        valid_records.append(record)

    logger.info("Filtered %d valid airports from source data", len(valid_records))
    return valid_records


def write_airports_csv(records: list[dict[str, str]]) -> None:
    """
    Write airport data to CSV file.

    Parameters
    ----------
    records : list[dict[str, str]]
        Airport records to write.

    Notes
    -----
    - Ensures output directory exists
    - Sorts airports by IATA code for consistency
    - Uses Unix line endings (LF)
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Sort by IATA code for consistent output
    sorted_records = sorted(records, key=lambda x: x.get("iata", ""))

    logger.info("Writing %d airports to %s", len(sorted_records), OUTPUT_FILE)

    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(sorted_records)

    logger.info("Successfully wrote airports.csv")


def main() -> None:
    """
    Main entry point for airport data generation.

    Orchestrates the complete workflow:
    1. Fetch data from BTS Aviation Facilities
    2. Transform records to expected schema
    3. Filter for valid airports
    4. Write to CSV file
    """
    logger.info("Starting airport data generation")

    try:
        # Step 1: Fetch data
        raw_records = fetch_airport_data()
        logger.info("Fetched %d records from source", len(raw_records))

        # Step 2: Transform records
        transformed_records = [transform_record(record) for record in raw_records]
        logger.info("Transformed %d records", len(transformed_records))

        # Step 3: Filter valid airports
        valid_airports = filter_valid_airports(transformed_records)

        # Step 4: Write to CSV
        write_airports_csv(valid_airports)

        logger.info("Airport data generation complete")

    except HTTPError as e:
        logger.exception("Failed to fetch data from source: %s", e)
        raise
    except NotImplementedError as e:
        logger.error("Implementation incomplete: %s", e)
        logger.info(
            "To complete this script, update fetch_airport_data() with actual BTS API endpoint"
        )
        raise
    except Exception:
        logger.exception("Unexpected error during airport data generation")
        raise


if __name__ == "__main__":
    main()
