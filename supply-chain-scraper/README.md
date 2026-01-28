Findchips Supply Chain Scraper
Overview

The Findchips Supply Chain Scraper is a high-performance, multi-threaded Selenium-based web scraping system designed to extract electronic component supply chain intelligence from Findchips.com.

The scraper systematically traverses Findchips parametric categories, identifies valid Manufacturer Part Numbers (MPNs), and captures real-time distributor pricing, stock availability, lead time, packaging, MOQ, and regional metadata.
Data is continuously persisted as date-stamped CSV files for downstream analytics and dashboarding (e.g., Power BI).

Key Capabilities

🔹 Automated discovery of Findchips parametric categories

🔹 Recursive category traversal with loop prevention

🔹 Robust MPN identification using multi-layer validation

🔹 Distributor-level extraction with price breaks and stock

🔹 Multi-threaded execution (6 parallel workers)

🔹 Thread-safe data collection and persistence

🔹 Automatic background CSV auto-save (every 5 minutes)

🔹 Strong data cleaning and normalization logic

🔹 Designed for large-scale scraping (hundreds of thousands of rows)

Technology Stack
Component	Description
Language	Python 3.8.10
Scraping Engine	Selenium (Chrome WebDriver)
Concurrency	Python threading
Storage	CSV (append-only, date-partitioned)
Parsing	Regex-based structured extraction
OS Compatibility	Windows / Linux
Project Structure
supply-chain-scraper/
│
├── main.py                # Core scraper implementation
├── requirements.txt       # External dependencies
├── pyproject.toml         # Python version constraint
├── output/
│   └── stock_YYYYMMDD.csv # Daily output files
└── README.md

Installation & Setup
1️⃣ Python Version

This project is locked to:

Python 3.8.10


Verify your version:

python --version

2️⃣ Create Virtual Environment (Recommended)
python -m venv venv
venv\Scripts\activate   # Windows
# source venv/bin/activate  # Linux / Mac

3️⃣ Install Dependencies
pip install -r requirements.txt


requirements.txt

selenium==4.40.0


All other imports are part of Python’s standard library.

4️⃣ Chrome & WebDriver

Google Chrome must be installed

Selenium Manager automatically resolves ChromeDriver (no manual setup required)

Execution

Run the scraper using:

python main.py

Runtime Behavior

Discovers all main parametric categories from Findchips

Divides categories across 6 worker threads

Each thread:

Opens its own browser instance

Recursively traverses categories

Extracts valid MPNs

Scrapes distributor pricing & inventory data

Data is:

Stored in memory (thread-safe)

Periodically auto-saved every 5 minutes

Final CSV is written on completion

Output Data
File Naming Convention
output/stock_YYYYMMDD.csv


Example:

stock_20260121.csv

Output Columns
Column	Description
MPN	Manufacturer Part Number
Price_Qty	Price break quantity
Unit_Price	Unit price at break
MFG_Name	Manufacturer
Supplier_Name	Distributor
MFG_Lead_Time	Lead time (weeks)
On_Hand_Stock	Available inventory
Stock_Per_Price_Break	Stock at pricing tier
Packaging_Type	Reel, Tape, Bulk, etc.
Date_Code	Manufacturing date code
COO	Country of Origin
MOQ	Minimum Order Quantity
Currency	Price currency
Main_Category	Top-level category
Distributor_Block	Raw distributor text
Disti_Part_Number	Distributor part number
Region	Americas / Europe / Asia
scrape_time	Timestamp of extraction
Data Quality & Validation

The scraper includes defensive validation to ensure accuracy:

Eliminates UI noise and parametric artifacts

Cross-validates MPNs using:

URL paths

Page titles

Page text

Manufacturer name strict validation (regex-based)

Country of Origin normalization

Price and stock multi-fallback extraction logic

Performance & Scalability

Parallelized across 6 independent WebDriver instances

Designed for long-running execution

Memory-safe auto-save ensures no data loss

Tested for large category trees and deep recursion

Known Limitations

Dependent on Findchips site structure (CSS changes may require updates)

Selenium-based (slower than API-based solutions)

Intended for data intelligence, not real-time trading systems

Intended Use Cases

Supply chain risk analysis

Procurement intelligence dashboards

Price fluctuation monitoring

Lead time trend analysis

Distributor availability comparison

Power BI / BI tool ingestion

Compliance & Ethics

This scraper:

Does not bypass authentication

Uses standard browser automation

Respects reasonable scraping intervals

Intended for research and analytics purposes

Users are responsible for complying with Findchips’ terms of service.

Author Notes

This project demonstrates:

Advanced Selenium usage

Thread-safe data engineering

Real-world web scraping robustness

Production-oriented data extraction design