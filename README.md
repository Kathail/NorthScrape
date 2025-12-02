# NorthScrape

**NorthScrape** is a specialized, multi-threaded GUI web scraper designed to generate, clean, and enrich business leads across Northern Ontario. 

It leverages **Tkinter** for the interface and **concurrent threading** to maximize performance on high-core-count CPUs (optimized for Ryzen 9 5900X).

## Features

- **Mass Lead Generation**: Scrapes YellowPages.ca for business names and addresses based on specific categories and Northern Ontario locations.
- **Data Enrichment**: 
  - Cross-references YellowPages and DuckDuckGo (HTML) to find phone numbers and websites.
  - Smart logic to handle redirects and clean URLs.
- **Data Cleaning**:
  - Automatically standardizes phone numbers to `(XXX) XXX-XXXX`.
  - Fixes and standardizes messy address strings (e.g., removing "District", inferring cities from Postal Codes).
- **High-Performance**: Uses a `ThreadPoolExecutor` with 20 workers to handle I/O-bound web requests without freezing the GUI.
- **Quality of Life**:
  - "Open Recent" menu with history persistence.
  - CSV Export/Import.
  - Real-time progress bar and status updates.

## Installation

1. Clone the repository:
   ```bash
   git clone [https://github.com/kathail/northscrape.git](https://github.com/kathail/northscrape.git)
   cd northscrape
