News Aggregator CLI

A simple and efficient Command-Line Interface (CLI) tool that aggregates news headlines from multiple sources using web scraping or NewsAPI.
Supports filtering, exporting, deduplication, and saving results for later use.

🚀 Features
✔ Fetch News From Multiple Sources

Pull headlines via web scraping or API calls

Combine all results into a single clean dataset
✔ CLI Filters
Filter by source
Filter by keyword
Filter by date
✔ Data Storage
Store aggregated results in JSON or SQLite
Load previously saved data for offline queries
✔ Export Options
Export filtered or full dataset to:
CSV
Excel (.xlsx)
✔ Deduplication
Automatically remove duplicate headlines

🛠️ Tech Stack
Python
Requests / BeautifulSoup (for scraping)
NewsAPI (optional)
SQLite / JSON
Pandas & OpenPyXL (for exporting)
Argparse (CLI interface)

📈 Future Enhancements
Add sentiment analysis
Build a dashboard version (Tkinter or web app)
Add more advanced filters
Add scheduler for automatic daily fetch
