# 📰 Targeted News Monitoring Pipeline

A Python pipeline using web scraping and LLM-based risk categorisation and summarisation. 

The system allows analysts to detect emerging risks such as supply chain disruptions, regulatory changes and geopolitical events more efficiently. It is particularly useful in regions with many non-English sources because LLMs are excellent at simultaneously translating and summarizing raw news content. Risk detection can be customised based on the entity of concern (e.g. a logistics firm), risk type (e.g. transport disruption events) and confidence rate (e.g. 95%). 

**Key technologies:** Python, BeautifulSoup, Pandas, Gemini API, prompt engineering.


## 🔍 Overview

The pipeline performs the following steps:

1. Scrapes headlines from multiple news sources
2. Deduplicates headlines against an SQLite database
3. Uses an LLM to identify risk-relevant headlines
4. Scrapes full article texts for flagged stories
5. Uses a two-stage LLM summarisation process to generate a final summary
6. Saves processed headlines to the database

This allows large volumes of news to be processed efficiently.


## 🧪 Example Flow

```
links.csv
     │
     ▼
scrape_headlines
     │
     │ Example output (Spanish):
     │ [
     │   "Sindicato ferroviario anuncia protestas nacionales",
     │   "Paro portuario en Buenaventura amenaza exportaciones",
     │   "Aumentan las exportaciones de café pese a retrasos logísticos",
     │   ...
     │ ]
     │
     ▼
deduplicate_headlines
     │
     │ Example output (Spanish):
     │ [
     │   "Paro portuario en Buenaventura amenaza exportaciones",
     │   "Aumentan las exportaciones de café pese a retrasos logísticos",
     │   ...
     │ ]
     │
     ▼
identify_risk_headlines
     │
     │ Example output (headline indices):
     │ [
     │   0, 
     │   7, 
     │   ...
     │ ]
     │
     ▼
scrape_stories
     │
     │ Example output (Spanish):
     │ [
     │   "Trabajadores portuarios en Buenaventura iniciaron un paro...",
     │   "Autoridades reportan retrasos en la cadena logística tras bloqueo...",
     │   ...
     │ ]
     │
     ▼
summarise_stories
     │
     │ Example output (English):
     │ "Labour disputes in Colombia's port and rail sectors may disrupt
     │ freight movement and export logistics in the coming days. Also..."
     │
     ▼
store_headlines
```


## 🗂️ Project Structure

```text
targeted-news-monitoring-pipeline/
│
├── main.py
├── config.py
├── links.csv
├── .env.example
├── pytest.ini
│
├── data/
│   └── .gitkeep
│
├── utils/
│   ├── __init__.py
│   └── database.py
│
├── news_monitoring_pipeline/
│   ├── __init__.py
│   ├── build_prompts.py
│   ├── scrape_headlines.py
│   ├── deduplicate_headlines.py
│   ├── identify_risk_headlines.py
│   ├── scrape_stories.py
│   ├── summarise_stories.py
│   └── store_headlines.py
│
└── tests/
    ├── utils/
    │   └── test_database.py
    │
    └── news_monitoring_pipeline/
        ├── test_scrape_headlines.py
        ├── test_identify_risk_headlines.py
        ├── test_scrape_stories.py
        └── test_summarise_stories.py
```


## 🚀 Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/jcarterlab/Targeted-News-Monitoring-Pipeline.git
cd Targeted-News-Monitoring-Pipeline

# 2. Create a virtual environment
python -m venv .venv

# 3. Activate the virtual environment (run the command for your OS)

# macOS / Linux
source .venv/bin/activate

# Windows (PowerShell)
.venv\Scripts\Activate.ps1

# 4. Install dependencies
pip install -r requirements.txt

# 5. Create a .env file from the template and enter your Gemini API key
cp .env.example .env

# 6. Run the pipeline 
python main.py
```

The pipeline will run using the example news sources provided in `links.csv` and default configuration. 

## ⚙️ Custom Configuration

The pipeline supports three levels of customisation:

### 1. News sources

Edit `links.csv` to provide the news listing URLs and the CSS selectors used to extract headlines and article content. Each row represents a news source the pipeline will monitor.

Example:

```
┌───────────┬───────────────────────────┬───────────────────┬─────┬───────────┬─────────────┐
│ website   │ page_url                  │ base_url          │ tag │ story_tag │ story_class │
├───────────┼───────────────────────────┼───────────────────┼─────┼───────────┼─────────────┤
│ El Tiempo │ www.eltiempo.com/colombia │ www.eltiempo.com/ │ a   │ div       │ paragraph   │
│ ...       │ ...                       │ ...               │ ... │ ...       │ ...         │
└───────────┴───────────────────────────┴───────────────────┴─────┴───────────┴─────────────┘
```

### 2. Risk detection parameters

Edit `.env` to define:

- **Entity of concern** (e.g. a logistics firm operating in Colombia)
- **Risk type** (e.g. transport disruption events)
- **Confidence threshold** for LLM classification (e.g. 95%)

Example:

```env
ENTITY_OF_CONCERN=a logistics firm
RISK_TYPE=transport disruption events
RISK_CONFIDENCE_THRESHOLD=95
```

### 3. Optional pipeline parameters

Edit `.env` to define:

- **Request timeout** for scraping (e.g. 10 seconds)
- **Minimum headline length** for filtering non-headlines (e.g. 25 characters)
- **Headline batch size** for LLM classification (e.g. 40 headlines)
- **Retry attempts** for failed LLM API calls before moving on (e.g. 3 attempts)
- **LLM wait time** between API calls (e.g. 10 seconds)
- **Basic model** for less complex tasks (e.g. gemini-2.5-flash)
- **Advanced model** for more complex tasks (e.g. gemini-2.5-pro)
- **Story words batch size** for LLM summarisation (e.g. 12,000 words)

Example:

```env
REQUEST_TIMEOUT=10
MIN_HEADLINE_LENGTH=25
LLM_HEADLINE_BATCH_SIZE=40
LLM_RETRY_ATTEMPTS=3
LLM_WAIT_TIME=10
BASIC_MODEL=gemini-2.5-flash
ADVANCED_MODEL=gemini-2.5-pro
LLM_STORY_WORDS_BATCH_SIZE=12000
```


## 📐 Architectural Advantages

- **Headline deduplication**  
Previously processed headlines are dropped by comparing new headlines against a database of those successfully processed in past runs (`processed_headlines.db`), reducing unnecessary re-processing. 

- **Batch headline identification**  
Scraped headlines are combined into numbered batches before a lightweight LLM is instructed to return only the indicies of potential risk headlines, decreasing the number of LLM calls needed for categorisation.

- **Two-stage LLM summarisation**  
Scraped story text is summarised by a lightweight LLM in batches before an advanced LLM is told to produce a final executive summary using the expert judgement of a head analyst, minimizing irrelevant details. 


## 📃 License

This project is licensed under the Apache 2.0 License - see the [LICENSE](https://github.com/jcarterlab/Targeted-News-Monitoring-Pipeline/blob/main/LICENSE) file for details.