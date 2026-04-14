# 📰 NewsMonitor

A Python news monitoring pipeline that uses web scraping, LLMs and optional email alerts.

The system allows analysts to detect emerging risks such as supply chain disruptions, regulatory changes and geopolitical events more efficiently. It is particularly useful in regions with many non-English sources because LLMs are excellent at simultaneously translating and summarizing raw news content. News monitoring can be customised based on the topic of concern (e.g. transport disruption events), entity of concern (e.g. a logistics firm operating in Colombia) and confidence threshold (e.g. the LLM must be at least 95% sure a headline is relevant before scraping the story text). 

**Key technologies:** Python, BeautifulSoup, SQLite, Pandas, Google Gemini API, Resend.


## 🔍 Overview

The pipeline performs the following steps:

1. Scrapes headlines from multiple news pages
2. Deduplicates headlines against an SQLite database
3. Uses an LLM to identify risk-relevant headlines
4. Scrapes full article texts for the flagged stories
5. Uses a two-stage LLM summarisation process to generate a final summary
6. Saves the final summary and processed headlines to an SQLite database
7. Optionally sends an email alert to the end user(s)

## 🧪 Example Flow

```
links.csv
     │
     ▼
scrape_headlines
     │
     │ Example output (Spanish headlines):
     │ [
     │   'Sindicato ferroviario anuncia protestas nacionales',
     │   'Paro portuario en Buenaventura amenaza exportaciones',
     │   'Aumentan las exportaciones de café pese a retrasos logísticos',
     │   ...
     │ ]
     │
     ▼
deduplicate_headlines
     │
     │ Example output (deduplicated Spanish headlines):
     │ [
     │   'Paro portuario en Buenaventura amenaza exportaciones',
     │   'Aumentan las exportaciones de café pese a retrasos logísticos',
     │   ...
     │ ]
     │
     ▼
identify_risk_headlines
     │
     │ Example output (risk headline indices):
     │ [
     │   0, 
     │   7, 
     │   ...
     │ ]
     │
     ▼
scrape_stories
     │
     │ Example output (Spanish news story text):
     │ [
     │   'Trabajadores portuarios en Buenaventura iniciaron un paro...',
     │   'Autoridades reportan retrasos en la cadena logística tras bloqueo...',
     │   ...
     │ ]
     │
     ▼
summarise_stories
     │
     │ Example output (English summary in Markdown):
     │ '''
     │   ## Summary of Potential Transport Disruption Risks
     │
     │   ### Ongoing Labour Disputes
     │
     │   Labour disputes in Colombia's port and rail sectors may disrupt
     │   freight movement and export logistics in the coming days. Also...
     │ '''
     │
     ▼
store_data
     │
     │ Example output (SQLite tables):
     │
     │ summaries
     │ ┌─────┬───────────────────────────────────────┬────────────────┬──────────────────────┐
     │ │ id  │ summary_text                          │ date_generated │ risk_type            │
     │ ├─────┼───────────────────────────────────────┼────────────────┼──────────────────────┤
     │ │ 1   │ ## Summary of Potential Transport...  │ 2026-03-31     │ transport disruption │
     │ │ ... │ ...                                   │ ...            │ ...                  │
     │ └─────┴───────────────────────────────────────┴────────────────┴──────────────────────┘
     │
     │ headlines
     │ ┌─────┬───────────────────────────────────┬──────────────────────────────┬────────────┐
     │ │ id  │ headline                          │ link                         │ summary_id │
     │ ├─────┼───────────────────────────────────┼──────────────────────────────┼────────────┤
     │ │ 1   │ Paro portuario en Buenaventura... │ www.eltiempo.com/nacion/     │ 1          │
     │ │ 2   │ Aumentan las exportaciones de...  │ www.portafolio.co/economia/  │ 1          │
     │ │ ... │ ...                               │ ...                          │ ...        │
     │ └─────┴───────────────────────────────────┴──────────────────────────────┴────────────┘
     │
     ▼
email_summary (optional)
     │
     │ Example output (English summary in HTML):
     │ '''
     │   SUMMARY OF POTENTIAL TRANSPORT DISRUPTION RISKS
     │
     │   ── Ongoing Labour Disputes ──
     │
     │   Labour disputes in Colombia's port and rail sectors may disrupt
     │   freight movement and export logistics in the coming days. Also...
     │ '''
     │
     ▼
the end user
```

## 🚀 Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/jcarterlab/NewsMonitor.git
cd NewsMonitor

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

The pipeline will run using the example news sources provided in `links.example.csv` and default configuration. 

## ⚙️ Custom Configuration

The pipeline supports four levels of customisation:

### 1. News sources (recommended)

Create a `links.csv` file from the template and enter the news site URLs and relevant CSS selectors needed to extract headlines and article content. Each row represents a news source the pipeline will monitor.

Example:

```
┌───────────┬───────────────────────────┬───────────────────┬─────┬───────────┬─────────────┐
│ website   │ page_url                  │ base_url          │ tag │ story_tag │ story_class │
├───────────┼───────────────────────────┼───────────────────┼─────┼───────────┼─────────────┤
│ El Tiempo │ www.eltiempo.com/colombia │ www.eltiempo.com/ │ a   │ div       │ paragraph   │
│ ...       │ ...                       │ ...               │ ... │ ...       │ ...         │
└───────────┴───────────────────────────┴───────────────────┴─────┴───────────┴─────────────┘
```

### 2. Monitoring parameters (recommended)

Edit `.env` to define:

- **Topic of concern** (e.g. transport disruption events)
- **Entity of concern** (e.g. a logistics firm operating in Colombia)
- **Confidence threshold** for LLM classification (e.g. 95%)

Example:

```env
TOPIC_OF_CONCERN=transport disruption events
ENTITY_OF_CONCERN=a logistics firm operating in Colombia
IDENTIFICATION_CONFIDENCE_THRESHOLD=95
```

### 3. Pipeline parameters (optional)

Edit `.env` to define:

- **Request heaer** for web scraping (e.g. {"User-Agent": "Mozilla/5.0 (compatible; RiskPipelineBot/1.0)"})
- **Request timeout** for web scraping (e.g. 10 seconds)
- **Minimum headline length** for filtering non-headlines (e.g. 25 characters)
- **Headline batch size** for LLM classification (e.g. 40 headlines)
- **Retry attempts** for failed LLM API calls before moving on (e.g. 3 attempts)
- **LLM wait time** between API calls (e.g. 10 seconds)
- **Basic model** for less complex tasks (e.g. gemini-2.5-flash)
- **Advanced model** for more complex tasks (e.g. gemini-2.5-pro)
- **Story words batch size** for LLM summarisation (e.g. 12,000 words)

Example:

```env
REQUEST_HEADER={"User-Agent": "Mozilla/5.0 (compatible; RiskPipelineBot/1.0)"}
REQUEST_TIMEOUT=10
MIN_HEADLINE_LENGTH=25
LLM_HEADLINE_BATCH_SIZE=40
LLM_RETRY_ATTEMPTS=3
LLM_WAIT_TIME=10
BASIC_MODEL=gemini-2.5-flash
ADVANCED_MODEL=gemini-2.5-pro
LLM_STORY_WORDS_BATCH_SIZE=12000
```

**Note:** Some news websites restrict automated requests. If scraping fails, update the `REQUEST_HEADER` (e.g. `User-Agent`) to mimic a standard browser.

### 4. Email alerts (optional)

To set up email alerts, you must do the following: 

#### 1) Enter your Resend API key in the .env file.

Example:

```env
RESEND_API_KEY=your_api_key_here
```

#### 2) Create an `emails.csv` file from the template and enter your email, name and active status. 

Example:

```
┌────────────────────────┬─────────────┬──────────────┐
│ email                  │ name        │ is_active    │
├────────────────────────┼─────────────┼──────────────┤
│ your_email@example.com │ your_name   │ true         │
│ ...                    │ ...         │ ...          │
└────────────────────────┴─────────────┴──────────────┘
```

**Note:** Without a verified domain in Resend, emails can only be sent to your own registered email address. To send summaries to multiple recipients, you must register and verify a custom domain.

#### 3) Edit `.env` to define:

- **Email enabled** (e.g. true if you want emails to be sent)
- **From email** (e.g. your_email@example.com)
- **Retry attempts** for failed emails before moving on (e.g. 3 attempts)
- **Email wait time** between email attempts (e.g. 2 seconds)

Example:

```env
EMAIL_ENABLED=true
FROM_EMAIL=your_verified_domain@example.com
EMAIL_RETRY_ATTEMPTS=3
EMAIL_WAIT_TIME=2
```
**Note:** If you do not have a verified domain, you can leave `FROM_EMAIL` blank. Emails will then be sent using Resend’s default sender (`onboarding@resend.dev`), but only to your own email address. 

## 🗂️ Project Structure

```text
NewsMonitor/
│
├── main.py
├── config.py
├── links.csv
├── emails.example.csv
├── .env.example
├── requirements.txt
├── pytest.ini
│
├── data/
│   ├── .gitkeep
│   └── logs/
│       └── .gitkeep
│
├── utils/
│   ├── __init__.py
│   └── database_helpers.py
│
├── newsmonitor/
│   ├── __init__.py
│   ├── build_prompts.py
│   ├── scrape_headlines.py
│   ├── deduplicate_headlines.py
│   ├── identify_risk_headlines.py
│   ├── scrape_stories.py
│   ├── summarise_stories.py
│   ├── store_data.py
│   └── email_summary.py
│
└── tests/
    ├── utils/
    │   └── test_database_helpers.py
    │
    └── newsmonitor/
        ├── test_scrape_headlines.py
        ├── test_deduplicate_headlines.py
        ├── test_identify_risk_headlines.py
        ├── test_scrape_stories.py
        ├── test_summarise_stories.py
        └── test_store_data.py
```

## 📃 License

This project is licensed under the Apache 2.0 License - see the [LICENSE](https://github.com/jcarterlab/NewsMonitor/blob/main/LICENSE) file for details.