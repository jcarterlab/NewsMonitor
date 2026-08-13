# 📰 NewsMonitor

An LLM-powered news monitoring pipeline for tracking and analysing news across multiple sources and languages.

## Overview

NewsMonitor scrapes news headlines, identifies relevant stories using an LLM, summarises the underlying articles and optionally sends email alerts.

The pipeline can be configured around a **topic**, **entity** and **confidence threshold**, making it suitable for a wide range of applications — from tracking technology trends and financial developments to monitoring emerging-market risks.

**Tech stack:** Python · BeautifulSoup · SQLite · Pandas · Google Gemini · Resend

## Features

* 🌐 **Multi-source monitoring** — Monitor multiple websites and languages from a single pipeline
* 🎯 **Targeted analysis** — Focus on specific topics, entities or events
* 🧠 **LLM relevance detection** — Filter headlines before processing full articles
* 🌍 **Cross-language analysis** — Translate and summarise non-English sources
* 💾 **Persistent results** — Store processed headlines and summaries in SQLite
* 📧 **Automated alerts** — Deliver summaries directly to users by email
* ⚙️ **Configurable pipeline** — Adapt sources, models and processing parameters

## How It Works

```text
News sources
     ↓
Scrape headlines
     ↓
Deduplicate against database
     ↓
LLM identifies relevant headlines
     ↓
Scrape relevant articles
     ↓
LLM summarises articles
     ↓
Store results in SQLite
     ↓
Optional email alert
```

## Example

NewsMonitor can be configured for different monitoring objectives, for example:

**Technology trends**

```env
TOPIC_OF_CONCERN=artificial intelligence developments
ENTITY_OF_CONCERN=large language models
IDENTIFICATION_CONFIDENCE_THRESHOLD=95
```

**Emerging-market risk**

```env
TOPIC_OF_CONCERN=transport disruption events
ENTITY_OF_CONCERN=a logistics firm operating in Colombia
IDENTIFICATION_CONFIDENCE_THRESHOLD=95
```

**Financial developments**

```env
TOPIC_OF_CONCERN=interest rate changes
ENTITY_OF_CONCERN=central banks
IDENTIFICATION_CONFIDENCE_THRESHOLD=95
```

For example, a Spanish headline such as:

> Paro portuario en Buenaventura amenaza exportaciones

can be identified as relevant, its full article retrieved and the resulting information incorporated into an English summary.

## Installation

```bash
git clone https://github.com/jcarterlab/NewsMonitor.git
cd NewsMonitor

python -m venv .venv
source .venv/bin/activate  # macOS/Linux or
.venv\Scripts\Activate.ps1  # Windows PowerShell

pip install -r requirements.txt
cp .env.example .env
```

Add your Gemini API key to `.env`, configure `links.csv`, then run:

```bash
python main.py
```

## Configuration

### News sources

Create `links.csv` from the provided template and define each source's URL and CSS selectors for extracting headlines and article text.

### Monitoring

Set the topic, entity and classification confidence threshold in `.env`:

```env
TOPIC_OF_CONCERN=transport disruption events
ENTITY_OF_CONCERN=a logistics firm operating in Colombia
IDENTIFICATION_CONFIDENCE_THRESHOLD=95
```

### Pipeline

Optional parameters control scraping, batching, retries, model selection and summarisation.

```env
REQUEST_TIMEOUT=10
LLM_HEADLINE_BATCH_SIZE=40
LLM_RETRY_ATTEMPTS=3
BASIC_MODEL=gemini-2.5-flash
ADVANCED_MODEL=gemini-2.5-pro
```

### Email alerts

Set `EMAIL_ENABLED=true`, provide a Resend API key and configure the sender and recipients using the supplied templates.

## Limitations

* **Web scraping:** News sites may change their HTML structure or restrict automated requests.
* **LLM classification:** Relevance decisions are probabilistic and depend on the configured model and threshold.
* **LLM summarisation:** Summaries may omit or misinterpret information from source articles.
* **Source configuration:** Each news source requires appropriate selectors for headline and article extraction.
* **API costs and limits:** Monitoring large numbers of sources can increase API usage and runtime.

## Inspiration

NewsMonitor builds on my **[Latin Risk Pulse](https://github.com/jcarterlab/Latin-Risk-Pulse-ML-model)** project. This was a previous idea to provide political risk monitoring services focusing on Latin America. 

While both projects use web scraping and LLMs to analyse emerging risks, NewsMonitor generalises the approach into a configurable news-monitoring pipeline. Rather than focusing on a fixed set of countries and risk indicators, it can be adapted to different **topics, entities and news sources**.
