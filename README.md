# 📰 Targeted News Monitoring Pipeline

A Python pipeline that uses web scraping and LLMs to monitor news events based on a certain risk type. The system is designed to allow analysts to detect emerging risks such as supply chain disruptions, regulatory changes or geopolitical events more efficiently. It is particularly useful in emerging markets with many non-English sources because LLMs are excellent at simultaneously translating and summarizing raw news content. 

**Key technologies:** Python, BeautifulSoup, Pandas, LLM APIs, prompt engineering.


## 🔍 Overview

The pipeline performs the following steps:

1. Scrapes headlines from multiple news listing pages
2. Uses an LLM to identify risk-relevant headlines
3. Scrapes full article texts for the flagged stories
4. Uses a two-stage LLM summarisation process to generate a final summary

This approach allows large volumes of news to be processed efficiently while focusing only on stories relevant to a specific risk type.


## ⚙️ Customization

New monitoring can be customised based on:

- Entity of concern (e.g. a logistics firm operating in Colombia)
- Risk type (e.g. transport disruption events)
- Confidence rate (e.g. 95%)

A variety of other parameters such as batch size, model type, retry attempts and more can also be adjusted. 


## 🧪 Example Flow

```
links.csv
     │
     ▼
scrape_headlines
     │
     │ Example output (Spanish):
     │ [
     │   "Paro portuario en Buenaventura amenaza exportaciones",
     │   "Aumentan las exportaciones de café pese a retrasos logísticos",
     │   "Sindicato ferroviario anuncia protestas nacionales",
     │   ...
     │ ]
     │
     ▼
identify_risk_headlines (lightweight model)
     │
     │ Example output (headline indices):
     │ [0, 2, 15, 21]
     │
     ▼
scrape_stories
     │
     │ Example output (Spanish):
     │ [
     │   "Trabajadores portuarios en Buenaventura iniciaron un paro...",
     │   "El sindicato nacional ferroviario anunció protestas que..."
     │   ...
     │ ]
     │
     ▼
summarise_stories (basic & advanced models)
     │
     │ Example output (English):
     │ "Labour disputes in Colombia's port and rail sectors may disrupt
     │ freight movement and export logistics in the coming days. Also..."
     │
     ▼
the end user
```


## 📐 Architectural Decisions


- **Batch headline identification:** The scraped headlines are evaluated in batches using a lightweight LLM model to improve efficiency. Headlines are joined together, separated by index numbers and fed into the LLM via a prompt asking it to return only the indices of potential risk stories as a Python-style list.

- **Two-stage LLM summarisation:** The scraped story text undergoes a two-stage LLM summarisation process to improve relevance. The story text is compiled into batches and summarised using a lightweight LLM model before an advanced model is instructed to use critical judgement in producing a concise final summary. 


## 🗂️ Project Structure

```text
targeted-news-monitoring-pipeline
│
├── main.py
├── config.py
├── links.csv
│
└── news_monitoring_pipeline
    ├── __init__.py
    ├── scrape_headlines.py
    ├── identify_risk_headlines.py
    ├── scrape_stories.py
    ├── summarise_stories.py
    └── build_prompts.py
```

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/jcarterlab/Targeted-News-Analysis-Pipeline.git

cd Targeted-News-Analysis-Pipeline

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

Configure the pipeline in `config.py` and provide your news source links and CSS selectors in `links.csv`.

Run the pipeline:

```bash
python main.py
```


## 📃 License

This project is licensed under the Apache 2.0 License - see the [LICENSE](https://github.com/jcarterlab/Targeted-News-Analysis-Pipeline/blob/main/LICENSE) file for details.