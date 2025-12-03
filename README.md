# Property Valuation Model Thesis

A machine learning project designed to estimate the value of residential properties (apartments and houses) in Hungary. This project integrates web scraping, data preprocessing, and machine learning models to provide accurate price predictions.

## Features

-   **Web Scraping**: Automated collection of real estate data from major Hungarian property sites (Jofogas, Otthon Centrum) using Scrapy.
-   **Data Pipeline**: Robust preprocessing and enrichment of raw data.
-   **Machine Learning**: XGBoost and Random Forest models trained to predict property prices based on features like size, location, condition, and more.
-   **Interactive Web App**: A Streamlit application for users to:
    -   Estimate property values based on custom inputs.
    -   Visualize market data (price distributions, correlations).
    -   Analyze model performance metrics.

## Tech Stack

-   **Language**: Python 3.10+
-   **Web Framework**: Streamlit
-   **Data Collection**: Scrapy
-   **Data Manipulation**: Pandas, NumPy
-   **Machine Learning**: XGBoost, Scikit-learn
-   **Visualization**: Plotly
-   **Database/Storage**: SQLite / CSV (via SQLAlchemy/Pandas)
-   **Development**: Jupyter Notebooks, VS Code

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd property-valuation-model-thesis
    ```

2.  **Create and activate a virtual environment (optional but recommended):**
    ```bash
    python -m venv .venv
    # Windows
    .venv\Scripts\activate
    # Linux/Mac
    source .venv/bin/activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Usage

### Running the Web Application

To launch the interactive valuation tool:

```bash
streamlit run src/app.py
```

The app will open in your default browser, allowing you to estimate prices and explore data.

### Running the Scraper

To collect fresh data from the supported websites:

```bash
python src/scraper/spider_control.py
```

This script triggers all spiders (`JofogasLakas`, `JofogasHaz`, `OcLakas`, `OcHaz`) and processes the data through the configured pipelines.

### Notebooks

The `notebooks/` directory contains Jupyter notebooks used for:
-   Exploratory Data Analysis (EDA)
-   Model training and experimentation (XGBoost, Random Forest)
-   Hyperparameter tuning

## Project Structure

-   `src/app.py`: Main Streamlit application entry point.
-   `src/scraper/`: Scrapy project for data collection.
-   `src/preprocessing/`: Scripts for cleaning and preparing data.
-   `src/enrichment/`: Feature engineering and data enrichment modules.
-   `src/models/`: Model definitions and training logic.
-   `data/`: Directory for storing raw and processed datasets.
-   `notebooks/`: Experimental notebooks and model artifacts.
