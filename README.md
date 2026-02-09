# Texas Storm Impact Analyzer

This project analyzes NOAA Storm Events data to identify which types of
storms cause the most economic damage in Texas and to model the likelihood
of high-damage events. The analysis focuses on real-world challenges such
as imbalanced data, noisy inputs, and tradeoffs between recall and
precision in risk prediction.

The dataset spans storm events from 2010–2025 and includes over 76,000
Texas-specific records derived from NOAA’s bulk Storm Events files.

---

## Project Goals

- Identify storm event types responsible for the highest total damage in Texas
- Analyze long-term trends and seasonal patterns in storm-related damage
- Build machine learning models to predict whether a storm event will cause
  high economic damage (≥ $1M)
- Compare linear and non-linear models on a highly imbalanced dataset

---

## Data Source

- **NOAA / NCEI Storm Events Database**
- Bulk yearly CSV files (StormEvents_details)
- Raw data is not committed to the repository due to size
- Publicly available at: https://www.ncei.noaa.gov/stormevents/

---

## Data Processing Pipeline

1. Download yearly NOAA Storm Events CSV files
2. Filter records to Texas-only events
3. Clean and normalize damage values (K / M / B → USD)
4. Engineer features such as total damage, year, and month
5. Store cleaned dataset as a Parquet file for efficient reuse

Final dataset:
- ~76,000 storm events
- Stored locally as `tx_2010_2025.parquet` (ignored by Git)

---

## Exploratory Data Analysis

Key findings from exploratory analysis include:

- **Flash Floods** account for the highest total economic damage in Texas
- Storm damage is highly uneven across years, with **2017** showing a major spike
- Damage exhibits strong seasonal patterns, particularly in spring and late summer

Visualizations are saved under `reports/figures/`.

---

## Modeling Approach

### Target Variable
- `high_damage`: 1 if total damage ≥ $1,000,000, else 0
- Only ~1.3% of events meet this threshold, making this a rare-event
  classification problem

### Models Trained
- Naive baseline (predict all events as low damage)
- Logistic Regression (class-weighted)
- Random Forest (class-weighted)

### Evaluation Metrics
- Precision, Recall, and F1-score
- Confusion matrices
- Accuracy is reported but not emphasized due to class imbalance

---

## Key Results

- Logistic Regression achieved the highest recall (~80%), correctly
  identifying the majority of high-damage storm events, at the cost of
  increased false positives.
- Random Forest improved precision and overall F1-score but resulted in
  more false negatives, missing a greater number of high-damage events
  compared to Logistic Regression.
- Feature importance analysis from the Random Forest model highlighted
  storm magnitude, seasonal timing, event type, reporting source, and
  geographic region as key predictors of damage severity.

Given the risk-focused nature of this problem, where missing a severe
storm event is more costly than issuing a false alarm, Logistic
Regression may be preferred for early-warning or decision-support
applications.

---

## Repository Structure
```
tx-storm-impact-analyzer/
├── src/ # Data ingestion and processing scripts
├── notebooks/ # EDA and modeling notebooks
├── reports/
│   └── figures/ # Saved visualizations
├── data/ # Ignored (raw and processed datasets)
├── requirements.txt
└── README.md
```

---

## Getting Started

### Requirements
- Python 3.10+
- Virtual environment support (venv, conda, etc.)

### Setup
```bash
git clone https://github.com/EseCristian/tx-storm-impact-analyzer.git
cd tx-storm-impact-analyzer
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
### Running the Pipeline

#### 1) Download NOAA Storm Events data
```bash
python src/download_noaa.py
```

#### 2) Build the Texas dataset
```bash
python src/make_tx_dataset.py
```

#### 3) Run the analysis notebooks
- `notebooks/01_eda_texas_storms.ipynb`
- `notebooks/02_modeling_damage_severity.ipynb`

> **Note:** Raw and processed datasets are excluded from version control due to size and can be regenerated using the provided scripts.

---

## Technologies Used

- Python
- Pandas, NumPy
- Matplotlib
- scikit-learn
- Jupyter Notebook

---

## Notes

This project emphasizes reproducibility and interpretability over maximum
model accuracy. All results are generated from publicly available data
and are intended for educational purposes.
