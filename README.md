# Social Media Dataset Analysis: Three Implementation Approaches

## Overview
This project demonstrates three different approaches to analyzing large social media datasets:
- **Pure Python** (Standard Library only)
- **Pandas** 
- **Polars**

We analyze three distinct datasets to compare performance, usability, and implementation complexity across these methods.

---

## Installation & Setup

### Prerequisites
- Python 3.8 or higher
- Git (optional, for cloning)

### Required Dependencies
Install all required packages using pip:

```bash
pip install pandas polars
```

Or install from requirements file:
```bash
pip install -r requirements.txt
```

**requirements.txt contents:**
```
pandas>=1.5.0
polars>=0.20.0
```

---

## Datasets

## Data Files
**No dataset files are committed to this repository, but you can download them from here: [Google Drive Link](https://drive.google.com/file/d/1Jq0fPb-tq76Ee_RtM58fT0_M3o-JDBwe/view?usp=sharing).**

### 1. Twitter Posts Dataset
- **File:** `period_03/2024_tw_posts_president_scored_anon.csv`
- **Size:** 27,304 rows × 47 columns
- **Content:** Scored and annotated Twitter posts from political figures
- **Key metrics:** Engagement data, message types, topic classifications

### 2. Facebook Ads Dataset
- **File:** `period_03/2024_fb_ads_president_scored_anon.csv`
- **Size:** 246,745 rows × 41 columns
- **Content:** Political campaign Facebook advertisements
- **Key metrics:** Audience targeting, spend data, message annotations

### 3. Facebook Posts Dataset
- **File:** `period_03/2024_fb_posts_president_scored_anon.csv`
- **Size:** 19,009 rows × 56 columns
- **Content:** Facebook posts from political pages
- **Key metrics:** Engagement reactions, message/topic classifications

---

## Project Structure

```
project/
├── README.md
├── results.md
├── requirements.txt
├── 2024_tw_posts/
│   ├── pure_python_stats.py
│   ├── pandas_stats.py
│   └── polars_stats.py
├── 2024_fb_ads/
│   ├── pure_python_stats.py
│   ├── pandas_stats.py
│   └── polars_stats.py
└── 2024_fb_posts/
    ├── pure_python_stats.py
    ├── pandas_stats.py
    └── polars_stats.py
```

---

## How to Execute the Analysis

Each dataset folder contains three implementation scripts. Navigate to the desired folder and run any script:

```bash
# Example: Analyze Twitter posts using Pandas
cd 2024_tw_posts
python pandas_stats.py

# Example: Analyze Facebook ads using Polars
cd 2024_fb_ads
python polars_stats.py

# Example: Analyze Facebook posts using Pure Python
cd 2024_fb_posts
python pure_python_stats.py
```

All scripts will:
1. Load and validate the dataset
2. Generate comprehensive descriptive statistics
3. Display formatted results in the terminal
4. Report execution time and memory usage

---

## Script Functionality

Each implementation provides identical analytical output:
- **Dataset overview:** Row/column counts, data types
- **Numerical statistics:** Mean, median, standard deviation, min/max
- **Categorical analysis:** Value counts, frequency distributions
- **Missing data assessment:** Null value counts and percentages
- **Top performers:** Most frequent values and highest engagement metrics

---

## Performance Expectations

Based on our testing, approximate execution times are:

| Dataset Size | Pure Python | Pandas | Polars |
|-------------|-------------|---------|---------|
| Small (19K rows) | ~0.8s | ~0.2s | ~0.3s |
| Medium (27K rows) | ~1.3s | ~0.1s | ~0.1s |
| Large (247K rows) | ~7.2s | ~7.1s | ~2.8s |

*Note: Times vary based on hardware specifications and system load.*

---

## Key Findings Summary

### Performance Hierarchy
1. **Polars:** Fastest for large datasets due to Rust-based parallel processing
2. **Pandas:** Excellent balance of speed and functionality for most use cases
3. **Pure Python:** Functional but significantly slower for large-scale analysis

### Use Case Recommendations
- **Large-scale data processing (100K+ rows):** Use Polars
- **Exploratory data analysis and visualization:** Use Pandas
- **Educational purposes or minimal dependencies:** Use Pure Python
- **Production analytics pipelines:** Use Polars or Pandas depending on ecosystem needs

---

## Detailed Results

For comprehensive analysis results, performance comparisons, and implementation insights, see **[results.md](results.md)**.

---

## Authors & Contributors
Analysis implementation by Disha Negi

---

## License
This project is available for educational and research purposes.