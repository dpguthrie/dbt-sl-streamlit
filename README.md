# dbt Cloud Semantic Layer on Streamlit

Use this streamlit app to view the metrics you've defined in your project.  The only thing you'll need to define is the `JDBC_URL` that you obtain from dbt Cloud.

# Developing

1. Clone the repo
2. Create virtual environment
```bash
python -m venv .venv
```
3. Install dependencies

- via `uv` (recommended)
```bash
pip install uv && uv pip install -r requirements.txt
```
- via `pip`
```bash
source .venv/bin/activate
pip install -r requirements.txt
```

Finally, run your app:

```bash
streamlit run 🏠_Home.py
```
