import streamlit as st
import pandas as pd
import requests
import re
import socket
import dns.resolver
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.exceptions import LocationParseError

st.set_page_config(page_title="Domain Enrichment Tool", layout="centered")
st.title("🔍 Domain Enrichment & Validation Tool")

# --- Country & TLD mapping ---
country_tlds = {
    "USA": [".com", ".io", ".ai"],
    "UK": [".co.uk", ".com", ".io", ".ai"],
    "Canada": [".ca", ".com", ".io", ".ai"],
    "Germany": [".de", ".com", ".io", ".ai"],
    "France": [".fr", ".com", ".io", ".ai"],
    "Netherlands": [".nl", ".com", ".io", ".ai"],
    "Belgium": [".be", ".com", ".io", ".ai"],
    "Sweden": [".se", ".com", ".io", ".ai"],
    "Austria": [".at", ".com", ".io", ".ai"],
    "Switzerland": [".ch", ".com", ".io", ".ai"],
    "Denmark": [".dk", ".com", ".io", ".ai"],
    "Finland": [".fi", ".com", ".io", ".ai"],
    "Norway": [".no", ".com", ".io", ".ai"],
    "Ireland": [".ie", ".com", ".io", ".ai"],
    "Luxembourg": [".lu", ".com", ".io", ".ai"],
    "Iceland": [".is", ".com", ".io", ".ai"],
    "Spain": [".es", ".com", ".io", ".ai"],
    "Singapore": [".sg", ".com", ".io", ".ai"],
    "United Arab Emirates": [".ae", ".com", ".io", ".ai"],
    "New Zealand": [".nz", ".com", ".io", ".ai"],
    "South Africa": [".za", ".com", ".io", ".ai"],
    "Japan": [".jp", ".com", ".io", ".ai"],
    "Israel": [".il", ".com", ".io", ".ai"],
    "South Korea": [".kr", ".com", ".io", ".ai"],
    "Hong Kong": [".hk", ".com", ".io", ".ai"],
    "Taiwan": [".tw", ".com", ".io", ".ai"]
}

# --- Functions ---
def clean_domain(domain):
    if pd.isna(domain):
        return None
    domain = domain.lower()
    domain = re.sub(r'^https?://', '', domain)
    domain = re.sub(r'^(www|careers|info|jobs|shop)\.', '', domain)
    return domain.strip().split('/')[0]

def is_valid_domain(domain):
    if not domain or not isinstance(domain, str):
        return False
    if len(domain) > 253 or '..' in domain:
        return False
    return re.match(r'^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', domain) is not None

def website_exists(domain):
    if not is_valid_domain(domain):
        return False
    for protocol in ['https', 'http']:
        try:
            response = requests.head(f"{protocol}://{domain}", timeout=5, allow_redirects=True)
            if response.status_code < 400:
                return True
        except (requests.RequestException, LocationParseError):
            continue
    return False

def is_not_microsoft(domain):
    try:
        answers = dns.resolver.resolve(domain, 'MX')
        for rdata in answers:
            mx = str(rdata.exchange).lower()
            if any(provider in mx for provider in [
                'outlook.com', 'office365.com', 'microsoft.com',
                'protection.outlook.com', 'mail.protection']):
                return False
        return True
    except:
        return False

def guess_domain_clearbit(company):
    try:
        url = f"https://autocomplete.clearbit.com/v1/companies/suggest?query={company}"
        r = requests.get(url)
        if r.status_code == 200:
            res = r.json()
            if res:
                return clean_domain(res[0].get("domain"))
        return None
    except:
        return None

def guess_domain_opencorporates(company, country):
    try:
        query = f"https://api.opencorporates.com/v0.4/companies/search?q={company}&jurisdiction_code={country.lower()}"
        r = requests.get(query)
        if r.status_code == 200:
            res = r.json()
            companies = res.get("results", {}).get("companies", [])
            for comp in companies:
                url = comp.get("company", {}).get("homepage_url")
                if url:
                    return clean_domain(url)
        return None
    except:
        return None

def fallback_guess(company, tlds):
    if not isinstance(company, str):
        return None
    name_clean = re.sub(r'[^a-z0-9]', '', company.lower())
    name_dash = re.sub(r'[^a-z0-9]', '-', company.lower())
    guesses = [f"{name}{tld}" for name in [name_clean, name_dash] for tld in tlds]
    for d in guesses:
        if website_exists(d):
            return d
    return None

def enrich_row(i, row, tlds, country, counters):
    domain = clean_domain(row['organization_domain_1'])
    source = 'existing'
    if pd.isna(domain):
        domain = guess_domain_clearbit(row['current_company'])
        source = 'clearbit'
        if domain:
            counters['clearbit'] += 1
    if pd.isna(domain):
        domain = guess_domain_opencorporates(row['current_company'], country)
        source = 'opencorporates'
        if domain:
            counters['opencorporates'] += 1
    if pd.isna(domain) or not website_exists(domain):
        domain = fallback_guess(row['current_company'], tlds)
        source = 'guessed'
        if domain:
            counters['guessed'] += 1
    valid = website_exists(domain)
    is_not_microsoft_flag = is_not_microsoft(domain)
    slow = False
    start = time.time()
    website_exists(domain)
    if time.time() - start > 3:
        slow = True
    return i, domain, valid, is_not_microsoft_flag, source, slow

# --- UI: Main controls and download section ---
country = st.selectbox("🌍 Select Country for Domain Guessing", list(country_tlds.keys()))
uploaded_file = st.file_uploader("📤 Upload CSV with required columns", type=["csv"])

if uploaded_file:
    try:
        df = pd.read_csv(uploaded_file)
        required_cols = ["profile_url", "first_name", "last_name", "current_company", "organization_domain_1"]
        df = df[[col for col in df.columns if col in required_cols]]
        tlds = country_tlds.get(country, [".com"])

        counters = {"clearbit": 0, "guessed": 0}
        total = len(df)
        progress = st.progress(0)
        status_text = st.empty()
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(enrich_row, i, row, tlds, country, counters) for i, row in df.iterrows()]
            for n, future in enumerate(as_completed(futures)):
                i, domain, valid, not_ms, source, slow = future.result()
                df.at[i, 'organization_domain_1'] = domain
                df.at[i, 'domain_valid'] = valid
                df.at[i, 'is_not_microsoft'] = not_ms
                df.at[i, 'source'] = source
                df.at[i, 'slow_lookup'] = slow

                elapsed = time.time() - start_time
                est_total_time = elapsed / (n + 1) * total
                remaining_time = est_total_time - elapsed
                progress.progress((n + 1) / total)
                status_text.text(f"Processing {n + 1}/{total} rows | Elapsed: {int(elapsed)}s | ETA: {int(remaining_time)}s")

        st.session_state.df_enriched = df

    except Exception as e:
        st.error(f"❌ Failed to read or process CSV: {e}")

if 'df_enriched' in st.session_state:
    df = st.session_state.df_enriched
    filename = st.text_input("Enter filename for download (without extension)", "enriched_output")

    df_valid = df[df['organization_domain_1'].notna() & df['organization_domain_1'].str.strip().ne('')]
    df_failed = df[df['organization_domain_1'].isna() | df['organization_domain_1'].str.strip().eq('')]
    df_non_ms = df_valid[df_valid['is_not_microsoft']]

    st.markdown("### 📊 Enrichment Summary")
    st.write(f"✅ Domains enriched: {len(df_valid)}")
    st.write(f"❌ Domains not enriched: {len(df_failed)}")
    st.write(f"📭 Non-Microsoft domains: {len(df_non_ms)}")

    st.markdown("### 📥 Downloads")
    st.download_button("📄 Download Full List", df_valid.to_csv(index=False), file_name=f"{filename}_full.csv", mime="text/csv")
    st.download_button("💡 Download Non-Microsoft Only", df_non_ms.to_csv(index=False), file_name=f"{filename}_non_microsoft.csv", mime="text/csv")
    st.download_button("⚠️ Download Not Enriched", df_failed.to_csv(index=False), file_name=f"{filename}_not_enriched.csv", mime="text/csv")
