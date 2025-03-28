import streamlit as st
import pandas as pd
import requests
import re
import socket
import dns.resolver

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

def website_exists(domain):
    if not domain:
        return False
    for protocol in ['https', 'http']:
        try:
            response = requests.head(f"{protocol}://{domain}", timeout=5, allow_redirects=True)
            if response.status_code < 400:
                return True
        except requests.RequestException:
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

# --- UI ---
country = st.selectbox("Select Country for Domain Guessing", list(country_tlds.keys()))
uploaded_file = st.file_uploader("Upload CSV", type=["csv"])

tlds = country_tlds.get(country, [".com"])

if uploaded_file:
    df = pd.read_csv(uploaded_file)
    st.subheader("📄 Original CSV Preview")
    st.dataframe(df.head())

    required = ["profile_url", "first_name", "last_name", "current_company", "organization_domain_1"]
    df = df[[col for col in df.columns if col in required]]
    df['organization_domain_1'] = df['organization_domain_1'].apply(clean_domain)

    # Enrich missing domains
    missing = df['organization_domain_1'].isna()
    df.loc[missing, 'organization_domain_1'] = df.loc[missing, 'current_company'].apply(guess_domain_clearbit)

    # Fallback for unresolved domains
    still_missing = df['organization_domain_1'].isna() | ~df['organization_domain_1'].apply(website_exists)
    df.loc[still_missing, 'organization_domain_1'] = df.loc[still_missing, 'current_company'].apply(lambda x: fallback_guess(x, tlds))

    # Validate final domains
    df['domain_valid'] = df['organization_domain_1'].apply(website_exists)
    df = df[df['domain_valid']].copy()
    df['is_not_microsoft'] = df['organization_domain_1'].apply(is_not_microsoft)
    df.drop(columns=['domain_valid'], inplace=True)

    st.subheader("✅ Enriched Preview")
    st.dataframe(df.head())

    st.download_button("📥 Download Enriched CSV", df.to_csv(index=False), file_name=f"enriched_{country.replace(' ', '_').lower()}.csv", mime="text/csv")
