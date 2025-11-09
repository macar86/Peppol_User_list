import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import xml.sax
from xml.sax.handler import ContentHandler
import psycopg
from tqdm import tqdm
import re
import json
import logging
from datetime import datetime

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("🚀 Starting Peppol Business Cards Update...")

# Config
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL not set! Add to GitHub Secrets.")
EXPORT_URL = "https://test-directory.peppol.eu/export/businesscards"
XML_PATH = "./directory-export-business-cards.xml"

def download_file_with_retry(url, file_path, max_retries=3, chunk_size=8192):
    session = requests.Session()
    retries = Retry(total=max_retries, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    logging.info(f"📥 Downloading {url}...")
    response = session.get(url, stream=True, timeout=60)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    with open(file_path, 'wb') as f, tqdm(
        desc="Download Progress",
        total=total_size,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
        mininterval=5.0,  # Update every 5 seconds to reduce spam
    ) as progress_bar:
        for chunk in response.iter_content(chunk_size=chunk_size):
            size = f.write(chunk)
            progress_bar.update(size)
    
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    logging.info(f"✅ Downloaded {file_size_mb:.2f} MB")

class PeppolHandler(ContentHandler):
    def __init__(self):
        self.business_data = {}
        self.current = None
        self.in_businesscard = False
        self.in_entity = False
        self.current_tag = ""
        self.text_buffer = ""
        self.processed = 0
        self.skipped = 0

    def startElement(self, name, attrs):
        if name is None:
            return  # Skip malformed elements
        name_lower = name.lower()
        if name_lower == "businesscard":
            self.in_businesscard = True
            self.current = {"doctypeids": [], "entity": {}}
            return
        if not self.in_businesscard:
            return
        if name_lower == "participant":
            self.current["participant"] = {
                "scheme": attrs.get("scheme"),
                "value": attrs.get("value")
            }
        elif name_lower == "entity":
            self.in_entity = True
            self.current["entity"] = {
                "countrycode": attrs.get("countrycode")
            }
        elif name_lower == "name" and self.in_entity:
            self.current["entity"]["name"] = attrs.get("name", "").strip()
            self.current["entity"]["language"] = attrs.get("language")
        elif name_lower == "id" and self.in_entity:
            self.current["entity"]["id"] = {
                "scheme": attrs.get("scheme"),
                "value": attrs.get("value")
            }
        elif name_lower in ["geoinfo", "additionalinfo", "website"] and self.in_entity:
            self.text_buffer = ""
        elif name_lower == "contact" and self.in_entity:
            self.current["entity"]["contact"] = {
                "type": attrs.get("type"),
                "name": attrs.get("name"),
                "phonenumber": attrs.get("phonenumber"),
                "email": attrs.get("email")
            }
        elif name_lower == "regdate" and self.in_entity:
            self.text_buffer = ""
        elif name_lower == "doctypeid":
            self.current["doctypeids"].append({
                "scheme": attrs.get("scheme"),
                "value": attrs.get("value"),
                "displayname": attrs.get("displayname"),
                "deprecated": attrs.get("deprecated") == "true"
            })
        self.current_tag = name_lower

    def characters(self, content):
        if self.in_businesscard and content:
            self.text_buffer += content.strip()

    def endElement(self, name):
        if name is None:
            return  # Skip malformed elements
        name_lower = name.lower()
        if name_lower == "businesscard":
            try:
                if not self.current.get("participant"):
                    self.skipped += 1
                    return
                participant_value = self.current["participant"].get("value")
                if not participant_value or ":" not in participant_value:
                    self.skipped += 1
                    logging.warning("⚠️ Skipping invalid participant")
                    return
                scheme_id, endpoint_id = participant_value.split(":", 1)
                full_pid = participant_value
                entity = self.current.get("entity", {})
                company_name = entity.get("name", "Unknown").strip()
                raw_types = [d.get("value") for d in self.current.get("doctypeids", []) if d.get("value")]
                extracted_types = [self.extract_document_type(v) for v in raw_types]
                uniq_types = list(set(t for t in extracted_types if t is not None))  # Filter None
                supports_invoice = any("invoice" in (t.lower() if t else "") for t in uniq_types)
                supports_creditnote = any("creditnote" in (t.lower() if t else "") for t in uniq_types)
                
                # Extra fields as JSON
                entity_extra = {
                    "id": entity.get("id"),
                    "geoinfo": entity.get("geoinfo"),
                    "additionalinfo": entity.get("additionalinfo"),
                    "contact": entity.get("contact"),
                    "website": entity.get("website"),
                    "language": entity.get("language")
                }
                
                self.business_data[full_pid] = {
                    "scheme_id": scheme_id,
                    "endpoint_id": endpoint_id,
                    "company_name": company_name,
                    "country_code": entity.get("countrycode", "").upper() or None,
                    "registration_date": entity.get("regdate"),
                    "entity_extra": entity_extra,
                    "raw_document_types": ", ".join(raw_types),
                    "document_types": uniq_types,
                    "supports_invoice": supports_invoice,
                    "supports_creditnote": supports_creditnote
                }
                self.processed += 1
                if self.processed % 10000 == 0:  # Reduced frequency: every 10k cards
                    logging.info(f"📊 Processed {self.processed} cards, skipped {self.skipped}...")
            except Exception as e:
                self.skipped += 1
                logging.warning(f"⚠️ Card processing error: {e}")
            finally:
                self.in_businesscard = False
                self.current = None
                self.text_buffer = ""
            return
        if self.in_entity:
            if name_lower in ["regdate", "geoinfo", "additionalinfo", "website"]:
                self.current["entity"][name_lower] = self.text_buffer
                self.text_buffer = ""
            elif name_lower == "entity":
                self.in_entity = False

    @staticmethod
    def extract_document_type(doctype_value):
        if not doctype_value:
            return None
        try:
            pattern1 = r"::([A-Za-z]+)-2::([A-Za-z]+)##"
            match1 = re.search(pattern1, doctype_value)
            if match1 and match1.group(2):
                return match1.group(2)
            pattern2 = r"::([A-Za-z]+)##"
            match2 = re.search(pattern2, doctype_value)
            if match2 and match2.group(1):
                return match2.group(1)
            if "CrossIndustryInvoice" in doctype_value:
                return "CrossIndustryInvoice"
            if "ApplicationResponse" in doctype_value:
                return "ApplicationResponse"
            if "Order" in doctype_value:
                return "Order"
        except Exception:
            pass
        return None

def load_to_db(business_data):
    logging.info(f"🗄️ Loading {len(business_data)} records to DB...")
    with psycopg.connect(DATABASE_URL, sslmode='require') as conn:
        # Drop and create table
        with conn.transaction():
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS participants_final")
            cur.execute("""
                CREATE TABLE participants_final (
                    id SERIAL PRIMARY KEY,
                    full_pid VARCHAR(500) UNIQUE NOT NULL,
                    scheme_id VARCHAR(50) NOT NULL,
                    endpoint_id VARCHAR(400) NOT NULL,
                    supports_invoice BOOLEAN DEFAULT FALSE,
                    supports_creditnote BOOLEAN DEFAULT FALSE,
                    company_name TEXT,
                    country_code VARCHAR(10),
                    registration_date VARCHAR(50),
                    entity_extra JSONB,
                    raw_document_types TEXT,
                    document_types JSONB,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
        
        # Insert batches with per-batch transaction
        batch_size = 5000
        entries = list(business_data.items())
        for i in tqdm(range(0, len(entries), batch_size), desc="Batch Insert", mininterval=1.0):  # Update every second
            batch = entries[i:i + batch_size]
            values = [(full_pid, data["scheme_id"], data["endpoint_id"], data["supports_invoice"], data["supports_creditnote"], data["company_name"], data["country_code"], data["registration_date"], json.dumps(data["entity_extra"]), data["raw_document_types"], json.dumps(data["document_types"])) for full_pid, data in batch]
            with conn.transaction():
                cur = conn.cursor()
                cur.executemany("""
                    INSERT INTO participants_final
                    (full_pid, scheme_id, endpoint_id, supports_invoice, supports_creditnote, company_name, country_code, registration_date, entity_extra, raw_document_types, document_types)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (full_pid) DO UPDATE SET
                        scheme_id = EXCLUDED.scheme_id,
                        endpoint_id = EXCLUDED.endpoint_id,
                        supports_invoice = EXCLUDED.supports_invoice,
                        supports_creditnote = EXCLUDED.supports_creditnote,
                        company_name = EXCLUDED.company_name,
                        country_code = EXCLUDED.country_code,
                        registration_date = EXCLUDED.registration_date,
                        entity_extra = EXCLUDED.entity_extra,
                        raw_document_types = EXCLUDED.raw_document_types,
                        document_types = EXCLUDED.document_types
                """, values)
        
        # Atomic swap
        with conn.transaction():
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS participants")
            cur.execute("ALTER TABLE participants_final RENAME TO participants")
        
        # Indexes with autocommit
        conn.autocommit = True
        indexes = [
            "idx_participants_full_pid ON participants(full_pid)",
            "idx_participants_endpoint_id ON participants(endpoint_id)",
            "idx_participants_scheme_id ON participants(scheme_id)",
            "idx_participants_supports_invoice ON participants(supports_invoice)",
            "idx_participants_supports_creditnote ON participants(supports_creditnote)",
            "idx_participants_country_code ON participants(country_code)"
        ]
        cur_idx = conn.cursor()
        for idx in indexes:
            try:
                cur_idx.execute(f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx}")
            except Exception as e:
                logging.warning(f"⚠️ Index creation warning: {e}")
        cur_idx.close()
        
        # Stats
        with conn.transaction():
            cur_stats = conn.cursor()
            cur_stats.execute("SELECT COUNT(*) FROM participants")
            count = cur_stats.fetchone()[0]
            logging.info(f"📊 Final count: {count}")
        logging.info("🎉 DB update complete!")

def main():
    try:
        download_file_with_retry(EXPORT_URL, XML_PATH)
        logging.info("📊 Parsing XML...")
        parser = xml.sax.make_parser()
        handler = PeppolHandler()
        parser.setContentHandler(handler)
        with open(XML_PATH, 'r', encoding='utf-8') as f:
            xml.sax.parse(f, handler)
        logging.info(f"✅ Parsed {handler.processed} cards, skipped {handler.skipped}, {len(handler.business_data)} unique")
        if handler.business_data:
            load_to_db(handler.business_data)
        else:
            raise ValueError("❌ No data parsed from XML!")
    except Exception as e:
        logging.error(f"💥 Error: {e}")
        raise
    finally:
        if os.path.exists(XML_PATH):
            os.remove(XML_PATH)
            logging.info("🧹 Cleaned XML")

if __name__ == "__main__":
    main()