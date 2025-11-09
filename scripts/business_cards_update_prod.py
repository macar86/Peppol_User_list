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
import tempfile
import gc
import sys
from datetime import datetime
import time  # For timing logs
# === LOGGING: Console + File ===
log_file = '/tmp/peppol.log'
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.warning
# === CONFIG ===
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL eksik!")
EXPORT_URL = "https://directory.peppol.eu/export/businesscards"
XML_PATH = tempfile.mktemp(suffix=".xml")
MIN_FILE_SIZE_MB = 2400  # Minimum expected file size in MB to consider download complete
# === REGEX ===
PATTERN1 = re.compile(r"::([A-Za-z]+)-2::([A-Za-z]+)##")
PATTERN2 = re.compile(r"::([A-Za-z]+)##")
# === DOWNLOAD ===
def download_file_with_retry(url, file_path, max_retries=3, chunk_size=16384, min_speed_mb=5):
    session = requests.Session()
    http_retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=http_retries))
    
    for attempt in range(1, max_retries + 1):
        if os.path.exists(file_path):
            os.remove(file_path)  # Clean previous incomplete download
        
        start_time = datetime.now()
        download_start_time = time.time()
        last_log_time = download_start_time
        downloaded_size = 0
        log("📥 İndiriliyor %s... (Deneme %d/%d, Beklenen ~2.4 GB)", url, attempt, max_retries)
        
        try:
            response = session.get(url, stream=True, timeout=300)  # 5 min timeout for large file
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(file_path, 'wb') as f, tqdm(
                desc=f"İndirme İlerlemesi (Deneme {attempt})",
                total=total_size,
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
                mininterval=5.0  # Update progress bar every 5 seconds to reduce log spam
            ) as progress_bar:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        size = f.write(chunk)
                        downloaded_size += size
                        progress_bar.update(size)
                        
                        # Periodic logging every 5 seconds
                        current_time = time.time()
                        if current_time - last_log_time >= 5:
                            duration = current_time - download_start_time
                            avg_speed_bps = downloaded_size / duration if duration > 0 else 0
                            avg_speed_mbps = avg_speed_bps / (1024 * 1024)
                            downloaded_mb = downloaded_size / (1024 * 1024)
                            log("📊 İndirilen: %.1f MB, ortalama hız: %.2f MB/s", downloaded_mb, avg_speed_mbps)
                            last_log_time = current_time
            
            end_time = datetime.now()
            final_duration = (end_time - start_time).total_seconds()
            file_size_bytes = os.path.getsize(file_path)
            
            if file_size_bytes > 0 and final_duration > 0:
                avg_speed_bps = file_size_bytes / final_duration
                avg_speed_mbps = avg_speed_bps / (1024 * 1024)
                file_size_mb = file_size_bytes / (1024 * 1024)
                log("📊 İndirme tamam: %.2f MB, %.2f MB/s ortalama hız", file_size_mb, avg_speed_mbps)
                
                if file_size_mb < MIN_FILE_SIZE_MB:
                    log("⚠️ Dosya çok küçük: %.2f MB < %.0f MB. Tekrar deneniyor...", file_size_mb, MIN_FILE_SIZE_MB)
                    continue
                
                if avg_speed_mbps >= min_speed_mb:
                    log("✅ Geçerli indirme: %.2f MB, %.2f MB/s hızında", file_size_mb, avg_speed_mbps)
                    return  # Success
                else:
                    log("⚠️ Düşük hız: %.2f MB/s < %.0f MB/s. Tekrar deneniyor...", avg_speed_mbps, min_speed_mb)
                    continue
            else:
                log("⚠️ Geçersiz indirme (sıfır boyut/süre). Tekrar deneniyor...")
                continue
                
        except requests.exceptions.RequestException as e:
            log("⚠️ HTTP hatası deneme %d: %s. Tekrar deneniyor...", attempt, e)
            continue
        except Exception as e:
            log("⚠️ Beklenmedik hata deneme %d: %s. Tekrar deneniyor...", attempt, e)
            continue
    
    # If all retries fail
    raise ValueError(f"❌ İndirme {max_retries} denemeden sonra başarısız: düşük hız, küçük dosya veya hatalar nedeniyle.")
# === HANDLER (değişmedi) ===
class PeppolHandler(ContentHandler):
    def __init__(self, insert_func, batch_size=25000):
        self.insert = insert_func
        self.batch_size = batch_size
        self.batch = []
        self.current = None
        self.in_businesscard = False
        self.in_entity = False
        self.text = ""
        self.processed = 0
        self.skipped = 0
    def startElement(self, name, attrs):
        name = name.lower()
        if name == "businesscard":
            self.in_businesscard = True
            self.current = {"doctypeids": [], "entity": {}}
        elif not self.in_businesscard:
            return
        elif name == "participant":
            self.current["participant"] = attrs.get("value")
        elif name == "entity":
            self.in_entity = True
            self.current["entity"]["countrycode"] = attrs.get("countrycode")
        elif name == "name" and self.in_entity:
            self.current["entity"]["name"] = attrs.get("name", "").strip()
        elif name == "id" and self.in_entity:
            self.current["entity"].setdefault("ids", []).append(attrs.get("value"))
        elif name in ["geoinfo", "additionalinfo", "website", "regdate"] and self.in_entity:
            self.text = ""
        elif name == "contact" and self.in_entity:
            self.current["entity"]["contact"] = {
                "name": attrs.get("name"), "email": attrs.get("email"), "phone": attrs.get("phonenumber")
            }
        elif name == "doctypeid":
            val = attrs.get("value")
            if val:
                self.current["doctypeids"].append(val)
    def characters(self, content):
        if self.in_businesscard:
            self.text += content.strip()
    def endElement(self, name):
        name = name.lower()
        if name == "businesscard":
            self._process_card()
        elif self.in_entity and name in ["regdate", "geoinfo", "additionalinfo", "website"]:
            self.current["entity"][name] = self.text
            self.text = ""
        if name == "entity":
            self.in_entity = False
    def _process_card(self):
        try:
            pid = self.current.get("participant")
            if not pid or ":" not in pid:
                self.skipped += 1
                return
            scheme_id, endpoint_id = pid.split(":", 1)
            entity = self.current.get("entity", {})
            name = entity.get("name", "Unknown").strip()
            country = (entity.get("countrycode") or "").upper()
            raw_types = self.current.get("doctypeids", [])
            doc_types = [self._extract_type(v) for v in raw_types]
            uniq_types = list(set(filter(None, doc_types)))
            supports_invoice = any("invoice" in t.lower() for t in uniq_types if t)
            supports_creditnote = any("creditnote" in t.lower() for t in uniq_types if t)
            record = (
                pid, scheme_id, endpoint_id,
                supports_invoice, supports_creditnote,
                name, country or None, entity.get("regdate"),
                json.dumps({
                    "id": entity.get("ids", []),
                    "contact": entity.get("contact"),
                    "website": entity.get("website"),
                    "geoinfo": entity.get("geoinfo"),
                    "additionalinfo": entity.get("additionalinfo")
                }),
                ", ".join(raw_types),
                json.dumps(uniq_types)
            )
            self.batch.append(record)
            self.processed += 1
            if len(self.batch) >= self.batch_size:
                self.insert(self.batch)
                self.batch = []
                if self.processed % self.batch_size == 0:
                    log("📊 İşlenen: %s kart", f"{self.processed:,}")
        except Exception as e:
            self.skipped += 1
        finally:
            self.current = None
            self.in_businesscard = False
    def flush(self):
        if self.batch:
            self.insert(self.batch)
            log("🗄️ Son batch yazıldı: %d", len(self.batch))
        log("✅ Toplam: %s işlendi, %s atlandı", f"{self.processed:,}", f"{self.skipped:,}")
    @staticmethod
    def _extract_type(val):
        if not val: return None
        if "Invoice" in val and "CrossIndustryInvoice" not in val:
            return "Invoice"
        if "CreditNote" in val:
            return "CreditNote"
        if "ApplicationResponse" in val:
            return "ApplicationResponse"
        if "Order" in val:
            return "Order"
        m = PATTERN1.search(val)
        if m and m.group(2): return m.group(2)
        m = PATTERN2.search(val)
        if m and m.group(1): return m.group(1)
        return None
# === SAFE DB OP ===
def safe_db_op(conn, op_name, op_func, rollback_on_fail=True, use_transaction=True):
    try:
        # Only log start for non-batch operations to reduce spam
        if "Batch Insert" not in op_name and "Index" not in op_name:
            log("🔄 %s başlıyor...", op_name)
        start = datetime.now()
        if use_transaction:
            with conn.transaction():
                result = op_func() # Artık result dönebilir
        else:
            result = op_func()
        duration = (datetime.now() - start).total_seconds()
        # Only log completion for non-batch operations
        if "Batch Insert" not in op_name and "Index" not in op_name:
            log("✅ %s tamamlandı (%.1fs)", op_name, duration)
        return result
    except Exception as e:
        log("❌ %s HATASI: %s", op_name, str(e))
        if rollback_on_fail and use_transaction:
            conn.rollback()
        return None
# === DB FONK'LAR ===
def create_table(conn):
    def op():
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS participants_final_prod")
        cur.execute("""
            CREATE TABLE participants_final_prod (
                full_pid VARCHAR(500) PRIMARY KEY,
                scheme_id VARCHAR(50),
                endpoint_id VARCHAR(400),
                supports_invoice BOOLEAN,
                supports_creditnote BOOLEAN,
                company_name TEXT,
                country_code VARCHAR(10),
                registration_date VARCHAR(50),
                entity_extra JSONB,
                raw_document_types TEXT,
                document_types JSONB
            )
        """)
    safe_db_op(conn, "Tablo Oluşturma", op)
def insert_batch(batch, conn):
    if not batch: return
    def op():
        cur = conn.cursor()
        cur.executemany("""
            INSERT INTO participants_final_prod VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (full_pid) DO UPDATE SET
                scheme_id=EXCLUDED.scheme_id,
                endpoint_id=EXCLUDED.endpoint_id,
                supports_invoice=EXCLUDED.supports_invoice,
                supports_creditnote=EXCLUDED.supports_creditnote,
                company_name=EXCLUDED.company_name,
                country_code=EXCLUDED.country_code,
                registration_date=EXCLUDED.registration_date,
                entity_extra=EXCLUDED.entity_extra,
                raw_document_types=EXCLUDED.raw_document_types,
                document_types=EXCLUDED.document_types
        """, batch)
    safe_db_op(conn, f"Batch Insert ({len(batch)} kayıt)", op)
def get_count(conn, table="participants_final_prod"):
    def op():
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        log("📊 %s: %s kayıt", table, f"{count:,}")
        return count
    return safe_db_op(conn, f"Count Al ({table})", op) or 0 # None ise 0 dön
def swap_tables(conn):
    def op():
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS participants_prod")
        cur.execute("ALTER TABLE participants_final_prod RENAME TO participants_prod")
    safe_db_op(conn, "Tablo Swap", op)
def create_indexes(conn):
    indexes = [
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pid ON participants_prod(full_pid)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_invoice ON participants_prod(supports_invoice)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creditnote ON participants_prod(supports_creditnote)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_country ON participants_prod(country_code)"
    ]
    # Transaction dışı: autocommit True yap
    old_autocommit = conn.autocommit
    conn.autocommit = True
    try:
        for i, idx_sql in enumerate(indexes, 1):
            def op():
                cur = conn.cursor()
                cur.execute(idx_sql)
                log("✅ Index %d/4: %s", i, idx_sql.split('ON')[0].strip())
            safe_db_op(conn, f"Index {i}/4", op, rollback_on_fail=False, use_transaction=False) # No transaction for concurrent indexes
    finally:
        conn.autocommit = old_autocommit
# === MAIN ===
def main():
    global start_time
    start_time = datetime.now()
    try:
        download_file_with_retry(EXPORT_URL, XML_PATH, min_speed_mb=5)
        log("📊 XML parse ediliyor...")
        conn = psycopg.connect(DATABASE_URL, sslmode='require', connect_timeout=600)
        create_table(conn)
        get_count(conn) # Başlangıç: 0
        def inserter(batch):
            insert_batch(batch, conn)
        parser = xml.sax.make_parser()
        handler = PeppolHandler(inserter, batch_size=25000)
        parser.setContentHandler(handler)
        with open(XML_PATH, 'r', encoding='utf-8') as f:
            xml.sax.parse(f, handler)
        handler.flush()
        gc.collect()
        # Post-ops
        final_count = get_count(conn, "participants_final_prod")
        swap_tables(conn)
        prod_count = get_count(conn, "participants_prod")
        create_indexes(conn)
        total_duration = (datetime.now() - start_time).total_seconds()
        log("🎉 DB update tamam! Toplam süre: %.1fs | Kayıt: %s", total_duration, f"{prod_count:,}")
    except Exception as e:
        log("💥 Ana hata: %s", str(e))
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()
        if os.path.exists(XML_PATH):
            os.remove(XML_PATH)
        log("🧹 Cleanup tamam")
if __name__ == "__main__":
    main()
    sys.exit(0)