import logging
import os
import sys
import re
import tempfile
import uuid
import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from playwright.async_api import async_playwright
from supabase import create_client, Client

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

@dataclass
class WorkerConfig:
    worker_id: int
    news_url: str
    supabase_url: str
    supabase_key: str
    supabase_bucket: str

# --- STORAGE CLASS ---
class SupabaseStorage:
    def __init__(self, url: str, key: str, bucket: str):
        self._client = create_client(url, key)
        self.bucket = bucket

    def upload_video(self, video_path: str) -> Optional[str]:
        file_name = f"{uuid.uuid4()}.wav" # NotebookLM scarica .wav
        storage_path = f"videos/{datetime.now().strftime('%Y/%m/%d')}/{file_name}"
        try:
            with open(video_path, "rb") as f:
                self._client.storage.from_(self.bucket).upload(
                    storage_path, f, {"content-type": "audio/wav"}
                )
            return self._client.storage.from_(self.bucket).get_public_url(storage_path)
        except Exception as e:
            logger.error(f"Errore upload Supabase: {e}")
            return None

    def create_execution_log(self, worker_id: int, news_url: str):
        res = self._client.table("execution_logs").insert({
            "worker_id": worker_id, 
            "news_url": news_url, 
            "status": "running", 
            "run_id": str(uuid.uuid4())
        }).execute()
        return res.data[0]["id"]

    def update_execution_log(self, log_id: int, status: str, **kwargs):
        data = {"status": status, "completed_at": datetime.utcnow().isoformat()}
        data.update(kwargs)
        self._client.table("execution_logs").update(data).eq("id", log_id).execute()

# --- NOTEBOOKLM CLIENT (PLAYWRIGHT) ---
class NotebookLMPlaywright:
    def __init__(self, auth_json_path: str):
        self.auth_json = auth_json_path

    async def run_pipeline(self, news_url: str, notebook_name: str) -> str:
        async with async_playwright() as p:
            # Headless=True per GitHub Actions
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(storage_state=self.auth_json)
            page = await context.new_page()

            try:
                logger.info("🌐 Navigazione verso NotebookLM...")
                await page.goto("https://notebooklm.google.com/", timeout=60000)
                await page.wait_for_load_state("networkidle")
                
                # Screenshot iniziale per debug
                await page.screenshot(path="debug_dashboard.png")

               # 1. Crea Nuovo Notebook
                logger.info("➕ Creazione nuovo notebook...")
                await page.get_by_role("button", name=re.compile(r"(Create new|Crea nuovo)", re.IGNORECASE)).first.click()
                
                # Aspettiamo che la modale si carichi bene
                await page.wait_for_timeout(5000)

                # 2. SELEZIONE FONTE "SITO WEB"
                logger.info("🔗 Selezione tipo fonte...")
                
                # Cerchiamo l'elemento che contiene l'icona del link
                # Usiamo una valutazione JavaScript per essere sicuri di colpire il centro del tasto
                try:
                    # Aspettiamo che la griglia delle fonti sia carica
                    await page.wait_for_selector("mat-icon", timeout=10000)
                    
                    # Troviamo il pulsante che contiene l'icona 'link' e clicchiamo via JS
                    await page.evaluate("""() => {
                        const icons = Array.from(document.querySelectorAll('mat-icon'));
                        const linkIcon = icons.find(i => i.textContent.includes('link'));
                        if (linkIcon) {
                            linkIcon.closest('button').click();
                        }
                    }""")
                    logger.info("✅ Click JS sulla fonte inviato.")
                except Exception as e:
                    logger.warning(f"⚠️ Click JS fallito: {e}. Provo metodo standard...")
                    await page.get_by_role("button", name=re.compile(r"(Website|Sito web|Link|Collegamento)", re.IGNORECASE)).first.click(force=True)

                # Diamo tempo alla finestra dell'URL di apparire (fondamentale)
                await page.wait_for_timeout(5000)

                # 3. INSERIMENTO URL
                logger.info(f"✍️ Inserimento URL: {news_url}")
                
                # Usiamo un selettore CSS che prende l'input ma ESCLUDE quello con classe title-input
                # Il selettore "input:not(.title-input)" è universale
                url_input = page.locator("input:not(.title-input)").first
                
                # Aspettiamo che sia pronto
                await url_input.wait_for(state="visible", timeout=15000)
                
                # Usiamo fill con force=True per ignorare il "velo" (backdrop)
                await url_input.fill(news_url, force=True)
                
                logger.info("🔗 URL inserito con successo.")
                await page.keyboard.press("Enter")
                
                # 4. CONFERMA (Tasto Aggiungi/Insert) via JavaScript
                await page.wait_for_timeout(3000)
                await page.evaluate("""() => {
                    const buttons = Array.from(document.querySelectorAll('button'));
                    const confirmBtn = buttons.find(b => 
                        b.textContent.includes('Insert') || 
                        b.textContent.includes('Aggiungi') || 
                        b.textContent.includes('Add') ||
                        b.textContent.includes('Conferma')
                    );
                    if (confirmBtn) confirmBtn.click();
                }""")
                
                # 4. Attesa e Download
                logger.info("⏳ Generazione in corso (può volerci qualche minuto)...")
                # Cerchiamo il testo Download o Scarica
                selector_download = "text=/Download|Scarica/"
                # Timeout di 5 minuti per la generazione
                await page.wait_for_selector(selector_download, timeout=300000)

                logger.info("💾 Avvio download file...")
                async with page.expect_download() as download_info:
                    await page.get_by_role("button", name=re.compile(r"(Download|Scarica)", re.IGNORECASE)).first.click()
                
                download = await download_info.value
                tmp_path = os.path.join(tempfile.gettempdir(), f"{uuid.uuid4()}.wav")
                await download.save_as(tmp_path)
                
                logger.info(f"✅ Audio scaricato con successo: {tmp_path}")
                return tmp_path

            except Exception as e:
                # In caso di errore, salva uno screenshot per capire dove si è fermato
                await page.screenshot(path="errore_esecuzione.png")
                logger.error(f"❌ Errore durante il workflow: {e}")
                raise e
            finally:
                await browser.close()

# --- MAIN WORKER LOGIC ---
class VideoWorker:
    def __init__(self, config: WorkerConfig):
        self.config = config
        self.storage = SupabaseStorage(config.supabase_url, config.supabase_key, config.supabase_bucket)

    async def run(self):
        log_id = self.storage.create_execution_log(self.config.worker_id, self.config.news_url)
        
        auth_json_path = "auth.json"
        auth_data = os.getenv("NOTEBOOKLM_AUTH_JSON")
        
        if not auth_data:
            logger.error("ERRORE: Variabile NOTEBOOKLM_AUTH_JSON non trovata!")
            self.storage.update_execution_log(log_id, "failed", error_message="Manca auth.json")
            return

        with open(auth_json_path, "w") as f:
            f.write(auth_data)

        try:
            nb_client = NotebookLMPlaywright(auth_json_path)
            notebook_name = f"EduRadar_{datetime.now().strftime('%Y%m%d_%H%M')}"
            
            # Esecuzione del processo su NotebookLM
            audio_path = await nb_client.run_pipeline(self.config.news_url, notebook_name)
            
            # Upload su Supabase Storage
            logger.info("📤 Caricamento su Supabase...")
            storage_url = self.storage.upload_video(audio_path)
            
            if storage_url:
                self.storage.update_execution_log(log_id, "success", video_storage=storage_url)
                logger.info(f"🚀 Missione compiuta! File disponibile qui: {storage_url}")
            else:
                raise Exception("Upload su storage fallito")
                
        except Exception as e:
            logger.error(f"❌ Fallimento Worker: {e}")
            self.storage.update_execution_log(log_id, "failed", error_message=str(e))
        finally:
            # Pulizia file temporanei
            if os.path.exists(auth_json_path): 
                os.remove(auth_json_path)
            if 'audio_path' in locals() and os.path.exists(audio_path):
                os.remove(audio_path)

async def main():
    # Caricamento configurazione da ambiente
    config = WorkerConfig(
        worker_id=int(os.getenv("WORKER_ID", 1)),
        news_url=os.getenv("NEWS_URL", "https://example.com"),
        supabase_url=os.getenv("SUPABASE_URL", ""),
        supabase_key=os.getenv("SUPABASE_SERVICE_KEY", ""),
        supabase_bucket=os.getenv("SUPABASE_VIDEO_BUCKET", "videos")
    )
    
    if not config.supabase_url or not config.supabase_key:
        logger.error("Configurazione Supabase incompleta!")
        return

    worker = VideoWorker(config)
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
