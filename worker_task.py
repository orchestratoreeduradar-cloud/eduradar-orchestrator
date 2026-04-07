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

@dataclass
class ExecutionResult:
    success: bool
    video_path: Optional[str] = None
    video_size_mb: float = 0.0
    storage_url: Optional[str] = None
    error_message: Optional[str] = None

# --- STORAGE CLASS (Rimasta quasi identica) ---
class SupabaseStorage:
    def __init__(self, url: str, key: str, bucket: str):
        self._client = create_client(url, key)
        self.bucket = bucket

    def upload_video(self, video_path: str) -> Optional[str]:
        file_name = f"{uuid.uuid4()}.mp4"
        storage_path = f"videos/{datetime.now().strftime('%Y/%m/%d')}/{file_name}"
        try:
            with open(video_path, "rb") as f:
                self._client.storage.from_(self.bucket).upload(
                    storage_path, f, {"content-type": "video/mp4"}
                )
            return self._client.storage.from_(self.bucket).get_public_url(storage_path)
        except Exception as e:
            logger.error(f"Errore upload Supabase: {e}")
            return None

    def create_execution_log(self, worker_id: int, news_url: str):
        res = self._client.table("execution_logs").insert({
            "worker_id": worker_id, "news_url": news_url, "status": "running", "run_id": str(uuid.uuid4())
        }).execute()
        return res.data[0]["id"]

    def update_execution_log(self, log_id: int, status: str, **kwargs):
        data = {"status": status, "completed_at": datetime.utcnow().isoformat()}
        data.update(kwargs)
        self._client.table("execution_logs").update(data).eq("id", log_id).execute()

# --- NEW NOTEBOOKLM CLIENT (PLAYWRIGHT) ---
class NotebookLMPlaywright:
    def __init__(self, auth_json_path: str):
        self.auth_json = auth_json_path

    async def run_pipeline(self, news_url: str, notebook_name: str) -> str:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(storage_state=self.auth_json)
            page = await context.new_page()

            try:
                logger.info("🌐 Navigazione verso NotebookLM...")
                await page.goto("https://notebooklm.google.com/", timeout=60000)
                
                # Screenshot per vedere se siamo loggati correttamente
                await page.screenshot(path="debug_dashboard.png")

                # 1. Crea Nuovo Notebook
                logger.info("➕ Creazione nuovo notebook...")
                btn_create = page.get_by_role("button", name=re.compile(r"(Create new|Crea nuovo)", re.IGNORECASE))
                await btn_create.click()
                
                await page.wait_for_timeout(5000)

                # 2. Aggiungi URL News come fonte
                # A volte bisogna cliccare sull'icona "Link" nella modale
                link_icon = page.get_by_role("button", name=re.compile(r"(Link|Collegamento)", re.IGNORECASE))
                if await link_icon.is_visible():
                    await link_icon.click()

                logger.info(f"🔗 Inserimento fonte: {news_url}")
                placeholder = page.get_by_placeholder(re.compile(r"https://", re.IGNORECASE))
                await placeholder.fill(news_url)
                await page.keyboard.press("Enter")
                
                # Aspettiamo che la fonte venga caricata e la modale si chiuda
                await page.wait_for_timeout(12000) 

                # 3. Generazione Guida Audio
                logger.info("🎙️ Apertura Guida del notebook...")
                btn_guide = page.get_by_role("button", name=re.compile(r"(Notebook guide|Guida del notebook)", re.IGNORECASE))
                await btn_guide.click()
                await page.wait_for_timeout(2000)

                logger.info("⚙️ Avvio generazione audio...")
                btn_gen = page.get_by_role("button", name=re.compile(r"(Generate|Genera)", re.IGNORECASE))
                await btn_gen.click()
                
                # 4. Attesa e Download
                logger.info("⏳ Generazione in corso (può volerci tempo)...")
                # Aspettiamo che il tasto "Download" o "Scarica" appaia
                selector_download = "text=/Download|Scarica/"
                await page.wait_for_selector(selector_download, timeout=300000)

                async with page.expect_download() as download_info:
                    await page.get_by_role("button", name=re.compile(r"(Download|Scarica)", re.IGNORECASE)).click()
                
                download = await download_info.value
                tmp_path = os.path.join(tempfile.gettempdir(), f"{uuid.uuid4()}.wav")
                await download.save_as(tmp_path)
                
                logger.info(f"✅ Audio scaricato in locale: {tmp_path}")
                return tmp_path

            except Exception as e:
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
        
        # Gestione auth.json
        auth_json_path = "auth.json"
        auth_data = os.getenv("NOTEBOOKLM_AUTH_JSON")
        if not auth_data:
            logger.error("Manca NOTEBOOKLM_AUTH_JSON!")
            return
        with open(auth_json_path, "w") as f:
            f.write(auth_data)

        try:
            nb_client = NotebookLMPlaywright(auth_json_path)
            notebook_name = f"News_{datetime.now().strftime('%Y%m%d_%H%M')}"
            
            # Eseguiamo il processo
            audio_path = await nb_client.run_pipeline(self.config.news_url, notebook_name)
            
            # Upload su Supabase
            storage_url = self.storage.upload_video(audio_path)
            
            self.storage.update_execution_log(log_id, "success", video_storage="supabase")
            logger.info(f"✅ Completato! URL: {storage_url}")
            
        except Exception as e:
            logger.error(f"❌ Fallimento: {e}")
            self.storage.update_execution_log(log_id, "failed", error_message=str(e))
        finally:
            if os.path.exists(auth_json_path): os.remove(auth_json_path)

async def main():
    # Prendi i dati dalle variabili d'ambiente (GitHub Secrets)
    config = WorkerConfig(
        worker_id=int(os.getenv("WORKER_ID", 1)),
        news_url=os.getenv("NEWS_URL", "https://example.com"),
        supabase_url=os.getenv("SUPABASE_URL", ""),
        supabase_key=os.getenv("SUPABASE_SERVICE_KEY", ""),
        supabase_bucket=os.getenv("SUPABASE_VIDEO_BUCKET", "videos")
    )
    
    worker = VideoWorker(config)
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
