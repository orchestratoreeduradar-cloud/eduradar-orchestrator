"""
Worker Task - Automazione NotebookLM
Usa i cookie per generare video e li carica su Supabase Storage
"""

import logging
import os
import random
import sys
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import requests
from supabase import create_client, Client
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
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


class SupabaseStorage:
    def __init__(self, url: str, key: str, bucket: str):
        self._client: Optional[Client] = None
        self.url = url
        self.key = key
        self.bucket = bucket

    @property
    def client(self) -> Client:
        if self._client is None:
            self._client = create_client(self.url, self.key)
        return self._client

    def upload_video(self, video_path: str) -> Optional[str]:
        file_name = f"{uuid.uuid4()}.mp4"
        date_path = datetime.now().strftime('%Y/%m/%d')
        storage_path = f"videos/{date_path}/{file_name}"
        
        logger.info(f"Upload video su Supabase Storage: {storage_path}")
        
        try:
            with open(video_path, "rb") as f:
                self.client.storage.from_(self.bucket).upload(
                    storage_path,
                    f,
                    file_options={"content-type": "video/mp4"}
                )
            
            public_url = self.client.storage.from_(self.bucket).get_public_url(storage_path)
            logger.info(f"Video caricato con successo: {public_url}")
            return public_url
        except Exception as e:
            logger.error(f"Errore upload Supabase Storage: {e}")
            return None

    def create_execution_log(self, worker_id: int, news_url: str) -> int:
        response = (
            self.client.table("execution_logs")
            .insert({
                "worker_id": worker_id,
                "news_url": news_url,
                "status": "running",
                "run_id": str(uuid.uuid4()),
            })
            .execute()
        )
        return response.data[0]["id"]

    def update_execution_log(
        self,
        log_id: int,
        status: str,
        error_message: Optional[str] = None,
        **kwargs,
    ):
        update_data = {"status": status}
        if error_message:
            update_data["error_message"] = error_message
        update_data.update(kwargs)
        update_data["completed_at"] = datetime.utcnow().isoformat()
        
        self.client.table("execution_logs").update(update_data).eq("id", log_id).execute()

    def release_worker(self, worker_id: int, success: bool, increment_video: bool = False):
        self.client.rpc(
            "release_worker_lock",
            {
                "p_worker_id": worker_id,
                "p_success": success,
                "p_increment_video": increment_video,
            }
        ).execute()

    def create_video_record(
        self,
        log_id: int,
        topic: str,
        news_url: str,
        video_size_mb: float,
        storage_url: str,
    ):
        self.client.table("videos").insert({
            "execution_log_id": log_id,
            "topic": topic,
            "original_news_url": news_url,
            "video_size_mb": video_size_mb,
            "storage_type": "supabase",
            "storage_url": storage_url,
            "telegram_sent": False,
        }).execute()

    def mark_news_completed(self, news_url: str, success: bool = True):
        status = "completed" if success else "failed"
        self.client.table("news_queue").update({
            "status": status,
            "updated_at": datetime.utcnow().isoformat(),
        }).eq("url", news_url).execute()


def jitter_sleep(min_seconds: float = 2.5, max_seconds: float = 7.8):
    delay = random.uniform(min_seconds, max_seconds)
    logger.debug(f"Jitter: attendo {delay:.2f}s")
    import time
    time.sleep(delay)


class NotebookLMClient:
    MAX_RETRIES = 3

    def __init__(self, cookies: str):
        self.cookies = cookies
        self._client = None
        self.notebook_id = None

    def _get_client(self):
        if self._client is None:
            # Prova a usare notebooklm_py se notebooklm fallisce
            try:
                from notebooklm import NotebookLM
            except ImportError:
                from notebooklm_py import NotebookLM
            
            self._client = NotebookLM(cookies=self.cookies)
        return self._client

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type(Exception),
    )
    def create_notebook(self, name: str) -> str:
        logger.info(f"Creazione notebook: {name}")
        jitter_sleep(3.0, 8.0)
        
        client = self._get_client()
        notebook = client.create_notebook(name=name)
        self.notebook_id = notebook.id
        logger.info(f"Notebook creato: {self.notebook_id}")
        return self.notebook_id

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type(Exception),
    )
    def add_sources(self, urls: list[str]):
        logger.info(f"Aggiunta {len(urls)} fonti al notebook")
        
        client = self._get_client()
        for url in urls:
            jitter_sleep()
            client.add_source(self.notebook_id, url=url)
            logger.debug(f"Aggiunta fonte: {url}")
        
        jitter_sleep(5.0, 15.0)

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type(Exception),
    )
    def create_presentation(self) -> str:
        logger.info("Generazione presentazione master")
        jitter_sleep(3.0, 8.0)
        
        client = self._get_client()
        presentation = client.studio.create_presentation(
            notebook_id=self.notebook_id,
            target_format="multimedia_pro",
        )
        
        logger.info(f"Presentazione generata: {presentation.id}")
        return presentation.id

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type(Exception),
    )
    def promote_to_source(self, presentation_id: str):
        logger.info("Promozione presentazione a fonte")
        jitter_sleep()
        
        client = self._get_client()
        client.promote_to_source(
            notebook_id=self.notebook_id,
            source_id=presentation_id,
        )

    def disable_original_sources(self, source_count: int):
        logger.info(f"Disabilitazione {source_count} fonti originali")
        
        client = self._get_client()
        sources = client.list_sources(self.notebook_id)
        
        for source in sources[:source_count]:
            jitter_sleep(1.0, 3.0)
            client.update_source(
                self.notebook_id,
                source.id,
                enabled=False,
            )

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type(Exception),
    )
    def generate_video(self, custom_instructions: str) -> str:
        logger.info("Generazione Video Overview")
        jitter_sleep(5.0, 10.0)
        
        client = self._get_client()
        video = client.studio.generate_artifact(
            notebook_id=self.notebook_id,
            artifact_type="video_overview",
            custom_instructions=custom_instructions,
        )
        
        logger.info(f"Video generato: {video.id}")
        return video.id

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type(Exception),
    )
    def download_video(self, video_id: str, output_path: str) -> str:
        logger.info("Download video")
        jitter_sleep()
        
        client = self._get_client()
        client.download_artifact(
            notebook_id=self.notebook_id,
            artifact_id=video_id,
            output_path=output_path,
        )
        
        logger.info(f"Video scaricato: {output_path}")
        return output_path

    def delete_notebook(self):
        if self.notebook_id:
            try:
                logger.info(f"Eliminazione notebook: {self.notebook_id}")
                client = self._get_client()
                client.delete_notebook(self.notebook_id)
                logger.info("Notebook eliminato con successo")
            except Exception as e:
                logger.error(f"Errore eliminazione notebook: {e}")


class VideoWorker:
    VIDEO_INSTRUCTIONS = """Esegui rigorosamente uno sliding sequenziale delle slide fornite.
Non aggiungere immagini o concetti esterni.
La narrazione deve commentare esclusivamente il contenuto testuale presente in ogni slide mentre viene visualizzata a schermo.
Ritmo da lezione accademica, pacato e didattico."""

    def __init__(self, config: WorkerConfig, cookies: str):
        self.config = config
        self.cookies = cookies
        self.storage = SupabaseStorage(
            config.supabase_url,
            config.supabase_key,
            config.supabase_bucket
        )
        self.notebook_client: Optional[NotebookLMClient] = None
        self.log_id: Optional[int] = None

    def run(self) -> ExecutionResult:
        try:
            if not self.cookies:
                raise ValueError("Cookie non forniti")

            self.notebook_client = NotebookLMClient(self.cookies)
            
            self.log_id = self.storage.create_execution_log(
                self.config.worker_id,
                self.config.news_url,
            )

            return self._execute_pipeline()

        except Exception as e:
            logger.exception(f"Errore durante esecuzione: {e}")
            return ExecutionResult(
                success=False,
                error_message=str(e),
            )
        finally:
            self._cleanup()

    def _execute_pipeline(self) -> ExecutionResult:
        notebook_name = f"Temp_News_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            self.notebook_client.create_notebook(notebook_name)

            sources = self._fetch_trusted_sources()
            all_urls = [self.config.news_url] + sources
            
            self.notebook_client.add_sources(all_urls)

            presentation_id = self.notebook_client.create_presentation()

            self.notebook_client.promote_to_source(presentation_id)

            self.notebook_client.disable_original_sources(len(all_urls))

            video_id = self.notebook_client.generate_video(self.VIDEO_INSTRUCTIONS)

            with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
                video_path = tmp.name
            
            self.notebook_client.download_video(video_id, video_path)

            video_size_mb = os.path.getsize(video_path) / (1024 * 1024)
            logger.info(f"Dimensione video: {video_size_mb:.2f} MB")

            storage_url = self.storage.upload_video(video_path)
            
            if not storage_url:
                raise RuntimeError("Upload video su Supabase Storage fallito")

            self.storage.update_execution_log(
                self.log_id,
                status="success",
                sources_found=len(sources),
                video_size_mb=video_size_mb,
                video_storage="supabase",
            )

            self.storage.create_video_record(
                log_id=self.log_id,
                topic=f"News Video {datetime.now().strftime('%Y-%m-%d')}",
                news_url=self.config.news_url,
                video_size_mb=video_size_mb,
                storage_url=storage_url,
            )

            self.storage.mark_news_completed(self.config.news_url, success=True)

            logger.info("=" * 50)
            logger.info("VIDEO CARICATO CON SUCCESSO")
            logger.info(f"URL: {storage_url}")
            logger.info("=" * 50)

            return ExecutionResult(
                success=True,
                video_path=video_path,
                video_size_mb=video_size_mb,
                storage_url=storage_url,
            )

        finally:
            if self.notebook_client:
                self.notebook_client.delete_notebook()

    def _fetch_trusted_sources(self) -> list[str]:
        logger.info("Recupero fonti attendibili...")
        
        try:
            response = (
                self.storage.client.table("trusted_domains")
                .select("domain")
                .eq("is_active", True)
                .order("priority", desc=True)
                .limit(10)
                .execute()
            )
            
            domains = [row["domain"] for row in response.data]
            logger.info(f"Domini attendibili caricati: {len(domains)}")
            
            return []
        except Exception as e:
            logger.error(f"Errore recupero domini: {e}")
            return []

    def _cleanup(self):
        success = True
        try:
            if hasattr(self, '_result'):
                success = self._result.success
        except:
            pass
        
        try:
            self.storage.release_worker(
                self.config.worker_id,
                success=success,
                increment_video=success,
            )
            logger.info(f"Worker {self.config.worker_id} rilasciato")
        except Exception as e:
            logger.error(f"Errore rilascio worker: {e}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Worker Task - Generazione Video")
    parser.add_argument("--worker-id", type=int, required=True)
    parser.add_argument("--news-url", type=str, required=True)
    
    args = parser.parse_args()

    cookies = os.getenv("GOOGLE_COOKIES", "")
    if not cookies:
        cookies = os.getenv("WORKER_1_COOKIES", "") or os.getenv("WORKER_2_COOKIES", "")
    
    if not cookies:
        logger.error("Nessun cookie Google configurato (GOOGLE_COOKIES, WORKER_1_COOKIES, WORKER_2_COOKIES)")
        sys.exit(1)
    
    supabase_url = os.getenv("SUPABASE_URL", "")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY", "")
    supabase_bucket = os.getenv("SUPABASE_VIDEO_BUCKET", "videos")
    
    if not supabase_url or not supabase_key:
        logger.error("SUPABASE_URL e SUPABASE_SERVICE_KEY devono essere configurati")
        sys.exit(1)
    
    config = WorkerConfig(
        worker_id=args.worker_id,
        news_url=args.news_url,
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        supabase_bucket=supabase_bucket,
    )
    
    worker = VideoWorker(config, cookies)
    result = worker.run()
    
    if result.success:
        logger.info("=" * 50)
        logger.info("COMPLETATO CON SUCCESSO!")
        logger.info(f"Video: {result.video_size_mb:.2f} MB")
        logger.info(f"Storage URL: {result.storage_url}")
        logger.info("=" * 50)
    else:
        logger.error(f"FALLITO: {result.error_message}")
        sys.exit(1)


if __name__ == "__main__":
    main()
