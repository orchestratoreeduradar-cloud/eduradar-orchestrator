"""
Orchestrator - Il "Regista" del sistema
Gestisce la coda news_queue e i lock dei worker
"""

import logging
import os
import sys
from datetime import datetime
from typing import Optional

from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class Orchestrator:
    MAX_WORKERS_TO_USE = 2
    MAX_NEWS_PER_RUN = 2

    def __init__(
        self,
        supabase_url: str,
        supabase_key: str,
    ):
        self._client: Optional[Client] = None
        self.supabase_url = supabase_url
        self.supabase_key = supabase_key

    @property
    def client(self) -> Client:
        if self._client is None:
            self._client = create_client(self.supabase_url, self.supabase_key)
        return self._client

    def reset_daily_counters(self) -> None:
        logger.info("Reset contatori giornalieri...")
        try:
            self.client.rpc("reset_daily_counters").execute()
            logger.info("Contatori resettati con successo")
        except Exception as e:
            logger.error(f"Errore reset contatori: {e}")

    def get_pending_news(self, limit: int = MAX_NEWS_PER_RUN) -> list[dict]:
        logger.info(f"Recupero notizie pending (limite: {limit})...")
        
        try:
            response = (
                self.client.table("news_queue")
                .select("*")
                .eq("status", "pending")
                .order("priority", desc=True)
                .order("created_at", desc=False)
                .limit(limit)
                .execute()
            )
            
            news_list = response.data
            logger.info(f"Trovate {len(news_list)} notizie pending")
            return news_list
        except Exception as e:
            logger.error(f"Errore recupero notizie: {e}")
            return []

    def get_available_workers(self) -> list[dict]:
        logger.info("Ricerca worker disponibili...")
        
        try:
            response = (
                self.client.table("workers")
                .select("*")
                .eq("is_locked", False)
                .eq("cookie_status", "active")
                .order("videos_today", desc=False)
                .order("last_run", desc=False)
                .limit(self.MAX_WORKERS_TO_USE)
                .execute()
            )
            
            workers = response.data
            logger.info(f"Trovati {len(workers)} worker disponibili")
            return workers
        except Exception as e:
            logger.error(f"Errore recupero worker: {e}")
            return []

    def acquire_worker_lock(self, worker_id: int, locked_by: str = "orchestrator") -> bool:
        logger.info(f"Tentativo acquisizione lock worker {worker_id}...")
        
        try:
            response = (
                self.client.table("workers")
                .update({
                    "is_locked": True,
                    "locked_at": datetime.utcnow().isoformat(),
                    "locked_by": locked_by,
                })
                .eq("id", worker_id)
                .eq("is_locked", False)
                .execute()
            )
            
            if response.data:
                logger.info(f"Lock acquisito per worker {worker_id}")
                return True
            else:
                logger.warning(f"Impossibile acquisire lock per worker {worker_id}")
                return False
        except Exception as e:
            logger.error(f"Errore acquisizione lock: {e}")
            return False

    def release_worker_lock(
        self,
        worker_id: int,
        success: bool = False,
        increment_video: bool = False,
    ) -> None:
        logger.info(f"Rilascio lock worker {worker_id}...")
        
        try:
            update_data = {
                "is_locked": False,
                "locked_at": None,
                "locked_by": None,
                "last_run": datetime.utcnow().isoformat(),
            }
            
            if success:
                update_data["last_success"] = datetime.utcnow().isoformat()
            else:
                update_data["last_failure"] = datetime.utcnow().isoformat()
            
            if increment_video:
                self.client.rpc(
                    "release_worker_lock",
                    {
                        "p_worker_id": worker_id,
                        "p_success": success,
                        "p_increment_video": increment_video,
                    }
                ).execute()
            else:
                self.client.table("workers").update(update_data).eq("id", worker_id).execute()
            
            logger.info(f"Lock rilasciato per worker {worker_id}")
        except Exception as e:
            logger.error(f"Errore rilascio lock: {e}")

    def mark_news_processing(self, news_id: int, worker_id: int) -> bool:
        logger.info(f"Marco notizia {news_id} come processing...")
        
        try:
            self.client.table("news_queue").update({
                "status": "processing",
                "assigned_worker_id": worker_id,
                "updated_at": datetime.utcnow().isoformat(),
            }).eq("id", news_id).execute()
            
            logger.info(f"Notizia {news_id} marcata come processing")
            return True
        except Exception as e:
            logger.error(f"Errore update notizia: {e}")
            return False

    def mark_news_completed(self, news_id: int, success: bool = True) -> None:
        status = "completed" if success else "failed"
        logger.info(f"Marco notizia {news_id} come {status}...")
        
        try:
            self.client.table("news_queue").update({
                "status": status,
                "updated_at": datetime.utcnow().isoformat(),
            }).eq("id", news_id).execute()
        except Exception as e:
            logger.error(f"Errore update notizia: {e}")

    def create_execution_log(self, worker_id: int, news_url: str, news_title: str = None) -> Optional[int]:
        logger.info(f"Creazione execution log per worker {worker_id}...")
        
        try:
            response = self.client.table("execution_logs").insert({
                "worker_id": worker_id,
                "news_url": news_url,
                "news_title": news_title,
                "status": "dispatched",
            }).execute()
            
            log_id = response.data[0]["id"]
            logger.info(f"Execution log creato: {log_id}")
            return log_id
        except Exception as e:
            logger.error(f"Errore creazione log: {e}")
            return None

    def run(self) -> dict:
        logger.info("=" * 60)
        logger.info("ORCHESTRATOR STARTED")
        logger.info(f"Timestamp: {datetime.utcnow().isoformat()}")
        logger.info("=" * 60)

        self.reset_daily_counters()

        pending_news = self.get_pending_news()
        
        if not pending_news:
            logger.info("Nessuna notizia da elaborare")
            return {"status": "no_news", "dispatched": 0}

        available_workers = self.get_available_workers()
        
        if not available_workers:
            logger.warning("Nessun worker disponibile")
            return {"status": "no_workers", "dispatched": 0}

        dispatched_count = 0
        results = []

        for i, news in enumerate(pending_news):
            if i >= len(available_workers):
                logger.info("Esauriti worker disponibili")
                break

            worker = available_workers[i]
            worker_id = worker["id"]
            news_id = news["id"]

            lock_acquired = self.acquire_worker_lock(worker_id)
            
            if not lock_acquired:
                logger.warning(f"Skip notizia {news_id}: lock non acquisito")
                continue

            self.mark_news_processing(news_id, worker_id)

            log_id = self.create_execution_log(
                worker_id,
                news["url"],
                news.get("title")
            )

            dispatch_info = {
                "news_id": news_id,
                "news_url": news["url"],
                "news_title": news.get("title"),
                "worker_id": worker_id,
                "account_id": worker["account_id"],
                "secret_name": worker.get("github_secret_name", ""),
                "log_id": log_id,
            }

            results.append(dispatch_info)
            dispatched_count += 1
            
            logger.info(f"Dispatch #{dispatched_count}:")
            logger.info(f"  - News: {news['url']}")
            logger.info(f"  - Worker: {worker['account_id']}")
            logger.info(f"  - Log ID: {log_id}")

        logger.info("=" * 60)
        logger.info(f"ORCHESTRATOR COMPLETED")
        logger.info(f"Dispatched: {dispatched_count} tasks")
        logger.info("=" * 60)

        return {
            "status": "success",
            "dispatched": dispatched_count,
            "tasks": results,
        }


def main():
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
    github_token = os.getenv("MY_GITHUB_TOKEN")
    
    if not supabase_url or not supabase_key:
        logger.error("SUPABASE_URL e SUPABASE_SERVICE_KEY devono essere impostati")
        sys.exit(1)
    
    if not github_token:
        logger.error("MY_GITHUB_TOKEN deve essere impostato")
        sys.exit(1)

    orchestrator = Orchestrator(supabase_url, supabase_key)
    result = orchestrator.run()

    print(f"\nResult: {result['status']}")
    print(f"Dispatched: {result['dispatched']}")

    if result["status"] != "success" or result["dispatched"] == 0:
        sys.exit(0)


if __name__ == "__main__":
    main()
