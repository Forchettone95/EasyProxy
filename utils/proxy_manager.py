import asyncio
import logging
import time
import threading
import cloudscraper
from typing import List, Dict, Optional, Callable, Any

logger = logging.getLogger(__name__)

class FreeProxyManager:
    """
    Manager for free proxy pools with parallel validation and caching.
    """
    _instances: Dict[str, 'FreeProxyManager'] = {}
    _lock = threading.Lock()

    def __init__(self, name: str, list_urls: List[str], cache_ttl: int = 1800, max_fetch: int = 0, max_good: int = 0):
        self.name = name
        self.list_urls = list_urls if isinstance(list_urls, list) else [list_urls]
        self.cache_ttl = cache_ttl
        self.max_fetch = max_fetch
        self.max_good = max_good
        self.proxies: List[str] = []
        self.expires_at: float = 0.0
        self.cursor: int = 0
        self._refresh_lock = asyncio.Lock()

    @classmethod
    def get_instance(cls, name: str, list_urls: List[str], **kwargs) -> 'FreeProxyManager':
        with cls._lock:
            if name not in cls._instances:
                cls._instances[name] = cls(name, list_urls, **kwargs)
            return cls._instances[name]

    def _normalize_proxy_url(self, proxy_value: str) -> str:
        proxy_value = proxy_value.strip()
        if not proxy_value:
            return ""
        if proxy_value.startswith("socks5://"):
            return proxy_value.replace("socks5://", "socks5h://", 1)
        if "://" not in proxy_value:
            return f"socks5h://{proxy_value}"
        return proxy_value

    async def _fetch_candidates(self) -> List[str]:
        all_candidates = []
        scraper = cloudscraper.create_scraper(delay=2)
        
        for url in self.list_urls:
            try:
                logger.debug(f"ProxyManager[{self.name}]: Fetching from {url}")
                resp = await asyncio.to_thread(scraper.get, url, timeout=25)
                resp.raise_for_status()
                
                count = 0
                for line in resp.text.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    normalized = self._normalize_proxy_url(line)
                    if normalized and normalized not in all_candidates:
                        all_candidates.append(normalized)
                        count += 1
                        if self.max_fetch > 0 and len(all_candidates) >= self.max_fetch:
                            break
                logger.info(f"ProxyManager[{self.name}]: Fetched {count} candidates from {url}")
                if self.max_fetch > 0 and len(all_candidates) >= self.max_fetch:
                    break
            except Exception as e:
                logger.warning(f"ProxyManager[{self.name}]: Failed to fetch proxy list from {url}: {e}")
        
        return all_candidates

    async def _probe_proxy_worker(self, proxy_url: str, probe_func: Callable[[str], Any], semaphore: asyncio.Semaphore, good_list: List[str]):
        # Se abbiamo già abbastanza proxy buoni, non serve continuare (se max_good > 0)
        if self.max_good > 0 and len(good_list) >= self.max_good:
            return

        async with semaphore:
            # Ri-controllo dopo il semaforo
            if self.max_good > 0 and len(good_list) >= self.max_good:
                return
                
            try:
                if asyncio.iscoroutinefunction(probe_func):
                    is_good = await probe_func(proxy_url)
                else:
                    is_good = await asyncio.to_thread(probe_func, proxy_url)
                
                if is_good:
                    if self.max_good <= 0 or len(good_list) < self.max_good:
                        good_list.append(proxy_url)
                        logger.info(f"ProxyManager[{self.name}]: Validated working proxy: {proxy_url}")
            except Exception:
                pass

    async def get_proxies(self, probe_func: Callable[[str], Any], force_refresh: bool = False) -> List[str]:
        now = time.time()
        if not force_refresh and self.proxies and self.expires_at > now:
            return list(self.proxies)

        async with self._refresh_lock:
            if not force_refresh and self.proxies and self.expires_at > time.time():
                return list(self.proxies)

            logger.info(f"ProxyManager[{self.name}]: Refreshing and validating free proxy pool (parallel, max_fetch={self.max_fetch})...")
            candidates = await self._fetch_candidates()
            if not candidates:
                return list(self.proxies)

            good = []
            semaphore = asyncio.Semaphore(100) # Concorrenza massiccia per check istantanei
            
            tasks = [self._probe_proxy_worker(c, probe_func, semaphore, good) for c in candidates]
            await asyncio.gather(*tasks)
            
            if good:
                self.proxies = good
                self.expires_at = time.time() + self.cache_ttl
                logger.info(f"ProxyManager[{self.name}]: Pool updated with {len(good)} working proxies.")
            else:
                logger.warning(f"ProxyManager[{self.name}]: No working proxies found in this batch.")
            
            return list(self.proxies)

    async def get_next_sequence(self, probe_func: Callable[[str], Any]) -> List[str]:
        proxies = await self.get_proxies(probe_func)
        if not proxies:
            return []
        
        idx = self.cursor % len(proxies)
        self.cursor = (idx + 1) % len(proxies)
        
        return proxies[idx:] + proxies[:idx]
