import logging
import redis.asyncio as aioredis
from config import Config

logger = logging.getLogger(__name__)

class RedisClient:
    def __init__(self, host: str = Config.REDIS_HOST, port: int = Config.REDIS_PORT):
        self.host = host
        self.port = port
        self.redis = None

    async def connect(self):
        if not self.redis:
            self.redis = aioredis.Redis(host=self.host, port=self.port, decode_responses=True)
            logger.info("Connected to Redis cache.")

    async def close(self):
        if self.redis:
            await self.redis.close()
            logger.info("Closed Redis connection.")

    async def is_duplicate(self, url_hash: str, ttl_seconds: int = 604800) -> bool:
        """
        Uses SETNX (Set if Not Exists) to atomically record key.
        Returns True if the URL was ALREADY processed (duplicate).
        Returns False if it is NEW (successfully set in Redis).
        """
        if not self.redis:
            await self.connect()

        key = f"scraped:{url_hash}"
        # set with nx=True returns True if key was set (new item), None/False if existed
        was_set = await self.redis.set(name=key, value="1", ex=ttl_seconds, nx=True)
        
        return not was_set