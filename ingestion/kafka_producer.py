import json
import logging
from aiokafka import AIOKafkaProducer
from models import NewsArticle

logger = logging.getLogger(__name__)

class KafkaProducerWrapper:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None
        )
        await self.producer.start()
        logger.info("Kafka Producer started.")

    async def stop(self):
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka Producer stopped.")

    async def send_article(self, topic: str, article: NewsArticle):
        if not self.producer:
            raise RuntimeError("Producer is not started. Call start() first.")

        # Key partition routing by country code
        payload = article.model_dump()
        await self.producer.send_and_wait(
            topic=topic,
            key=article.source_country,
            value=payload
        )