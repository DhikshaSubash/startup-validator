from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, Text
from sqlalchemy.orm import declarative_base, sessionmaker
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json, os, time, threading, uuid

app = FastAPI(title="Scoring Engine", version="1.0.0")

# ─── POSTGRESQL ───────────────────────────────────────
DB_URL = (
    f"postgresql://{os.getenv('POSTGRES_USER','admin')}:"
    f"{os.getenv('POSTGRES_PASSWORD','secret123')}@"
    f"postgres:5432/{os.getenv('POSTGRES_DB','startup_validator')}"
)

def create_engine_with_retry(url, retries=10, delay=3):
    for attempt in range(retries):
        try:
            engine = create_engine(url)
            engine.connect()
            print("Scoring Engine: PostgreSQL connected")
            return engine
        except Exception as e:
            print(f"DB attempt {attempt+1}/{retries}: {e}")
            time.sleep(delay)
    raise Exception("Could not connect to PostgreSQL")

engine       = create_engine_with_retry(DB_URL)
SessionLocal = sessionmaker(bind=engine)
Base         = declarative_base()

class ScoreModel(Base):
    __tablename__ = "scores"
    id                = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    idea_id           = Column(String, unique=True, nullable=False)
    final_score       = Column(Integer)
    verdict           = Column(String)
    trend_score       = Column(Integer)
    sentiment_score   = Column(Integer)
    saturation_score  = Column(Integer)
    problem_urgency   = Column(Integer)
    trend_direction   = Column(String)
    sentiment_label   = Column(String)
    saturation_level  = Column(String)
    competitor_count  = Column(Integer)
    recommendation    = Column(Text)
    created_at        = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(engine)

# ─── MONGODB ──────────────────────────────────────────
def get_mongo():
    client = MongoClient(os.getenv("MONGO_URI", "mongodb://mongo:27017/startup_validator"))
    return client.startup_validator

# ─── KAFKA PRODUCER ───────────────────────────────────
def get_producer():
    for i in range(10):
        try:
            return KafkaProducer(
                bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
        except Exception as e:
            print(f"Producer attempt {i+1}/10: {e}")
            time.sleep(3)
    return None

# ─── SCORING FORMULA ──────────────────────────────────
def calculate_score(trend: dict, sentiment: dict, competitor: dict) -> dict:
    trend_score      = trend.get("trend_score", 50)
    sentiment_score  = sentiment.get("sentiment_score", 50)
    saturation_score = competitor.get("saturation_score", 50)
    problem_urgency  = sentiment.get("problem_urgency", 50)
    trend_direction  = trend.get("trend_direction", "stable")
    sentiment_label  = sentiment.get("sentiment_label", "neutral")
    saturation_level = competitor.get("saturation_level", "medium")
    competitor_count = competitor.get("competitor_count", 0)

    # Inverse saturation — less competition is better
    saturation_inverse = 100 - saturation_score

    # Weighted formula
    final_score = int(
        (trend_score      * 0.30) +
        (sentiment_score  * 0.25) +
        (saturation_inverse * 0.25) +
        (problem_urgency  * 0.20)
    )

    final_score = max(0, min(100, final_score))

    # Verdict
    if final_score >= 75:
        verdict = "Strong Go"
    elif final_score >= 55:
        verdict = "Promising"
    elif final_score >= 35:
        verdict = "Needs Work"
    else:
        verdict = "High Risk"

    # Recommendation
    tips = []
    if trend_direction == "declining":
        tips.append("Market interest is declining — consider pivoting the industry angle.")
    elif trend_direction == "rising":
        tips.append("Market is trending upward — good timing to enter.")

    if saturation_level == "high":
        tips.append("High competition detected — focus on a strong differentiator.")
    elif saturation_level == "low":
        tips.append("Low competition — first-mover advantage possible.")

    if sentiment_label == "negative":
        tips.append("Strong problem-market fit — people are frustrated with current solutions.")
    elif problem_urgency > 70:
        tips.append("High problem urgency detected — users actively seek a solution.")

    recommendation = " ".join(tips) if tips else "Solid idea with balanced market conditions."

    return {
        "final_score":      final_score,
        "verdict":          verdict,
        "trend_score":      trend_score,
        "sentiment_score":  sentiment_score,
        "saturation_score": saturation_score,
        "problem_urgency":  problem_urgency,
        "trend_direction":  trend_direction,
        "sentiment_label":  sentiment_label,
        "saturation_level": saturation_level,
        "competitor_count": competitor_count,
        "recommendation":   recommendation
    }

# ─── WAIT FOR ALL DATA THEN SCORE ─────────────────────
def wait_and_score(idea_id: str, db, producer, retries=12, delay=5):
    for attempt in range(retries):
        trend      = db.market_data.find_one({"idea_id": idea_id})
        sentiment  = db.sentiment_data.find_one({"idea_id": idea_id})
        competitor = db.competitor_data.find_one({"idea_id": idea_id})

        if trend and sentiment and competitor:
            print(f"Scoring Engine: all data ready for {idea_id}")
            scored = calculate_score(trend, sentiment, competitor)

            session = SessionLocal()
            try:
                existing = session.query(ScoreModel).filter_by(idea_id=idea_id).first()
                if existing:
                    for k, v in scored.items():
                        setattr(existing, k, v)
                else:
                    session.add(ScoreModel(idea_id=idea_id, **scored))
                session.commit()
            finally:
                session.close()

            if producer:
                producer.send("score-ready", {"idea_id": idea_id, **scored})
                producer.flush()

            print(f"Scoring Engine: score={scored['final_score']} verdict={scored['verdict']}")
            return

        print(f"Scoring Engine: waiting for data ({attempt+1}/{retries}) - "
              f"trend={'yes' if trend else 'no'} "
              f"sentiment={'yes' if sentiment else 'no'} "
              f"competitor={'yes' if competitor else 'no'}")
        time.sleep(delay)

    print(f"Scoring Engine: timeout waiting for all data for {idea_id}")

# ─── KAFKA CONSUMER THREAD ────────────────────────────
def consume_ideas():
    for i in range(15):
        try:
            consumer = KafkaConsumer(
                "idea-submitted",
                bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="scoring-engine-group",
                auto_offset_reset="earliest"
            )
            print("Scoring Engine: Kafka connected")
            break
        except Exception as e:
            print(f"Consumer attempt {i+1}/15: {e}")
            time.sleep(4)
            if i == 14:
                return

    producer = get_producer()
    db       = get_mongo()

    for message in consumer:
        idea    = message.value
        idea_id = idea.get("idea_id")
        print(f"Scoring Engine: received idea {idea_id}")
        t = threading.Thread(
            target=wait_and_score,
            args=(idea_id, db, producer),
            daemon=True
        )
        t.start()

@app.on_event("startup")
def startup_event():
    t = threading.Thread(target=consume_ideas, daemon=True)
    t.start()

# ─── ROUTES ───────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "scoring-engine"}

@app.get("/score/{idea_id}")
def get_score(idea_id: str):
    session = SessionLocal()
    try:
        score = session.query(ScoreModel).filter_by(idea_id=idea_id).first()
        if not score:
            raise HTTPException(status_code=404, detail="Score not ready yet")
        return {
            "idea_id":          score.idea_id,
            "final_score":      score.final_score,
            "verdict":          score.verdict,
            "trend_score":      score.trend_score,
            "sentiment_score":  score.sentiment_score,
            "saturation_score": score.saturation_score,
            "problem_urgency":  score.problem_urgency,
            "trend_direction":  score.trend_direction,
            "sentiment_label":  score.sentiment_label,
            "saturation_level": score.saturation_level,
            "competitor_count": score.competitor_count,
            "recommendation":   score.recommendation,
            "created_at":       score.created_at
        }
    finally:
        session.close()

@app.get("/scores")
def get_all_scores():
    session = SessionLocal()
    try:
        scores = session.query(ScoreModel).order_by(ScoreModel.created_at.desc()).limit(20).all()
        return [
            {
                "idea_id":     s.idea_id,
                "final_score": s.final_score,
                "verdict":     s.verdict,
                "created_at":  s.created_at
            }
            for s in scores
        ]
    finally:
        session.close()