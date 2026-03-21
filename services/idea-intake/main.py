from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, String, Text, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from kafka import KafkaProducer
from datetime import datetime
import json, os, uuid, time

app = FastAPI(title="Idea Intake Service", version="1.0.0")

# ─── DATABASE WITH RETRY ──────────────────────────────
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
            print("Connected to PostgreSQL successfully")
            return engine
        except Exception as e:
            print(f"Attempt {attempt+1}/{retries} - DB not ready: {e}")
            time.sleep(delay)
    raise Exception("Could not connect to PostgreSQL after retries")

engine = create_engine_with_retry(DB_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class IdeaModel(Base):
    __tablename__ = "ideas"
    id          = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_email  = Column(String, nullable=False)
    title       = Column(String, nullable=False)
    description = Column(Text, nullable=False)
    industry    = Column(String)
    created_at  = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(engine)

# ─── KAFKA PRODUCER ───────────────────────────────────
def get_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    except Exception:
        return None

# ─── MODELS ───────────────────────────────────────────
class IdeaInput(BaseModel):
    user_email: str
    title: str
    description: str
    industry: str

# ─── ROUTES ───────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "idea-intake"}

@app.post("/ideas")
def submit_idea(idea: IdeaInput):
    db = SessionLocal()
    try:
        new_idea = IdeaModel(
            user_email=idea.user_email,
            title=idea.title,
            description=idea.description,
            industry=idea.industry
        )
        db.add(new_idea)
        db.commit()
        db.refresh(new_idea)

        producer = get_producer()
        if producer:
            producer.send("idea-submitted", {
                "idea_id": new_idea.id,
                "title": new_idea.title,
                "description": new_idea.description,
                "industry": new_idea.industry
            })
            producer.flush()

        return {
            "idea_id": new_idea.id,
            "message": "Idea submitted and validation started"
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.get("/ideas/{idea_id}")
def get_idea(idea_id: str):
    db = SessionLocal()
    try:
        idea = db.query(IdeaModel).filter(IdeaModel.id == idea_id).first()
        if not idea:
            raise HTTPException(status_code=404, detail="Idea not found")
        return {
            "id": idea.id,
            "title": idea.title,
            "description": idea.description,
            "industry": idea.industry,
            "created_at": idea.created_at
        }
    finally:
        db.close()