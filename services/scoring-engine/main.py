from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, Text
from sqlalchemy.orm import declarative_base, sessionmaker
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json, os, time, threading, uuid, httpx

app = FastAPI(title="Scoring Engine", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    id                   = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    idea_id              = Column(String, unique=True, nullable=False)
    idea_title           = Column(String, default="Untitled Idea")
    final_score          = Column(Integer)
    verdict              = Column(String)
    trend_score          = Column(Integer)
    sentiment_score      = Column(Integer)
    saturation_score     = Column(Integer)
    problem_urgency      = Column(Integer)
    trend_direction      = Column(String)
    sentiment_label      = Column(String)
    saturation_level     = Column(String)
    competitor_count     = Column(Integer)
    recommendation       = Column(Text)
    tam_billion          = Column(Float)
    sam_billion          = Column(Float)
    som_billion          = Column(Float)
    market_growth        = Column(Float)
    swot_strengths       = Column(Text)
    swot_weaknesses      = Column(Text)
    swot_opportunities   = Column(Text)
    swot_threats         = Column(Text)
    ai_summary           = Column(Text)
    ai_swot_strengths    = Column(Text)
    ai_swot_weaknesses   = Column(Text)
    ai_swot_opportunities= Column(Text)
    ai_swot_threats      = Column(Text)
    ai_recommendation    = Column(Text)
    ai_risk_level        = Column(String)
    ai_risk_reason       = Column(Text)
    created_at           = Column(DateTime, default=datetime.utcnow)
    

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
    industry         = trend.get("industry", "technology")

    saturation_inverse = 100 - saturation_score

    final_score = int(
        (trend_score        * 0.30) +
        (sentiment_score    * 0.25) +
        (saturation_inverse * 0.25) +
        (problem_urgency    * 0.20)
    )
    final_score = max(0, min(100, final_score))

    if final_score >= 75:
        verdict = "Strong Go"
    elif final_score >= 55:
        verdict = "Promising"
    elif final_score >= 35:
        verdict = "Needs Work"
    else:
        verdict = "High Risk"

    # TAM/SAM/SOM
    base_markets = {
        "technology":    {"tam": 5200, "growth": 0.18},
        "healthcare":    {"tam": 8300, "growth": 0.15},
        "edtech":        {"tam": 404,  "growth": 0.16},
        "fintech":       {"tam": 3100, "growth": 0.20},
        "food delivery": {"tam": 900,  "growth": 0.12},
        "transportation":{"tam": 7500, "growth": 0.10},
        "agriculture":   {"tam": 1200, "growth": 0.08},
        "retail":        {"tam": 4200, "growth": 0.09},
        "entertainment": {"tam": 2800, "growth": 0.14},
        "real estate":   {"tam": 3700, "growth": 0.07},
    }
    market   = base_markets.get(industry.lower(), {"tam": 1000, "growth": 0.12})
    tam_b    = market["tam"]
    sam_b    = round(tam_b * 0.15, 1)
    som_b    = round(sam_b * 0.05, 1)

    # Rule-based SWOT fallback
    strengths, weaknesses, opportunities, threats = [], [], [], []

    if trend_direction == "rising":
        strengths.append("Market is trending upward with growing demand")
    if sentiment_label == "negative":
        strengths.append("Strong problem-market fit — users frustrated with alternatives")
    if saturation_level == "low":
        strengths.append("Low competition gives first-mover advantage")
    if problem_urgency > 70:
        strengths.append("High problem urgency indicates strong willingness to pay")
    if not strengths:
        strengths.append("Entering an established market with proven demand")

    if trend_direction == "declining":
        weaknesses.append("Market interest is declining — timing risk")
    if saturation_level == "high":
        weaknesses.append("Highly saturated market requires strong differentiation")
    if sentiment_score < 40:
        weaknesses.append("Negative public perception in this space")
    if competitor_count > 4:
        weaknesses.append(f"{competitor_count} direct competitors already established")
    if not weaknesses:
        weaknesses.append("Competitive market requires clear unique value proposition")

    if trend_direction in ["rising", "stable"]:
        opportunities.append("Growing market creates room for new entrants")
    opportunities.append("Underserved customer segments may exist in niche verticals")
    if saturation_level != "high":
        opportunities.append("Partnership opportunities with adjacent market players")
    opportunities.append("Digital-first approach can reduce customer acquisition costs")

    if saturation_level == "high":
        threats.append("Established competitors with large marketing budgets")
    threats.append("Potential regulatory changes in the industry")
    if trend_direction == "declining":
        threats.append("Shrinking market may limit long-term growth potential")
    threats.append("Risk of larger players copying the product if successful")

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
        tips.append("Strong problem-market fit — people frustrated with current solutions.")
    elif problem_urgency > 70:
        tips.append("High problem urgency — users actively seek a solution.")
    recommendation = " ".join(tips) if tips else "Solid idea with balanced market conditions."

    return {
        "final_score":        final_score,
        "verdict":            verdict,
        "trend_score":        trend_score,
        "sentiment_score":    sentiment_score,
        "saturation_score":   saturation_score,
        "problem_urgency":    problem_urgency,
        "trend_direction":    trend_direction,
        "sentiment_label":    sentiment_label,
        "saturation_level":   saturation_level,
        "competitor_count":   competitor_count,
        "recommendation":     recommendation,
        "tam_billion":        tam_b,
        "sam_billion":        sam_b,
        "som_billion":        som_b,
        "market_growth":      round(market["growth"] * 100, 1),
        "swot_strengths":     strengths,
        "swot_weaknesses":    weaknesses,
        "swot_opportunities": opportunities,
        "swot_threats":       threats,
    }

# ─── AI ENRICHMENT ────────────────────────────────────
def enrich_with_ai(idea_title: str, idea_description: str,
                   industry: str, score_data: dict) -> dict:
    try:
        import google.generativeai as genai

        genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
        model = genai.GenerativeModel("gemini-2.0-flash")

        prompt = f"""You are a startup advisor analyzing a business idea. Based on the data provided, give a detailed analysis.

Startup Idea: {idea_title}
Description: {idea_description}
Industry: {industry}
Validation Score: {score_data['final_score']}/100
Verdict: {score_data['verdict']}
Market Trend: {score_data['trend_direction']} (score: {score_data['trend_score']}/100)
Public Sentiment: {score_data['sentiment_label']} (score: {score_data['sentiment_score']}/100)
Competition Level: {score_data['saturation_level']} (score: {score_data['saturation_score']}/100)
Problem Urgency: {score_data['problem_urgency']}/100
Number of Competitors: {score_data['competitor_count']}
Total Addressable Market: ${score_data['tam_billion']}B

Respond with ONLY a valid JSON object. No markdown, no code blocks, no explanation. Just the raw JSON:
{{
  "summary": "2-3 sentence executive summary specific to this exact idea and its market position",
  "strengths": ["very specific strength 1 about this idea", "very specific strength 2", "very specific strength 3"],
  "weaknesses": ["very specific weakness 1 about this idea", "very specific weakness 2"],
  "opportunities": ["very specific opportunity 1 for this idea", "very specific opportunity 2", "very specific opportunity 3"],
  "threats": ["very specific threat 1 for this idea", "very specific threat 2"],
  "recommendation": "2-3 sentence actionable recommendation with concrete next steps specific to this idea",
  "risk_level": "Low",
  "risk_reason": "One sentence explaining the single biggest risk for this specific idea"
}}"""

        response = model.generate_content(prompt)
        text     = response.text.strip()
        text     = text.replace("```json", "").replace("```", "").strip()
        result   = json.loads(text)
        print(f"Gemini AI enrichment successful for: {idea_title}")
        return result

    except Exception as e:
        print(f"Gemini AI enrichment failed: {e}")
        return None

# ─── WAIT AND SCORE ───────────────────────────────────
def wait_and_score(idea_id: str, db, producer, retries=12, delay=5):
    for attempt in range(retries):
        trend      = db.market_data.find_one({"idea_id": idea_id})
        sentiment  = db.sentiment_data.find_one({"idea_id": idea_id})
        competitor = db.competitor_data.find_one({"idea_id": idea_id})

        if trend and sentiment and competitor:
            print(f"Scoring Engine: all data ready for {idea_id}")

            scored = calculate_score(trend, sentiment, competitor)

            # Get idea details
            idea_doc         = db.ideas_meta.find_one({"idea_id": idea_id}) or {}
            idea_title       = idea_doc.get("title", "Untitled Idea")
            idea_description = idea_doc.get("description", "")
            industry         = trend.get("industry", "technology")

            # AI enrichment
            ai_data = enrich_with_ai(idea_title, idea_description, industry, scored)
            if ai_data:
                scored["ai_summary"]              = ai_data.get("summary", "")
                scored["ai_swot_strengths"]       = json.dumps(ai_data.get("strengths", []))
                scored["ai_swot_weaknesses"]      = json.dumps(ai_data.get("weaknesses", []))
                scored["ai_swot_opportunities"]   = json.dumps(ai_data.get("opportunities", []))
                scored["ai_swot_threats"]         = json.dumps(ai_data.get("threats", []))
                scored["ai_recommendation"]       = ai_data.get("recommendation", "")
                scored["ai_risk_level"]           = ai_data.get("risk_level", "Medium")
                scored["ai_risk_reason"]          = ai_data.get("risk_reason", "")

            # Serialize SWOT lists to JSON strings
            for key in ["swot_strengths","swot_weaknesses",
                        "swot_opportunities","swot_threats"]:
                if key in scored and isinstance(scored[key], list):
                    scored[key] = json.dumps(scored[key])

            session = SessionLocal()
            try:
                existing = session.query(ScoreModel).filter_by(idea_id=idea_id).first()
                if existing:
                    for k, v in scored.items():
                        if hasattr(existing, k):
                            setattr(existing, k, v)
                    existing.idea_title = idea_title
                else:
                    session.add(ScoreModel(
                        idea_id    = idea_id,
                        idea_title = idea_title,
                        **scored
                    ))
                session.commit()
            except Exception as e:
                session.rollback()
                print(f"Scoring Engine DB error: {e}")
            finally:
                session.close()

            if producer:
                producer.send("score-ready", {
                    "idea_id":     idea_id,
                    "idea_title":  idea_title,
                    "final_score": scored["final_score"],
                    "verdict":     scored["verdict"]
                })
                producer.flush()

            print(f"Scoring Engine: score={scored['final_score']} "
                  f"verdict={scored['verdict']} title={idea_title}")
            return

        print(f"Scoring Engine: waiting ({attempt+1}/{retries}) — "
              f"trend={'yes' if trend else 'no'} "
              f"sentiment={'yes' if sentiment else 'no'} "
              f"competitor={'yes' if competitor else 'no'}")
        time.sleep(delay)

    print(f"Scoring Engine: timeout for {idea_id}")

# ─── KAFKA CONSUMER ───────────────────────────────────
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
        print(f"Scoring Engine: received {idea_id}")
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
            "idea_id":              score.idea_id,
            "idea_title":           score.idea_title,
            "final_score":          score.final_score,
            "verdict":              score.verdict,
            "trend_score":          score.trend_score,
            "sentiment_score":      score.sentiment_score,
            "saturation_score":     score.saturation_score,
            "problem_urgency":      score.problem_urgency,
            "trend_direction":      score.trend_direction,
            "sentiment_label":      score.sentiment_label,
            "saturation_level":     score.saturation_level,
            "competitor_count":     score.competitor_count,
            "recommendation":       score.recommendation,
            "tam_billion":          score.tam_billion,
            "sam_billion":          score.sam_billion,
            "som_billion":          score.som_billion,
            "market_growth":        score.market_growth,
            "swot_strengths":       json.loads(score.swot_strengths or "[]"),
            "swot_weaknesses":      json.loads(score.swot_weaknesses or "[]"),
            "swot_opportunities":   json.loads(score.swot_opportunities or "[]"),
            "swot_threats":         json.loads(score.swot_threats or "[]"),
            "ai_summary":           score.ai_summary,
            "ai_swot_strengths":    json.loads(score.ai_swot_strengths or "[]"),
            "ai_swot_weaknesses":   json.loads(score.ai_swot_weaknesses or "[]"),
            "ai_swot_opportunities":json.loads(score.ai_swot_opportunities or "[]"),
            "ai_swot_threats":      json.loads(score.ai_swot_threats or "[]"),
            "ai_recommendation":    score.ai_recommendation,
            "ai_risk_level":        score.ai_risk_level,
            "ai_risk_reason":       score.ai_risk_reason,
            "created_at":           score.created_at,
        }
    finally:
        session.close()

@app.get("/scores")
def get_all_scores(user_email: str = None):
    session = SessionLocal()
    try:
        if user_email:
            try:
                r = httpx.get(
                    f"http://idea-intake:8001/ideas/user/{user_email}",
                    timeout=5
                )
                user_ideas = r.json() if r.status_code == 200 else []
                idea_ids   = [i["id"] for i in user_ideas]
                scores     = session.query(ScoreModel).filter(
                    ScoreModel.idea_id.in_(idea_ids)
                ).order_by(ScoreModel.created_at.desc()).limit(50).all()
            except Exception as e:
                print(f"User filter error: {e}")
                scores = []
        else:
            scores = session.query(ScoreModel).order_by(
                ScoreModel.created_at.desc()
            ).limit(50).all()

        return [
            {
                "idea_id":     s.idea_id,
                "idea_title":  s.idea_title or "Untitled Idea",
                "final_score": s.final_score,
                "verdict":     s.verdict,
                "created_at":  s.created_at,
            }
            for s in scores
        ]
    finally:
        session.close()