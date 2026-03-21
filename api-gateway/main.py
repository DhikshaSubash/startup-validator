from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
from passlib.context import CryptContext
from pydantic import BaseModel
from datetime import datetime, timedelta
import os
import warnings
warnings.filterwarnings("ignore", ".*error reading bcrypt version.*")

app = FastAPI(title="Startup Validator - API Gateway", version="1.0.0")

# ─── CORS ─────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── CONFIG ───────────────────────────────────────────
SECRET_KEY = os.getenv("SECRET_KEY", "fallback-secret")
ALGORITHM = "HS256"
TOKEN_EXPIRE_MINUTES = 60 * 24

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto", bcrypt__rounds=12)
security = HTTPBearer()

# ─── MODELS ───────────────────────────────────────────
class UserRegister(BaseModel):
    email: str
    password: str
    name: str

class UserLogin(BaseModel):
    email: str
    password: str

# ─── TEMP USER STORE ──────────────────────────────────
# We replace this with real PostgreSQL in the next step
fake_users = {}

# ─── HELPERS ──────────────────────────────────────────
def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)

def create_token(data: dict) -> str:
    payload = data.copy()
    payload["exp"] = datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRE_MINUTES)
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    try:
        payload = jwt.decode(
            credentials.credentials,
            SECRET_KEY,
            algorithms=[ALGORITHM]
        )
        email = payload.get("sub")
        if not email:
            raise HTTPException(status_code=401, detail="Invalid token")
        return email
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

# ─── ROUTES ───────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "api-gateway"}

@app.post("/auth/register")
def register(user: UserRegister):
    if user.email in fake_users:
        raise HTTPException(status_code=400, detail="Email already registered")
    fake_users[user.email] = {
        "name": user.name,
        "email": user.email,
        "password": hash_password(user.password)
    }
    token = create_token({"sub": user.email})
    return {"token": token, "message": "Registered successfully"}

@app.post("/auth/login")
def login(user: UserLogin):
    stored = fake_users.get(user.email)
    if not stored or not verify_password(user.password, stored["password"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = create_token({"sub": user.email})
    return {"token": token, "message": "Login successful"}

@app.get("/auth/me")
def me(current_user: str = Depends(get_current_user)):
    return {"email": current_user}