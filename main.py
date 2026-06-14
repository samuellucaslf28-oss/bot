"""
Backend ZProCloud — compatível com o bot Zenith.
Expõe:
  - REST:     POST /api/auth/callback   (OAuth2 callback do Discord)
              POST /api/bot/register    (registrar credenciais)
              GET  /api/bots/{id}/verified-members
              GET  /api/bot/{id}/verified-count
  - Socket.IO: eventos register, check_user_verification, list_members,
               check_auth_count, recover, update_definitions, synchronization

Banco: MongoDB (mesmo do bot, collection separada "cloud_members")
"""

import os
import logging
import asyncio
import time
from datetime import datetime

import aiohttp
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
import socketio
import pymongo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cloud-backend")

# ─────────────────────────────────────────────────────────────────────────────
# Config (via env vars — Railway injeta automaticamente)
# ─────────────────────────────────────────────────────────────────────────────

MONGO_URL        = os.environ.get("MONGO_URL", "")
DB_NAME          = os.environ.get("DB_NAME", "Zpro")
PORT             = int(os.environ.get("PORT", 8080))

# ─────────────────────────────────────────────────────────────────────────────
# MongoDB
# ─────────────────────────────────────────────────────────────────────────────

mongo_client = pymongo.MongoClient(MONGO_URL)
db           = mongo_client[DB_NAME]
members_col  = db["cloud_members"]   # {user_id, bot_id, guild_id, access_token, refresh_token, verified_at, ...}
bots_col     = db["cloud_bots"]      # {bot_id, client_id, client_secret, token, main_server_id}

# índice para busca rápida
members_col.create_index([("bot_id", 1), ("user_id", 1)], unique=True)

# ─────────────────────────────────────────────────────────────────────────────
# FastAPI + Socket.IO
# ─────────────────────────────────────────────────────────────────────────────

sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")
app = FastAPI()
socket_app = socketio.ASGIApp(sio, other_asgi_app=app)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

async def exchange_code(code: str, redirect_uri: str, client_id: str, client_secret: str) -> dict:
    """Troca code OAuth2 por access_token."""
    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://discord.com/api/v10/oauth2/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        ) as resp:
            if resp.status != 200:
                raise HTTPException(400, f"Discord token exchange failed: {await resp.text()}")
            return await resp.json()


async def get_discord_user(access_token: str) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "https://discord.com/api/v10/users/@me",
            headers={"Authorization": f"Bearer {access_token}"},
        ) as resp:
            if resp.status != 200:
                raise HTTPException(400, "Failed to fetch Discord user")
            return await resp.json()


async def add_to_guild(bot_token: str, guild_id: str, user_id: str, access_token: str) -> bool:
    """Adiciona membro ao servidor via API do Discord."""
    async with aiohttp.ClientSession() as session:
        async with session.put(
            f"https://discord.com/api/v10/guilds/{guild_id}/members/{user_id}",
            headers={"Authorization": f"Bot {bot_token}", "Content-Type": "application/json"},
            json={"access_token": access_token},
        ) as resp:
            return resp.status in (201, 204)


def get_bot_config(bot_id: str) -> dict | None:
    return bots_col.find_one({"$or": [{"bot_id": bot_id}, {"client_id": bot_id}]}, {"_id": 0})


# ─────────────────────────────────────────────────────────────────────────────
# REST endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"status": "ok", "service": "ZProCloud Backend"}


@app.get("/api/auth/callback")
async def oauth_callback(request: Request):
    """Recebe o callback OAuth2 do Discord e salva o membro verificado."""
    params    = request.query_params
    code      = params.get("code")
    state     = params.get("state")   # formato: "{client_id}-{guild_id}"
    error     = params.get("error")

    if error or not code or not state:
        return HTMLResponse("""
        <html><body style='font-family:sans-serif;text-align:center;padding:50px'>
        <h2>❌ Verificação cancelada</h2>
        <p>Você cancelou a autorização. Feche esta janela e tente novamente.</p>
        </body></html>""", status_code=400)

    # Extrair client_id e guild_id do state
    try:
        parts    = state.split("-")
        client_id = parts[0]
        guild_id  = parts[1] if len(parts) > 1 else None
    except Exception:
        return HTMLResponse("<html><body>Estado inválido.</body></html>", status_code=400)

    # Buscar config do bot pelo client_id
    bot_cfg = bots_col.find_one({"client_id": client_id}, {"_id": 0})
    if not bot_cfg:
        return HTMLResponse("""
        <html><body style='font-family:sans-serif;text-align:center;padding:50px'>
        <h2>⚠️ Bot não registrado</h2>
        <p>Configure as credenciais no painel do bot primeiro.</p>
        </body></html>""", status_code=404)

    redirect_uri = f"{request.base_url}api/auth/callback"
    # Garantir https em produção
    if str(request.base_url).startswith("http://") and "localhost" not in str(request.base_url):
        redirect_uri = redirect_uri.replace("http://", "https://")

    try:
        token_data   = await exchange_code(code, redirect_uri, client_id, bot_cfg["client_secret"])
        access_token = token_data["access_token"]
        user_data    = await get_discord_user(access_token)
        user_id      = user_data["id"]
        username     = user_data.get("username", "")
    except Exception as e:
        logger.error(f"OAuth error: {e}")
        return HTMLResponse(f"""
        <html><body style='font-family:sans-serif;text-align:center;padding:50px'>
        <h2>❌ Erro na verificação</h2><p>{e}</p>
        </body></html>""", status_code=500)

    # Salvar/atualizar membro verificado
    now = datetime.utcnow().isoformat()
    members_col.update_one(
        {"bot_id": client_id, "user_id": user_id},
        {"$set": {
            "bot_id":        client_id,
            "user_id":       user_id,
            "username":      username,
            "guild_id":      guild_id,
            "access_token":  access_token,
            "refresh_token": token_data.get("refresh_token", ""),
            "verified_at":   now,
            "is_verified":   True,
        }},
        upsert=True,
    )
    logger.info(f"✅ Membro verificado: {username} ({user_id})")

    # Tentar adicionar ao servidor
    added = False
    if guild_id and bot_cfg.get("token"):
        added = await add_to_guild(bot_cfg["token"], guild_id, user_id, access_token)

    # Notificar o bot via Socket.IO (se conectado)
    await sio.emit("auth_log", {
        "success": True,
        "guild_id": guild_id,
        "user": {
            "id": user_id,
            "username": username,
            "verified_at": now,
        }
    })

    return HTMLResponse(f"""
    <html>
    <head><style>
      body{{font-family:sans-serif;text-align:center;padding:60px;background:#23272a;color:#fff}}
      .box{{background:#2c2f33;border-radius:12px;padding:40px;max-width:400px;margin:auto}}
      h2{{color:#57f287}}
    </style></head>
    <body><div class='box'>
      <h2>✅ Verificado com sucesso!</h2>
      <p>Olá, <b>{username}</b>!</p>
      <p>{'Você foi adicionado ao servidor.' if added else 'Seu cargo de verificado será atribuído em breve.'}</p>
      <p style='color:#aaa;font-size:13px'>Pode fechar esta janela.</p>
    </div></body></html>""")


@app.post("/api/bot/register")
async def register_bot_http(request: Request):
    """Registra credenciais do bot OAuth2."""
    body = await request.json()
    token         = body.get("token")
    client_secret = body.get("clientSecret")
    client_id     = body.get("clientId")
    main_bot_id   = body.get("mainBotId")

    if not all([token, client_secret, client_id, main_bot_id]):
        return JSONResponse({"success": False, "message": "Campos obrigatórios faltando"}, 400)

    # Validar token com Discord
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "https://discord.com/api/v10/users/@me",
            headers={"Authorization": f"Bot {token}"},
        ) as resp:
            if resp.status != 200:
                return JSONResponse({"success": False, "message": "Token do bot inválido."}, 400)
            bot_info = await resp.json()

    bots_col.update_one(
        {"client_id": client_id},
        {"$set": {
            "bot_id":        main_bot_id,
            "client_id":     client_id,
            "client_secret": client_secret,
            "token":         token,
            "name":          bot_info.get("username", ""),
            "registered_at": datetime.utcnow().isoformat(),
        }},
        upsert=True,
    )
    logger.info(f"✅ Bot registrado: {bot_info.get('username')} ({client_id})")
    return JSONResponse({"success": True, "message": "Bot registrado com sucesso!"})


@app.get("/api/bots/{bot_id}/verified-members")
async def get_verified_members(bot_id: str, limit: int = 100):
    """Retorna membros verificados para o /puxarmemb."""
    limit   = min(limit, 500)
    members = list(members_col.find(
        {"bot_id": bot_id, "is_verified": True},
        {"_id": 0, "user_id": 1, "access_token": 1, "username": 1, "guild_id": 1}
    ).limit(limit))
    # Renomear user_id → id para compatibilidade
    for m in members:
        m["id"] = m.pop("user_id", None)
    return {"success": True, "members": members, "total": len(members)}


@app.get("/api/bot/{bot_id}/verified-count")
async def verified_count(bot_id: str):
    count = members_col.count_documents({"bot_id": bot_id, "is_verified": True})
    return {"success": True, "count": count}


# ─────────────────────────────────────────────────────────────────────────────
# Socket.IO events
# ─────────────────────────────────────────────────────────────────────────────

@sio.event
async def connect(sid, environ):
    logger.info(f"[SIO] Cliente conectado: {sid}")


@sio.event
async def disconnect(sid):
    logger.info(f"[SIO] Cliente desconectado: {sid}")


@sio.on("register")
async def sio_register(sid, data):
    main_bot_id   = data.get("mainBotId")
    token         = data.get("token")
    client_secret = data.get("clientSecret")
    client_id     = data.get("clientId")

    if not all([main_bot_id, token, client_secret, client_id]):
        return {"success": False, "message": "Campos obrigatórios faltando"}

    async with aiohttp.ClientSession() as session:
        async with session.get(
            "https://discord.com/api/v10/users/@me",
            headers={"Authorization": f"Bot {token}"},
        ) as resp:
            if resp.status != 200:
                return {"success": False, "message": "Token do bot inválido."}
            bot_info = await resp.json()

    bots_col.update_one(
        {"client_id": client_id},
        {"$set": {
            "bot_id":        main_bot_id,
            "client_id":     client_id,
            "client_secret": client_secret,
            "token":         token,
            "name":          bot_info.get("username", ""),
            "registered_at": datetime.utcnow().isoformat(),
        }},
        upsert=True,
    )
    return {"success": True, "message": "Bot registrado com sucesso!"}


@sio.on("check_user_verification")
async def sio_check_verification(sid, data):
    bot_id  = data.get("botId")
    user_id = str(data.get("userId", ""))
    member  = members_col.find_one({"bot_id": bot_id, "user_id": user_id}, {"_id": 0})
    is_verified = bool(member and member.get("is_verified"))
    return {"success": True, "data": {"is_verified": is_verified}}


@sio.on("list_members")
async def sio_list_members(sid, data):
    bot_id  = data.get("botId")
    members = list(members_col.find(
        {"bot_id": bot_id, "is_verified": True},
        {"_id": 0, "user_id": 1, "access_token": 1, "username": 1}
    ))
    for m in members:
        m["id"] = m.pop("user_id", None)
    return {"success": True, "data": {"members": members, "count": len(members)}}


@sio.on("check_auth_count")
async def sio_auth_count(sid, data):
    bot_id = data.get("botId")
    count  = members_col.count_documents({"bot_id": bot_id, "is_verified": True})
    return {"success": True, "data": {"count": count}}


@sio.on("recover")
async def sio_recover(sid, data):
    bot_id  = data.get("botId")
    members = list(members_col.find(
        {"bot_id": bot_id, "is_verified": True},
        {"_id": 0, "user_id": 1, "access_token": 1, "username": 1, "guild_id": 1}
    ))
    for m in members:
        m["id"] = m.pop("user_id", None)
    return {"success": True, "data": {"members": members}}


@sio.on("update_definitions")
async def sio_update_definitions(sid, data):
    # Apenas ack — definições ficam no bot
    return {"success": True, "message": "Definições recebidas"}


@sio.on("synchronization")
async def sio_synchronization(sid, data):
    bot_id = data.get("botId")
    count  = members_col.count_documents({"bot_id": bot_id, "is_verified": True})
    return {"success": True, "data": {"verified_count": count}}


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(socket_app, host="0.0.0.0", port=PORT)
