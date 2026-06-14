"""
Backend ZProCloud — compatível com o bot Zenith.
"""

import os
import logging
import asyncio
from datetime import datetime

import aiohttp
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
import socketio
import pymongo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cloud-backend")

MONGO_URL = os.environ.get("MONGO_URL", "")
DB_NAME   = os.environ.get("DB_NAME", "Zpro")
PORT      = int(os.environ.get("PORT", 8080))

mongo_client = pymongo.MongoClient(MONGO_URL)
db           = mongo_client[DB_NAME]
members_col  = db["cloud_members"]
bots_col     = db["cloud_bots"]

members_col.create_index([("bot_id", 1), ("user_id", 1)], unique=True)

sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")
app = FastAPI()
socket_app = socketio.ASGIApp(sio, other_asgi_app=app)

# Mapa de sid conectados por bot_id
connected_bots: dict[str, str] = {}  # bot_id → sid


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

async def exchange_code(code: str, redirect_uri: str, client_id: str, client_secret: str) -> dict:
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


async def get_user_guilds(access_token: str) -> list:
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "https://discord.com/api/v10/users/@me/guilds",
            headers={"Authorization": f"Bearer {access_token}"},
        ) as resp:
            if resp.status != 200:
                return []
            return await resp.json()


async def get_user_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "Desconhecido"


async def get_ip_info(ip: str) -> dict:
    """Busca país/cidade/ISP do IP via ip-api.com (gratuito)."""
    if ip in ("Desconhecido", "127.0.0.1", "::1"):
        return {}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"http://ip-api.com/json/{ip}?fields=country,regionName,city,isp,org,query",
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
    except Exception:
        pass
    return {}


async def add_to_guild(bot_token: str, guild_id: str, user_id: str, access_token: str) -> bool:
    async with aiohttp.ClientSession() as session:
        async with session.put(
            f"https://discord.com/api/v10/guilds/{guild_id}/members/{user_id}",
            headers={"Authorization": f"Bot {bot_token}", "Content-Type": "application/json"},
            json={"access_token": access_token},
        ) as resp:
            return resp.status in (201, 204)


def get_bot_config(client_id: str) -> dict | None:
    return bots_col.find_one({"client_id": client_id}, {"_id": 0})


async def notify_bot_verification(client_id: str, payload: dict):
    """Envia evento auth_log para o bot via Socket.IO."""
    sid = connected_bots.get(client_id)
    if sid:
        try:
            await sio.emit("auth_log", payload, to=sid)
            logger.info(f"✅ auth_log enviado para bot {client_id} (sid {sid})")
        except Exception as e:
            logger.warning(f"Erro ao notificar bot: {e}")
    else:
        logger.info(f"Bot {client_id} não conectado via Socket.IO")


# ─────────────────────────────────────────────────────────────────────────────
# REST
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"status": "ok", "service": "ZProCloud Backend"}


@app.get("/api/auth/callback")
async def oauth_callback(request: Request):
    params = request.query_params
    code   = params.get("code")
    state  = params.get("state")
    error  = params.get("error")

    if error or not code or not state:
        return HTMLResponse("""
        <html><head><style>
          body{font-family:sans-serif;text-align:center;padding:60px;background:#23272a;color:#fff}
          .box{background:#2c2f33;border-radius:12px;padding:40px;max-width:400px;margin:auto}
          h2{color:#ed4245}
        </style></head><body><div class='box'>
          <h2>❌ Verificação cancelada</h2>
          <p>Você cancelou a autorização. Feche esta janela e tente novamente.</p>
        </div></body></html>""", status_code=400)

    try:
        parts     = state.split("-")
        client_id = parts[0]
        guild_id  = parts[1] if len(parts) > 1 else None
    except Exception:
        return HTMLResponse("<html><body>Estado inválido.</body></html>", status_code=400)

    bot_cfg = get_bot_config(client_id)
    if not bot_cfg:
        return HTMLResponse("""
        <html><head><style>
          body{font-family:sans-serif;text-align:center;padding:60px;background:#23272a;color:#fff}
          .box{background:#2c2f33;border-radius:12px;padding:40px;max-width:400px;margin:auto}
          h2{color:#f39c12}
        </style></head><body><div class='box'>
          <h2>⚠️ Bot não registrado</h2>
          <p>Configure as credenciais no painel do bot primeiro.</p>
        </div></body></html>""", status_code=404)

    # Montar redirect_uri
    base = str(request.base_url).rstrip("/")
    if not base.startswith("https") and "localhost" not in base:
        base = base.replace("http://", "https://")
    redirect_uri = f"{base}/api/auth/callback"

    try:
        token_data   = await exchange_code(code, redirect_uri, client_id, bot_cfg["client_secret"])
        access_token = token_data["access_token"]
        user_data    = await get_discord_user(access_token)
        user_id      = user_data["id"]
        username     = user_data.get("username", "")
        discriminator = user_data.get("discriminator", "0")
        avatar_hash  = user_data.get("avatar")
        avatar_url   = f"https://cdn.discordapp.com/avatars/{user_id}/{avatar_hash}.png" if avatar_hash else f"https://cdn.discordapp.com/embed/avatars/0.png"
        email        = user_data.get("email", "Não autorizado")
        verified_email = user_data.get("verified", False)
        mfa_enabled  = user_data.get("mfa_enabled", False)
    except Exception as e:
        logger.error(f"OAuth error: {e}")
        return HTMLResponse(f"""
        <html><head><style>
          body{{font-family:sans-serif;text-align:center;padding:60px;background:#23272a;color:#fff}}
          .box{{background:#2c2f33;border-radius:12px;padding:40px;max-width:400px;margin:auto}}
          h2{{color:#ed4245}}
        </style></head><body><div class='box'>
          <h2>❌ Erro na verificação</h2><p>{e}</p>
        </div></body></html>""", status_code=500)

    # Coletar IP e geo
    ip = await get_user_ip(request)
    geo = await get_ip_info(ip)

    # Blacklist check — busca na collection do bot
    try:
        bot_collection = mongo_client[DB_NAME][bot_cfg.get("bot_id", client_id)]
        bl_doc = bot_collection.find_one({"_id": "verificacao_blacklist"}) or {}
        if user_id in bl_doc.get("users", []):
            logger.warning(f"🚫 Blacklist: {username} ({user_id})")
            # Logar tentativa bloqueada
            try:
                logs_doc = bot_collection.find_one({"_id": "verificacao_logs"}) or {}
                logs = logs_doc.get("logs", [])
                logs.insert(0, {"user_id": user_id, "username": username, "action": "blocked",
                                "by": "blacklist", "timestamp": int(datetime.utcnow().timestamp())})
                bot_collection.replace_one({"_id": "verificacao_logs"},
                    {"_id": "verificacao_logs", "logs": logs[:100]}, upsert=True)
            except Exception:
                pass
            # Notificar bot
            await notify_bot_verification(client_id, {
                "success": False, "blocked": True, "guild_id": guild_id,
                "ip": ip, "geo": geo,
                "user": {"id": user_id, "username": username, "avatar": avatar_url,
                         "email": email, "verified_email": verified_email, "mfa": mfa_enabled},
            })
            return HTMLResponse("""
            <html><head><style>
              body{font-family:sans-serif;text-align:center;padding:60px;background:#23272a;color:#fff}
              .box{background:#2c2f33;border-radius:12px;padding:40px;max-width:400px;margin:auto}
              h2{color:#ed4245}
            </style></head><body><div class='box'>
              <h2>🚫 Acesso Negado</h2>
              <p>Você não tem permissão para verificar neste servidor.</p>
              <p style='color:#aaa;font-size:13px'>Entre em contato com a administração se acredita que isso é um erro.</p>
            </div></body></html>""", status_code=403)
    except Exception as e:
        logger.warning(f"Erro ao checar blacklist: {e}")

    now = datetime.utcnow().isoformat()

    # Salvar membro verificado
    members_col.update_one(
        {"bot_id": client_id, "user_id": user_id},
        {"$set": {
            "bot_id": client_id, "user_id": user_id,
            "username": username, "guild_id": guild_id,
            "access_token": access_token,
            "refresh_token": token_data.get("refresh_token", ""),
            "verified_at": now, "is_verified": True,
            "ip": ip, "geo": geo,
        }},
        upsert=True,
    )

    # Salvar log no bot
    try:
        bot_collection = mongo_client[DB_NAME][bot_cfg.get("bot_id", client_id)]
        logs_doc = bot_collection.find_one({"_id": "verificacao_logs"}) or {}
        logs = logs_doc.get("logs", [])
        logs.insert(0, {"user_id": user_id, "username": username, "action": "verified",
                        "by": "oauth2", "timestamp": int(datetime.utcnow().timestamp()),
                        "ip": ip, "geo": geo})
        bot_collection.replace_one({"_id": "verificacao_logs"},
            {"_id": "verificacao_logs", "logs": logs[:100]}, upsert=True)
    except Exception as e:
        logger.warning(f"Erro ao salvar log: {e}")

    logger.info(f"✅ Verificado: {username} ({user_id}) — IP: {ip}")

    # Adicionar ao servidor
    added = False
    if guild_id and bot_cfg.get("token"):
        added = await add_to_guild(bot_cfg["token"], guild_id, user_id, access_token)

    # Notificar bot via Socket.IO com TODOS os dados para o log bonito
    await notify_bot_verification(client_id, {
        "success": True, "guild_id": guild_id,
        "ip": ip, "geo": geo,
        "user": {
            "id": user_id, "username": username,
            "discriminator": discriminator,
            "avatar": avatar_url,
            "email": email,
            "verified_email": verified_email,
            "mfa": mfa_enabled,
            "verified_at": now,
        },
    })

    return HTMLResponse(f"""
    <html>
    <head><style>
      body{{font-family:sans-serif;text-align:center;padding:60px;background:#23272a;color:#fff}}
      .box{{background:#2c2f33;border-radius:12px;padding:40px;max-width:400px;margin:auto}}
      h2{{color:#57f287}} .tag{{background:#5c5ef0;border-radius:6px;padding:2px 10px;font-size:13px}}
    </style></head>
    <body><div class='box'>
      <img src='{avatar_url}' style='width:72px;height:72px;border-radius:50%;margin-bottom:16px'><br>
      <h2>✅ Verificado com sucesso!</h2>
      <p>Olá, <b>{username}</b>!</p>
      <p>{'Você foi adicionado ao servidor.' if added else 'Seu cargo de verificado será atribuído em breve.'}</p>
      <p style='color:#aaa;font-size:13px'>Pode fechar esta janela.</p>
    </div></body></html>""")


@app.post("/api/bot/register")
async def register_bot_http(request: Request):
    body = await request.json()
    token = body.get("token")
    client_secret = body.get("clientSecret")
    client_id = body.get("clientId")
    main_bot_id = body.get("mainBotId")

    if not all([token, client_secret, client_id, main_bot_id]):
        return JSONResponse({"success": False, "message": "Campos obrigatórios faltando"}, 400)

    async with aiohttp.ClientSession() as session:
        async with session.get("https://discord.com/api/v10/users/@me",
                               headers={"Authorization": f"Bot {token}"}) as resp:
            if resp.status != 200:
                return JSONResponse({"success": False, "message": "Token do bot inválido."}, 400)
            bot_info = await resp.json()

    bots_col.update_one({"client_id": client_id}, {"$set": {
        "bot_id": main_bot_id, "client_id": client_id,
        "client_secret": client_secret, "token": token,
        "name": bot_info.get("username", ""),
        "registered_at": datetime.utcnow().isoformat(),
    }}, upsert=True)

    return JSONResponse({"success": True, "message": "Bot registrado com sucesso!"})


@app.get("/api/bots/{bot_id}/verified-members")
async def get_verified_members(bot_id: str, limit: int = 100):
    limit = min(limit, 500)
    members = list(members_col.find(
        {"bot_id": bot_id, "is_verified": True},
        {"_id": 0, "user_id": 1, "access_token": 1, "username": 1, "guild_id": 1}
    ).limit(limit))
    for m in members:
        m["id"] = m.pop("user_id", None)
    return {"success": True, "members": members, "total": len(members)}


@app.get("/api/bot/{bot_id}/verified-count")
async def verified_count(bot_id: str):
    count = members_col.count_documents({"bot_id": bot_id, "is_verified": True})
    return {"success": True, "count": count}


# ─────────────────────────────────────────────────────────────────────────────
# Socket.IO
# ─────────────────────────────────────────────────────────────────────────────

@sio.event
async def connect(sid, environ, auth):
    # Aceitar qualquer conexão — autenticação é feita pelo identify event
    logger.info(f"[SIO] Conectado: {sid}")


@sio.event
async def disconnect(sid):
    # Remover do mapa
    for bot_id, s in list(connected_bots.items()):
        if s == sid:
            del connected_bots[bot_id]
            logger.info(f"[SIO] Bot {bot_id} desconectado")
    logger.info(f"[SIO] Desconectado: {sid}")


@sio.on("identify")
async def sio_identify(sid, data):
    """Bot se identifica com seu client_id para receber notificações."""
    client_id = data.get("clientId") or data.get("botId")
    if client_id:
        connected_bots[client_id] = sid
        logger.info(f"[SIO] Bot {client_id} identificado → sid {sid}")
    return {"success": True}


@sio.on("register")
async def sio_register(sid, data):
    main_bot_id = data.get("mainBotId")
    token = data.get("token")
    client_secret = data.get("clientSecret")
    client_id = data.get("clientId")

    if not all([main_bot_id, token, client_secret, client_id]):
        return {"success": False, "message": "Campos obrigatórios faltando"}

    async with aiohttp.ClientSession() as session:
        async with session.get("https://discord.com/api/v10/users/@me",
                               headers={"Authorization": f"Bot {token}"}) as resp:
            if resp.status != 200:
                return {"success": False, "message": "Token do bot inválido."}
            bot_info = await resp.json()

    bots_col.update_one({"client_id": client_id}, {"$set": {
        "bot_id": main_bot_id, "client_id": client_id,
        "client_secret": client_secret, "token": token,
        "name": bot_info.get("username", ""),
        "registered_at": datetime.utcnow().isoformat(),
    }}, upsert=True)

    # Registrar sid
    connected_bots[client_id] = sid
    return {"success": True, "message": "Bot registrado com sucesso!"}


@sio.on("check_user_verification")
async def sio_check_verification(sid, data):
    bot_id = data.get("botId")
    user_id = str(data.get("userId", ""))
    member = members_col.find_one({"bot_id": bot_id, "user_id": user_id}, {"_id": 0})
    return {"success": True, "data": {"is_verified": bool(member and member.get("is_verified"))}}


@sio.on("list_members")
async def sio_list_members(sid, data):
    bot_id = data.get("botId")
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
    count = members_col.count_documents({"bot_id": bot_id, "is_verified": True})
    return {"success": True, "data": {"count": count}}


@sio.on("recover")
async def sio_recover(sid, data):
    bot_id = data.get("botId")
    members = list(members_col.find(
        {"bot_id": bot_id, "is_verified": True},
        {"_id": 0, "user_id": 1, "access_token": 1, "username": 1, "guild_id": 1}
    ))
    for m in members:
        m["id"] = m.pop("user_id", None)
    return {"success": True, "data": {"members": members}}


@sio.on("update_definitions")
async def sio_update_definitions(sid, data):
    return {"success": True, "message": "Definições recebidas"}


@sio.on("synchronization")
async def sio_synchronization(sid, data):
    bot_id = data.get("botId")
    count = members_col.count_documents({"bot_id": bot_id, "is_verified": True})
    return {"success": True, "data": {"verified_count": count}}


if __name__ == "__main__":
    uvicorn.run(socket_app, host="0.0.0.0", port=PORT)
