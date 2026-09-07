"""Toca um som quando alguém entra em canal de voz."""

from __future__ import annotations

import asyncio
import ctypes.util
import logging
import os
import sys
from pathlib import Path

import discord
from discord import opus as discord_opus
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
SOUND_PATH = BASE_DIR / "src" / "sound" / "burenya.mp3"
EXIT_DELAY_SECONDS = 2.0

log = logging.getLogger("neko.greeter")


def _load_opus() -> None:
    """Carrega libopus (Termux às vezes não acha sozinho)."""
    if discord_opus.is_loaded():
        return
    candidates = ["libopus.so", "libopus.so.0", "libopus.so.1", "libopus.so.8"]
    for name in candidates:
        try:
            discord_opus.load_opus(name)
            if discord_opus.is_loaded():
                log.info("libopus carregado via %s", name)
                return
        except Exception:
            continue
    found = ctypes.util.find_library("opus")
    if found:
        try:
            discord_opus.load_opus(found)
        except Exception:
            pass
    if not discord_opus.is_loaded():
        log.error(
            "libopus não encontrado! Instale com: pkg install libopus"
        )


class _GuildState:
    """Estado por guild: fila de reproduções e worker."""

    __slots__ = ("lock", "pending", "worker", "channel")

    def __init__(self) -> None:
        self.lock: asyncio.Lock = asyncio.Lock()
        self.pending: int = 0
        self.worker: asyncio.Task | None = None
        self.channel: discord.abc.VoiceChannel | discord.StageChannel | None = None


class JoinAnnouncer:
    """Toca o som de entrada e controla a conexão de voz."""

    def __init__(
        self,
        client: discord.Client,
        *,
        sound_path: Path = SOUND_PATH,
        exit_delay: float = EXIT_DELAY_SECONDS,
    ) -> None:
        self.client = client
        self.sound_path = sound_path
        self.exit_delay = exit_delay
        self._states: dict[int, _GuildState] = {}

    async def handle_join(
        self, channel: discord.abc.VoiceChannel | discord.StageChannel
    ) -> None:
        """Membro entrou em canal de voz."""
        guild = channel.guild
        state = self._states.setdefault(guild.id, _GuildState())

        async with state.lock:
            state.pending += 1
            await self._ensure_connected(state, channel)
            if state.worker is None or state.worker.done():
                state.worker = asyncio.create_task(
                    self._run(guild.id), name=f"greeter-{guild.id}"
                )

    async def _ensure_connected(
        self, state: _GuildState, channel: discord.abc.VoiceChannel | discord.StageChannel
    ) -> None:
        """Conecta/move o bot. Tenta de novo se o Discord invalidar a sessão."""
        guild = channel.guild
        vc = guild.voice_client
        if vc is not None and not vc.is_connected():
            await self._drop_voice_client(guild, vc)
            vc = None

        for attempt in range(2):
            try:
                if vc is None:
                    await channel.connect(
                        timeout=15.0, reconnect=False, self_deaf=False
                    )
                elif vc.channel != channel:
                    await vc.move_to(channel)
            except (discord.ClientException, discord.ConnectionClosed, asyncio.TimeoutError):
                if attempt == 1:
                    raise
                log.warning("Guild %s: falha na conexão; retentando.", guild.id)
                stale = guild.voice_client
                if stale is not None:
                    await self._drop_voice_client(guild, stale)
                await asyncio.sleep(1.0)
                vc = None
                continue
            break

        vc = guild.voice_client
        if vc is None or not vc.is_connected():
            raise RuntimeError(
                f"Voz não conectou no canal #{channel.name} (guild {guild.id})"
            )
        state.channel = channel

    async def _drop_voice_client(
        self,
        guild: discord.Guild,
        vc: discord.VoiceClient | discord.VoiceProtocol,
    ) -> None:
        """Desconecta e limpa o voice client por completo."""
        try:
            await vc.disconnect(force=True)
        except Exception:
            log.warning("Falha no disconnect (guild %s)", guild.id, exc_info=True)
        try:
            vc.cleanup()
        except Exception:
            log.warning("Falha no cleanup (guild %s)", guild.id, exc_info=True)
        remover = getattr(self.client, "_remove_voice_client", None)
        if remover is not None:
            try:
                remover(guild.id)
            except Exception:
                log.warning(
                    "Falha ao remover voice client do registry (guild %s)",
                    guild.id,
                    exc_info=True,
                )

    async def _run(self, guild_id: int) -> None:
        """Consome a fila e desconecta quando silenciar."""
        state = self._states[guild_id]
        try:
            while True:
                async with state.lock:
                    plays = state.pending
                    state.pending = 0
                if plays:
                    log.debug("Guild %s: %d reprodução(ões) na fila.", guild_id, plays)

                for _ in range(plays):
                    await self._play_once(state)

                await asyncio.sleep(self.exit_delay)

                async with state.lock:
                    if state.pending:
                        continue  # chegou gente durante o aguardo/reprodução

                    guild = self.client.get_guild(guild_id)
                    vc = guild.voice_client if guild is not None else None
                    state.channel = None
                    state.worker = None
                    if guild is not None and vc is not None:
                        try:
                            await self._drop_voice_client(guild, vc)
                        except Exception:
                            log.exception("Erro ao desconectar (guild %s)", guild_id)
                    log.info("Guild %s: sem entradas, desconectando.", guild_id)
                    return
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Worker do guild %s morreu inesperadamente", guild_id)
            state.worker = None

    async def _play_once(
        self, state: _GuildState
    ) -> None:
        """Toca o som uma vez e espera terminar."""
        if state.channel is None:
            log.warning("Pulando reprodução: sem canal associado.")
            return
        vc = state.channel.guild.voice_client
        if vc is None or not vc.is_connected():
            log.warning(
                "Pulando reprodução: voice client %s em #%s.",
                "ausente" if vc is None else "desconectado",
                state.channel.name,
            )
            return

        loop = asyncio.get_running_loop()
        done = asyncio.Event()

        def _after(error: Exception | None) -> None:
            if error is not None:
                log.error("Erro na reprodução: %r", error)
            try:
                loop.call_soon_threadsafe(done.set)
            except RuntimeError:
                pass

        try:
            if vc.is_playing():
                vc.stop()
            source = discord.FFmpegPCMAudio(
                str(self.sound_path),
                before_options="-loglevel warning",
            )
            vc.play(source, after=_after)
        except discord.opus.OpusNotLoaded:
            log.error("Opus não carregado — instale libopus (pkg install libopus).")
            return
        except Exception:
            log.exception("Falha ao iniciar reprodução")
            return

        try:
            await asyncio.wait_for(done.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            log.warning("Reprodução >30s; parando.")
            try:
                if vc.is_playing():
                    vc.stop()
            except Exception:
                pass

    def cancel_guild(self, guild_id: int) -> None:
        state = self._states.get(guild_id)
        if state is not None and state.worker is not None:
            state.worker.cancel()

    def cancel_all(self) -> None:
        for state in self._states.values():
            if state.worker is not None:
                state.worker.cancel()
        self._states.clear()


class GreeterClient(discord.Client):
    def __init__(self, *, sound_path: Path = SOUND_PATH) -> None:
        intents = discord.Intents.default()
        intents.voice_states = True
        super().__init__(intents=intents)
        self.announcer = JoinAnnouncer(self, sound_path=sound_path)

    async def on_ready(self) -> None:
        assert self.user is not None
        log.info("Conectado como %s (id=%s)", self.user, self.user.id)

        perms = discord.Permissions(connect=True, speak=True, view_channel=True)
        invite = discord.utils.oauth_url(
            self.user.id, permissions=perms, scopes=("bot",)
        )
        log.info("Convite com permissões de voz: %s", invite)

    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ) -> None:
        # Só "entrou agora": antes sem canal, depois em um canal.
        if before.channel is not None or after.channel is None:
            return
        if member.bot:
            return

        log.info(
            "%s entrou em #%s — agendando som de entrada.",
            getattr(member, "display_name", None) or member,
            after.channel,
        )
        try:
            await self.announcer.handle_join(after.channel)
        except Exception:
            log.exception("Erro ao tratar entrada de %s", member)

    async def on_guild_remove(self, guild: discord.Guild) -> None:
        self.announcer.cancel_guild(guild.id)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    logging.getLogger("discord.gateway").setLevel(logging.WARNING)

    load_dotenv(BASE_DIR / ".env")
    token = os.getenv("TOKEN") or os.getenv("DISCORD_TOKEN")
    if not token:
        sys.exit("Erro: defina TOKEN no arquivo .env")

    if not SOUND_PATH.exists():
        sys.exit(f"Erro: arquivo de som não encontrado: {SOUND_PATH}")

    _load_opus()

    client = GreeterClient(sound_path=SOUND_PATH)

    try:
        client.run(token, log_handler=None)
    except discord.LoginFailure:
        sys.exit("Erro: token inválido (LoginFailure). Verifique o TOKEN no .env")


if __name__ == "__main__":
    main()
