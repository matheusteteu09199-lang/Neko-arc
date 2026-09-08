"""Toca um som quando alguém entra em canal de voz e fica na call."""

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

log = logging.getLogger("neko.greeter")


def _load_opus() -> None:
    """Carrega libopus (Termux às vezes não acha sozinho)."""
    if discord_opus.is_loaded():
        return
    for name in ("libopus.so", "libopus.so.0", "libopus.so.1", "libopus.so.8"):
        try:
            discord_opus.load_opus(name)
            if discord_opus.is_loaded():
                log.info("libopus via %s", name)
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
        log.error("libopus não encontrado! pkg install libopus")


class GreeterClient(discord.Client):
    def __init__(self, *, sound_path: Path = SOUND_PATH) -> None:
        intents = discord.Intents.default()
        intents.voice_states = True
        intents.members = True
        super().__init__(intents=intents)
        self.sound_path = sound_path
        self._locks: dict[int, asyncio.Lock] = {}

    def _lock_for(self, guild_id: int) -> asyncio.Lock:
        return self._locks.setdefault(guild_id, asyncio.Lock())

    async def on_ready(self) -> None:
        assert self.user is not None
        log.info("Conectado como %s", self.user)

    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ) -> None:
        if member.bot:
            return

        # Saiu: se a call ficou vazia de humanos, bot desconecta.
        if before.channel and not after.channel:
            await self._maybe_disconnect(before.channel)
            return

        # Entrou (de lugar nenhum): conecta e toca som.
        if not before.channel and after.channel:
            await self._announce(after.channel)
            return

    async def _announce(self, channel: discord.abc.VoiceChannel) -> None:
        guild = channel.guild
        async with self._lock_for(guild.id):
            vc = guild.voice_client
            try:
                if vc is None or not vc.is_connected():
                    if vc is not None:
                        await self._drop(guild, vc)
                    vc = await channel.connect(
                        timeout=15.0, reconnect=False, self_deaf=False
                    )
                elif vc.channel != channel:
                    await vc.move_to(channel)
            except Exception:
                log.exception("Falha ao conectar em #%s", channel.name)
                return

            # Dá tempo do Discord abrir a sessão UDP antes de mandar áudio.
            await asyncio.sleep(0.4)
            await self._play(vc)

    async def _play(self, vc: discord.VoiceClient) -> None:
        if vc.is_playing():
            vc.stop()
        done = asyncio.Event()
        loop = asyncio.get_running_loop()

        def _after(err: Exception | None) -> None:
            if err:
                log.error("Erro na reprodução: %r", err)
            try:
                loop.call_soon_threadsafe(done.set)
            except RuntimeError:
                pass

        try:
            # apad estende o áudio com silêncio: evita o corte do Discord
            # quando o som é muito curto e o fim chega antes do stream estabilizar.
            source = discord.FFmpegPCMAudio(
                str(self.sound_path),
                before_options="-loglevel warning",
                options="-af apad=pad_dur=1.5",
            )
            vc.play(source, after=_after)
        except Exception:
            log.exception("Falha ao iniciar reprodução")
            return

        try:
            await asyncio.wait_for(done.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            if vc.is_playing():
                vc.stop()

    async def _maybe_disconnect(self, channel: discord.abc.VoiceChannel) -> None:
        guild = channel.guild
        async with self._lock_for(guild.id):
            vc = guild.voice_client
            if vc is None or vc.channel != channel:
                return
            # Recarrega a lista (a voz do membro que saiu já foi removida).
            humans = [m for m in channel.members if not m.bot]
            if humans:
                return
            log.info("Guild %s: call vazia, saindo.", guild.id)
            await self._drop(guild, vc)

    async def _drop(self, guild: discord.Guild, vc: discord.VoiceClient) -> None:
        try:
            await vc.disconnect(force=True)
        except Exception:
            log.warning("disconnect falhou (guild %s)", guild.id, exc_info=True)
        try:
            vc.cleanup()
        except Exception:
            log.warning("cleanup falhou (guild %s)", guild.id, exc_info=True)
        remover = getattr(self, "_remove_voice_client", None)
        if remover is not None:
            try:
                remover(guild.id)
            except Exception:
                log.warning("remove_voice_client falhou", exc_info=True)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    logging.getLogger("discord.gateway").setLevel(logging.WARNING)

    load_dotenv(BASE_DIR / ".env")
    token = os.getenv("TOKEN") or os.getenv("DISCORD_TOKEN")
    if not token:
        sys.exit("Erro: defina TOKEN no .env")
    if not SOUND_PATH.exists():
        sys.exit(f"Erro: {SOUND_PATH} não encontrado")

    _load_opus()
    client = GreeterClient(sound_path=SOUND_PATH)
    try:
        client.run(token, log_handler=None)
    except discord.LoginFailure:
        sys.exit("Erro: token inválido")


if __name__ == "__main__":
    main()
