"""CLI de ingesta. Dentro del contenedor: python -m lakehouse.ingestion <comando> (ver --help).

Comandos: synthetic (no necesita MinIO ni Spark), landing, bronze, genres y all (landing + bronze + genres).
Responsable: Ricardo Zeron del Valle (Ingeniero de Datos, Ingesta)
"""

from __future__ import annotations

import argparse
import sys

from lakehouse.common.config import get_settings
from lakehouse.common.errors import LakehouseError
from lakehouse.common.log import get_logger

log = get_logger("lakehouse.ingestion")


def _synthetic(args: argparse.Namespace) -> None:
    from lakehouse.ingestion.synthetic import generate_streams, write_files

    paths = write_files(generate_streams(args.rows, args.seed), args.out, args.files)
    log.info("Sinteticos: %d filas en %d archivos dentro de %s", args.rows, len(paths), args.out)


def _landing(args: argparse.Namespace) -> None:
    from lakehouse.common.storage import S3ObjectStore
    from lakehouse.ingestion.landing import upload_spotify_files

    settings = get_settings()
    result = upload_spotify_files(S3ObjectStore(settings), args.source_dir or settings.spotify_source_dir)
    log.info("Landing: subidos=%d, ya estaban=%d", len(result.uploaded), len(result.skipped))


def _with_spark(args: argparse.Namespace, steps: list[str]) -> None:
    from lakehouse.common.spark import build_spark
    from lakehouse.common.storage import S3ObjectStore
    from lakehouse.ingestion.bronze import BronzeTables, ingest_bronze, table_counts

    settings = get_settings()
    store, tables = S3ObjectStore(settings), BronzeTables.from_settings(settings)
    spark = build_spark("ingestion", settings)
    try:
        if "bronze" in steps:
            ingest_bronze(spark, store, settings, tables, force=args.force)
            counts = table_counts(spark, tables)
            log.info("Bronce total: %s", ", ".join(f"{k}={v}" for k, v in counts.items()))
        if "genres" in steps and args.genres != "skip":
            from lakehouse.ingestion.musicbrainz import enrich_genres
            from lakehouse.ingestion.synthetic import SyntheticSource

            source = SyntheticSource() if args.genres == "synthetic" else None
            enrich_genres(spark, store, settings, source, tables, limit=args.limit)
    finally:
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m lakehouse.ingestion", description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    syn = sub.add_parser("synthetic", help="Genera JSON sinteticos con el esquema de Spotify")
    syn.add_argument("--out", required=True, help="Carpeta de salida")
    syn.add_argument("--rows", type=int, default=5000)
    syn.add_argument("--seed", type=int, default=42)
    syn.add_argument("--files", type=int, default=2)

    land = sub.add_parser("landing", help="Sube los JSON a s3://landing/spotify/<fecha>/")
    land.add_argument("--source-dir", help="Carpeta con los JSON (default: SPOTIFY_SOURCE_DIR)")

    bronze = sub.add_parser("bronze", help="Landing a Bronce (Delta), idempotente")
    bronze.add_argument("--force", action="store_true", help="Reprocesa aunque ya este en la bitacora")

    genres = sub.add_parser("genres", help="Generos por artista (MusicBrainz o sinteticos)")
    genres.add_argument("--genres", choices=("api", "synthetic"), default="api", help="Fuente de generos")
    genres.add_argument("--limit", type=int, help="Maximo de artistas nuevos a consultar en esta corrida")

    full = sub.add_parser("all", help="landing + bronze + genres")
    full.add_argument("--source-dir")
    full.add_argument("--force", action="store_true")
    full.add_argument("--genres", choices=("api", "synthetic", "skip"), default="skip")
    full.add_argument("--limit", type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "synthetic":
            _synthetic(args)
        elif args.command == "landing":
            _landing(args)
        elif args.command == "bronze":
            args.genres = "skip"
            _with_spark(args, ["bronze"])
        elif args.command == "genres":
            args.force = False
            _with_spark(args, ["genres"])
        else:
            _landing(args)
            _with_spark(args, ["bronze", "genres"])
    except LakehouseError as exc:
        log.error("Ingesta FALLO: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
