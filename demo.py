#!/usr/bin/env python3
"""
VisPipe Demo — watch an image travel through the full pipeline.

Run with: python demo.py
"""
import sys, os, time, json, io, hashlib
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "services/preprocessor"))

import cv2
import numpy as np
import psycopg2
from minio import Minio
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from pipeline import preprocess, encode_to_png, load_image, resize, to_grayscale, normalize

console = Console()

MINIO_ENDPOINT   = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
DATABASE_URL     = "postgresql://vispipe:vispipe@localhost:5432/vispipe"
SAMPLE_IMAGE     = "sample_images/capture_001.jpg"
COMPARISON_OUT   = "demo_comparison.png"


# ─── helpers ────────────────────────────────────────────────────────────────

def pause(msg="Press Enter to continue..."):
    if sys.stdin.isatty():
        console.print(f"\n[dim]{msg}[/dim]")
        input()
    else:
        console.print(f"\n[dim]{msg} (auto-continuing)[/dim]")
        time.sleep(1)

def minio_client():
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                 secret_key=MINIO_SECRET_KEY, secure=False)

def db_connect():
    return psycopg2.connect(DATABASE_URL)


# ─── Step renderers ─────────────────────────────────────────────────────────

def step_banner(n, title, subtitle=""):
    console.print()
    console.print(Panel(
        f"[bold white]{title}[/bold white]\n[dim]{subtitle}[/dim]" if subtitle else f"[bold white]{title}[/bold white]",
        title=f"[cyan]Step {n}[/cyan]",
        border_style="cyan",
        padding=(0, 2),
    ))


def show_raw_image(image_bytes: bytes):
    arr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    h, w, c = img.shape

    t = Table(box=box.ROUNDED, show_header=True, header_style="bold magenta")
    t.add_column("Property", style="bold")
    t.add_column("Value", style="green")
    t.add_row("Dimensions",    f"{w} × {h} pixels")
    t.add_row("Channels",      f"{c}  (B, G, R — OpenCV order)")
    t.add_row("Dtype",         str(img.dtype))
    t.add_row("Pixel range",   f"0 – 255  (raw uint8)")
    t.add_row("File size",     f"{len(image_bytes):,} bytes  ({len(image_bytes)//1024} KB)")
    t.add_row("Format",        "JPEG")

    # Show a sample pixel
    mid_y, mid_x = h // 2, w // 2
    b, g, r = img[mid_y, mid_x]
    t.add_row("Centre pixel",  f"B={b}  G={g}  R={r}  (raw 0-255 values)")
    t.add_row("AI-ready?",     "[red]✗  Too large, wrong format, unnormalized[/red]")

    console.print(t)


def show_preprocessing_steps(image_bytes: bytes):
    arr = np.frombuffer(image_bytes, np.uint8)
    raw = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    resized = resize(raw)
    grayscale = to_grayscale(resized)
    normalized = normalize(grayscale)

    steps = [
        ("1. Load",      raw,        f"shape={raw.shape}  dtype={raw.dtype}  range=[0, 255]"),
        ("2. Resize",    resized,    f"shape={resized.shape}  (224×224)"),
        ("3. Grayscale", grayscale,  f"shape={grayscale.shape}  channels: 3 → 1"),
        ("4. Normalize", normalized, f"dtype=float32  range=[{normalized.min():.3f}, {normalized.max():.3f}]"),
    ]

    t = Table(box=box.ROUNDED, show_header=True, header_style="bold magenta")
    t.add_column("Stage",  style="bold cyan", width=14)
    t.add_column("Result", style="green")
    t.add_column("Memory", style="yellow", justify="right")

    for name, arr_, info in steps:
        mem = arr_.nbytes
        t.add_row(name, info, f"{mem:,} B")

    console.print(t)

    reduction = (1 - (normalized.nbytes / raw.nbytes)) * 100
    console.print(f"\n  [bold]Memory reduction:[/bold] {raw.nbytes:,} B  →  {normalized.nbytes:,} B  "
                  f"([green]{reduction:.0f}% smaller[/green])\n")


def save_comparison_image(image_bytes: bytes):
    arr   = np.frombuffer(image_bytes, np.uint8)
    raw   = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    raw_r = cv2.cvtColor(raw, cv2.COLOR_BGR2RGB)

    processed = preprocess(image_bytes)

    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    fig.patch.set_facecolor("#1a1a2e")

    axes[0].imshow(raw_r)
    axes[0].set_title(f"BEFORE  ({raw.shape[1]}×{raw.shape[0]} px, BGR, uint8, 0–255)",
                      color="white", fontsize=9, pad=8)
    axes[0].axis("off")

    axes[1].imshow(processed, cmap="gray", vmin=0, vmax=1)
    axes[1].set_title(f"AFTER  (224×224 px, grayscale, float32, 0.0–1.0)",
                      color="white", fontsize=9, pad=8)
    axes[1].axis("off")

    for ax in axes:
        for spine in ax.spines.values():
            spine.set_edgecolor("#555")

    plt.suptitle("OpenCV Preprocessing Pipeline", color="white", fontsize=11, y=1.01)
    plt.tight_layout()
    plt.savefig(COMPARISON_OUT, dpi=130, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close()


def upload_to_bronze(m, image_bytes: bytes, filename: str) -> str:
    if not m.bucket_exists("bronze"):
        m.make_bucket("bronze")
    m.put_object("bronze", filename,
                 io.BytesIO(image_bytes), len(image_bytes),
                 content_type="image/jpeg")
    return f"bronze/{filename}"


def snapshot_silver(m) -> set:
    """Snapshot current silver objects before upload so we can detect new ones."""
    try:
        return {o.object_name for o in m.list_objects("silver")}
    except Exception:
        return set()


def poll_silver(m, before: set, timeout=45):
    """Wait until a new .png appears in silver that wasn't there before upload."""
    import time as _time
    start = _time.time()
    console.print("\n  [dim]Waiting for preprocessor to process the image...[/dim]")

    while _time.time() - start < timeout:
        try:
            current = {o.object_name for o in m.list_objects("silver")}
        except Exception:
            current = set()
        new = current - before
        if new:
            return new.pop()
        console.print("  [dim]  · still waiting...[/dim]")
        _time.sleep(2)
    return None


def show_silver_image(m, silver_name: str):
    response = m.get_object("silver", silver_name)
    try:
        data = response.read()
    finally:
        response.close()
        response.release_conn()

    arr  = np.frombuffer(data, np.uint8)
    img  = cv2.imdecode(arr, cv2.IMREAD_GRAYSCALE)
    h, w = img.shape

    t = Table(box=box.ROUNDED, show_header=True, header_style="bold magenta")
    t.add_column("Property", style="bold")
    t.add_column("Value",    style="green")
    t.add_row("Dimensions",   f"{w} × {h} pixels")
    t.add_row("Channels",     "1  (grayscale)")
    t.add_row("Dtype",        "uint8  (stored as PNG, was float32 during processing)")
    t.add_row("Pixel range",  "0 – 255  (re-encoded for storage)")
    t.add_row("File size",    f"{len(data):,} bytes  ({len(data)//1024} KB)")
    t.add_row("Format",       "PNG  (lossless)")
    t.add_row("MD5 checksum", hashlib.md5(data).hexdigest())
    t.add_row("AI-ready?",    "[green]✓  Standard size, single channel, clean[/green]")
    console.print(t)


def snapshot_gold_count() -> int:
    """Count existing processed rows before upload."""
    try:
        db = db_connect()
        with db.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM images WHERE status = 'processed'")
            count = cur.fetchone()[0]
        db.close()
        return count
    except Exception:
        return 0


def poll_gold(before_count: int, timeout=30):
    """Poll PostgreSQL until a new processed row appears (count increases)."""
    import time as _time
    start = _time.time()
    console.print("\n  [dim]Waiting for metadata extractor to write Gold record...[/dim]")
    db = db_connect()
    while _time.time() - start < timeout:
        with db.cursor() as cur:
            cur.execute(
                "SELECT image_id, status, width, height, channels, file_size_bytes, checksum, processed_at "
                "FROM images WHERE status = 'processed' ORDER BY processed_at DESC LIMIT 1"
            )
            row = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM images WHERE status = 'processed'")
            count = cur.fetchone()[0]
            if count > before_count and row:
                db.close()
                return row
        console.print("  [dim]  · still waiting...[/dim]")
        _time.sleep(2)
    db.close()
    return None


def show_gold_record(row):
    image_id, status, w, h, ch, size, checksum, processed_at = row

    t = Table(box=box.ROUNDED, show_header=True, header_style="bold magenta")
    t.add_column("Column",   style="bold")
    t.add_column("Value",    style="green")
    t.add_row("image_id",        str(image_id))
    t.add_row("status",          f"[green]{status}[/green]")
    t.add_row("width × height",  f"{w} × {h}")
    t.add_row("channels",        str(ch))
    t.add_row("file_size_bytes", f"{size:,}")
    t.add_row("checksum (MD5)",  checksum)
    t.add_row("processed_at",    str(processed_at))
    console.print(t)


# ─── Main demo ──────────────────────────────────────────────────────────────

def main():
    console.print(Panel.fit(
        "[bold white]VisPipe — Live Pipeline Demo[/bold white]\n"
        "[dim]Watch an image travel: Bronze → Silver → Gold[/dim]",
        border_style="bright_blue",
        padding=(1, 4),
    ))
    pause()

    # ── Step 1: Raw image ────────────────────────────────────────────────────
    step_banner(1, "The Raw Image",
                "This is the raw input — large, noisy, 3-channel, not normalized.")

    with open(SAMPLE_IMAGE, "rb") as f:
        raw_bytes = f.read()

    show_raw_image(raw_bytes)
    pause()

    # ── Step 2: Preprocessing ───────────────────────────────────────────────
    step_banner(2, "OpenCV Preprocessing",
                "Four transformations turn the raw image into an AI-ready matrix.")

    show_preprocessing_steps(raw_bytes)

    console.print("  Saving a before/after visual comparison...")
    save_comparison_image(raw_bytes)
    console.print(f"  [green]✓[/green]  Saved to [bold]{COMPARISON_OUT}[/bold] — open it to see the visual difference.\n")
    pause()

    # ── Step 3: Upload to Bronze ────────────────────────────────────────────
    step_banner(3, "Uploading to Bronze (MinIO)",
                "Dropping the raw image into the Bronze bucket — this triggers the pipeline.")

    m = minio_client()
    before_silver = snapshot_silver(m)           # snapshot BEFORE upload
    before_gold   = snapshot_gold_count()        # snapshot BEFORE upload
    filename = f"demo_{int(time.time())}.jpg"
    path = upload_to_bronze(m, raw_bytes, filename)

    console.print(f"\n  [green]✓[/green]  Uploaded [bold]{filename}[/bold] to MinIO bucket [bold]bronze/[/bold]")
    console.print(f"  [dim]Full path: {path}[/dim]")
    console.print(f"\n  [dim]The Ingestor is polling the bronze bucket every 5 seconds.[/dim]")
    console.print(f"  [dim]It will detect this new file and publish a message to the[/dim]")
    console.print(f"  [dim][bold]raw_images[/bold] RabbitMQ queue...[/dim]")
    pause()

    # ── Step 4: Silver ──────────────────────────────────────────────────────
    step_banner(4, "Preprocessor → Silver (MinIO)",
                "The preprocessor consumed the queue message, ran the OpenCV pipeline, and saved to silver/.")

    silver_name = poll_silver(m, before=before_silver, timeout=45)
    if not silver_name:
        console.print("[red]Timed out waiting for silver image. Is Docker running?[/red]")
        sys.exit(1)

    console.print(f"\n  [green]✓[/green]  Processed image appeared in [bold]silver/{silver_name}[/bold]\n")
    show_silver_image(m, silver_name)
    pause()

    # ── Step 5: Gold ────────────────────────────────────────────────────────
    step_banner(5, "Metadata Extractor → Gold (PostgreSQL)",
                "The metadata extractor wrote a structured record to the images table.")

    row = poll_gold(before_count=before_gold, timeout=30)
    if not row:
        console.print("[red]Timed out waiting for Gold record.[/red]")
        sys.exit(1)

    console.print(f"\n  [green]✓[/green]  Row written to PostgreSQL [bold]images[/bold] table:\n")
    show_gold_record(row)
    pause()

    # ── Step 6: Summary ─────────────────────────────────────────────────────
    step_banner(6, "Pipeline Complete", "Here's the full journey.")

    summary = Table(box=box.ROUNDED, show_header=True, header_style="bold white")
    summary.add_column("Layer",    style="bold cyan")
    summary.add_column("Where",    style="yellow")
    summary.add_column("What",     style="green")
    summary.add_column("Service",  style="dim")

    summary.add_row("Bronze", "MinIO bronze/",    f"{filename}  (raw JPEG, {len(raw_bytes)//1024} KB)", "ingestor")
    summary.add_row("Silver", "MinIO silver/",    f"{silver_name}  (224×224 grayscale PNG)", "preprocessor")
    summary.add_row("Gold",   "PostgreSQL images", "metadata row — checksum, dims, status, timestamp", "metadata_extractor")

    console.print(summary)
    console.print(f"\n  [bold green]✓  Open [yellow]{COMPARISON_OUT}[/yellow] to see before vs after visually.[/bold green]\n")


if __name__ == "__main__":
    main()
