"""Charts built from the small aggregated tables (pandas + matplotlib)."""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # render to files, no window needed
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

INK = "#1f2328"
MUTED = "#57606a"
GRID = "#d8dee4"
SERIES = "#2a78d6"
DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _style(ax, title: str, subtitle: str) -> None:
    ax.set_title(f"{title}\n", loc="left", fontsize=13, fontweight="bold", color=INK)
    ax.text(0, 1.02, subtitle, transform=ax.transAxes, fontsize=9.5, color=MUTED)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=9)


def demand_heatmap(df: pd.DataFrame, month: str, path: Path) -> None:
    grid = (
        df.pivot(index="pickup_weekday", columns="pickup_hour", values="avg_trips_per_day")
        .reindex(index=range(1, 8), columns=range(24))
    )
    fig, ax = plt.subplots(figsize=(11, 4.2))
    image = ax.imshow(grid.values, aspect="auto", cmap="Blues")
    ax.set_yticks(range(7), DAY_NAMES)
    ax.set_xticks(range(0, 24, 2), [f"{h:02d}:00" for h in range(0, 24, 2)])
    ax.set_xlabel("Pickup hour", color=MUTED, fontsize=9)
    _style(ax, "When people take taxis", f"Average trips per day by weekday and pickup hour, NYC yellow taxis, {month}")
    for side in ("left", "bottom"):
        ax.spines[side].set_visible(False)
    bar = fig.colorbar(image, ax=ax, fraction=0.025, pad=0.02)
    bar.set_label("Average trips per day", color=MUTED, fontsize=9)
    bar.ax.tick_params(colors=MUTED, labelsize=8)
    bar.outline.set_visible(False)
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)


def top_zones_bar(df: pd.DataFrame, month: str, path: Path, n: int = 10) -> None:
    top = df.head(n).iloc[::-1]
    labels = [f"{z} ({b})" for z, b in zip(top["zone"], top["borough"])]
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.barh(labels, top["trips"], color=SERIES, height=0.62)
    ax.xaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.set_xlabel("Trips", color=MUTED, fontsize=9)
    _style(ax, f"Top {n} pickup zones", f"Trips starting in each zone, {month}")
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)


def tips_line(df: pd.DataFrame, month: str, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(9, 4.2))
    ax.plot(df["pickup_hour"], df["median_tip_pct"], color=SERIES, linewidth=2, marker="o", markersize=4)
    ax.yaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_xticks(range(0, 24, 2), [f"{h:02d}:00" for h in range(0, 24, 2)])
    low = max(0, np.floor(df["median_tip_pct"].min() - 2))
    ax.set_ylim(low, np.ceil(df["median_tip_pct"].max() + 2))
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.set_xlabel("Pickup hour", color=MUTED, fontsize=9)
    _style(ax, "Median tip by hour", f"Tip as a share of the fare, credit-card trips only, {month}")
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)
