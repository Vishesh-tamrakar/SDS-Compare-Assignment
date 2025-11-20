import os
import pandas as pd
import matplotlib.pyplot as plt

RESULTS_DIR = "results"


def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def plot_scalability():
    file_path = os.path.join(RESULTS_DIR, "scalability_results.csv")
    if not os.path.exists(file_path):
        print("scalability_results.csv not found")
        return

    df = pd.read_csv(file_path)

    plt.figure(figsize=(8, 5))
    plt.plot(df["p"], df["avg_latency_s"], marker="o")
    plt.xlabel("Number of Producers (p)")
    plt.ylabel("Average Latency (seconds)")
    plt.title("Scalability Plot")
    plt.grid(True)

    out = os.path.join(RESULTS_DIR, "plots_scalability.png")
    plt.savefig(out)
    plt.close()
    print(f"Saved {out}")


def plot_visualization(engine, mode):
    """
    engine = 'spark' or 'flink'
    mode = 'spp' or 'mmpp'
    """
    file_path = os.path.join(RESULTS_DIR, engine, f"{engine}_{mode}_visualization.csv")

    if not os.path.exists(file_path):
        print(f"No file {engine}_{mode}_visualization.csv -> skipping")
        return

    df = pd.read_csv(file_path)

    if "window_start_ms" not in df.columns or "measured_latency_ms" not in df.columns:
        print(f"Missing columns in {file_path}")
        return

    # Convert timestamps
    df["window_start_ms"] = pd.to_datetime(df["window_start_ms"], unit="ms")

    # Normalize time axis (assignment expects seconds)
    df["t"] = (df["window_start_ms"] - df["window_start_ms"].min()).dt.total_seconds()

    # Smooth latency with a rolling window (20 points)
    df["smoothed"] = df["measured_latency_ms"].rolling(window=20, min_periods=1).mean()

    plt.figure(figsize=(12, 5))
    plt.plot(df["t"], df["smoothed"], linewidth=2)
    plt.xlabel("Time (seconds)")
    plt.ylabel("Measured Latency (ms)")
    plt.title(f"{engine.upper()} – {mode.upper()} Latency Over Time (Smoothed)")
    plt.grid(True)

    # Ensure output directory exists
    out_dir = os.path.join(RESULTS_DIR, engine, "plots")
    ensure_dir(out_dir)

    out_path = os.path.join(out_dir, f"{engine}_{mode}_latency.png")
    plt.savefig(out_path)
    plt.close()

    print(f"Saved {out_path}")


if __name__ == "__main__":
    print("=== Generating Plots ===")

    plot_scalability()

    # Spark plots
    plot_visualization("spark", "spp")
    plot_visualization("spark", "mmpp")

    # Flink plots
    plot_visualization("flink", "spp")
    plot_visualization("flink", "mmpp")

    print("All plots completed.")
