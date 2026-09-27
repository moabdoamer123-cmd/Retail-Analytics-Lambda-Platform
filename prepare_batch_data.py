import argparse
import os
import shutil
import subprocess

from retail_common import BATCH_RATIO, clean, load_raw, split_cutoff

HDFS_DIR = "/user/hadoop/raw_batch"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="online_retail_II.csv")
    ap.add_argument("--out", default="historical_orders_70.csv")
    args = ap.parse_args()

    print("⏳ Loading dataset...")
    raw = load_raw(args.file)
    cleaned = clean(raw)
    cutoff = split_cutoff(cleaned)

    batch_raw = raw[raw["InvoiceDate"] <= cutoff]                 
    batch_clean_rows = int((cleaned["InvoiceDate"] <= cutoff).sum())
    stream_rows = int((cleaned["InvoiceDate"] > cutoff).sum())

    batch_raw.to_csv(args.out, index=False)
    print(f"✂️  Split point (InvoiceDate): {cutoff}")
    print(f"✅ Saved {len(batch_raw):,} RAW rows to '{args.out}'  (first {BATCH_RATIO:.0%} of the timeline)")
    print(f"📌 After Spark cleaning you should get ~{batch_clean_rows:,} rows in Hive/ClickHouse (orders_batch)")
    print(f"📌 The Kafka producer will stream {stream_rows:,} rows (orders_realtime)")

    
    if shutil.which("hdfs"):
        print("🚀 Uploading to HDFS...")
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_DIR], check=True)
        subprocess.run(["hdfs", "dfs", "-put", "-f", args.out, HDFS_DIR + "/"], check=True)
        print(f"🎉 Uploaded to HDFS: {HDFS_DIR}/{os.path.basename(args.out)}")
    else:
        print("\nℹ️  'hdfs' command not found on this machine. Upload the file from the machine that runs HDFS:")
        print(f"   docker cp {args.out} namenode:/tmp/{args.out}")
        print(f"   docker exec namenode hdfs dfs -mkdir -p {HDFS_DIR}")
        print(f"   docker exec namenode hdfs dfs -put -f /tmp/{args.out} {HDFS_DIR}/")
        print("   (replace 'namenode' with the real HDFS namenode container name)")


if __name__ == "__main__":
    main()