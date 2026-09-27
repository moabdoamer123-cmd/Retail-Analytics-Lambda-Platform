import re
import zlib

BATCH_RATIO = 0.70  

PAYMENT_METHODS = ["Credit Card", "PayPal", "Cash on Delivery", "Direct Bank Transfer"]

CATEGORY_KEYWORDS = {
    "Lighting":        {"LIGHT", "LIGHTS", "LAMP", "CANDLE", "CANDLES", "LANTERN"},
    "Kitchenware":     {"MUG", "CUP", "PLATE", "BOWL", "JAR", "TEAPOT", "TEA", "CAKE", "SPOON", "BAKING"},
    "Office Supplies": {"PEN", "PENS", "PENCIL", "NOTEBOOK", "RULER", "ERASER", "STICKER", "STICKERS"},
    "Gifts":           {"CARD", "CARDS", "WRAP", "GIFT", "BAG", "BOX", "TOY", "GAME"},
}
DEFAULT_CATEGORY = "Home Decor"


# Python / pandas side

def get_category(description: str) -> str:
    tokens = set(re.findall(r"[A-Z]+", description.upper()))
    for category, keywords in CATEGORY_KEYWORDS.items():
        if tokens & keywords:
            return category
    return DEFAULT_CATEGORY


def get_payment(invoice: str) -> str:

    return PAYMENT_METHODS[zlib.crc32(invoice.encode()) % len(PAYMENT_METHODS)]


def load_raw(path):
    import pandas as pd
    return pd.read_csv(path, dtype={"Invoice": str, "StockCode": str},
                       parse_dates=["InvoiceDate"])


def clean(df):
    """Same cleaning rules that the Spark batch job applies."""
    df = df.dropna(subset=["Description", "Customer ID"])
    df = df[df["Description"].astype(str).str.strip() != ""]
    df = df[~df["Invoice"].str.startswith("C")]
    df = df[(df["Quantity"] > 0) & (df["Price"] > 0)]        
    df = df[df["StockCode"].str.match(r"^\d{5}")]            
    return df.sort_values("InvoiceDate", kind="stable")


def split_cutoff(cleaned_df, ratio=BATCH_RATIO):
    ts = cleaned_df["InvoiceDate"].sort_values().reset_index(drop=True)
    return ts.iloc[max(int(len(ts) * ratio) - 1, 0)]



# Spark side (imports are lazy so the producer machine doesn't need pyspark)

def spark_category_col(desc_col):
    """Spark column expression equivalent to get_category()."""
    from pyspark.sql import functions as F
    upper = F.upper(desc_col)
    expr = F.lit(DEFAULT_CATEGORY)
    for category in reversed(list(CATEGORY_KEYWORDS)):        
        words = "|".join(sorted(CATEGORY_KEYWORDS[category]))
        pattern = "(?<![A-Z])(" + words + ")(?![A-Z])"        
        expr = F.when(upper.rlike(pattern), F.lit(category)).otherwise(expr)
    return expr


def spark_payment_col(order_id_col):
    from pyspark.sql import functions as F
    idx = F.pmod(F.crc32(order_id_col.cast("binary")), F.lit(len(PAYMENT_METHODS)))
    methods = F.array(*[F.lit(p) for p in PAYMENT_METHODS])
    return F.element_at(methods, (idx + 1).cast("int"))