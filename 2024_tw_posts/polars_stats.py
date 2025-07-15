import polars as pl

class TwitterPostsAnalyzer:
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path
        self.df = None
        self.numeric_columns = []
        self.text_columns = []

    def load_and_clean_data(self):
        print("╔" + "═" * 50 + "╗\n║" + " LOADING AND CLEANING DATA ".center(50) + "║\n╚" + "═" * 50 + "╝")
        self.df = pl.read_csv(self.csv_file_path, null_values=["", "na", "n/a", "none", "null", "nan", "#n/a", "#null!", "undefined"], ignore_errors=True)
        initial_rows = len(self.df)
        self.df = self.df.unique()
        self._identify_column_types()
        for col in self.numeric_columns:
            self.df = self.df.with_columns(pl.col(col).cast(pl.Float64, strict=False).alias(col))
        null_count = self.df.null_count().sum_horizontal().item()
        print(f"Dataset loaded: {len(self.df):,} rows × {len(self.df.columns)} columns\nDuplicates removed: {initial_rows - len(self.df)}\nNulls: {null_count}\nNumeric columns: {self.numeric_columns}\nText columns: {self.text_columns}")
        return self.df

    def _identify_column_types(self):
        self.numeric_columns, self.text_columns = [], []
        for col in self.df.columns:
            if self.df[col].dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
                self.numeric_columns.append(col)
            else:
                sample = self.df[col].drop_nulls().head(100)
                try:
                    numeric_ratio = sample.cast(pl.Float64, strict=False).drop_nulls().len() / len(sample) if len(sample) > 0 else 0
                    (self.numeric_columns if numeric_ratio > 0.8 else self.text_columns).append(col)
                except:
                    self.text_columns.append(col)

    def _format_table(self, df_result, max_width=20):
        if df_result.is_empty(): return "No data to display"
        headers, rows = df_result.columns, []
        for row in df_result.iter_rows():
            rows.append([item[:max_width-3] + "..." if isinstance(item, str) and len(item) > max_width else f"{item:.2f}" if isinstance(item, float) else str(item) for item in row])
        widths = [max(len(str(h)), max(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]
        sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
        return "\n".join([sep, "|" + "|".join(f" {h:<{widths[i]}} " for i, h in enumerate(headers)) + "|", sep] + 
                        ["|" + "|".join(f" {row[i]:<{widths[i]}} " for i in range(len(row))) + "|" for row in rows] + [sep])

    def compute_overall_statistics(self):
        print(f"\nOVERALL DATASET STATISTICS:\nRows: {len(self.df):,}, Columns: {len(self.df.columns)}")
        if self.numeric_columns:
            exprs = [pl.col(col).count().alias(f"{col}_count") for col in self.numeric_columns] + \
                   [pl.col(col).mean().alias(f"{col}_mean") for col in self.numeric_columns] + \
                   [pl.col(col).min().alias(f"{col}_min") for col in self.numeric_columns] + \
                   [pl.col(col).max().alias(f"{col}_max") for col in self.numeric_columns] + \
                   [pl.col(col).std().alias(f"{col}_std") for col in self.numeric_columns]
            stats = self.df.select(exprs)
            numeric_stats = [{"Column": col, "Count": int(stats.select(f"{col}_count").item() or 0), 
                            "Mean": round(stats.select(f"{col}_mean").item() or 0, 2), "Min": stats.select(f"{col}_min").item() or 0,
                            "Max": stats.select(f"{col}_max").item() or 0, "Std": round(stats.select(f"{col}_std").item() or 0, 2)} 
                           for col in self.numeric_columns]
            print("\nNumeric Column Statistics:")
            print(self._format_table(pl.DataFrame(numeric_stats)))
        
        if self.text_columns:
            text_stats = []
            for col in self.text_columns:
                vc = self.df[col].drop_nulls().value_counts().sort("count", descending=True)
                text_stats.append({"Column": col, "Count": self.df[col].drop_nulls().len(), "Unique": self.df[col].n_unique(),
                                 "Top": str(vc[0, col] if len(vc) > 0 else "N/A")[:20], "TopCount": vc[0, "count"] if len(vc) > 0 else 0})
            print("\nText Column Statistics:")
            print(self._format_table(pl.DataFrame(text_stats)))

    def group_and_print(self, by_cols, topn=10):
        grouped = self.df.group_by(by_cols).len().sort("len", descending=True).head(topn)
        total_groups = self.df.group_by(by_cols).len().height
        print(f"\nGROUP BY {by_cols}: {total_groups} total groups (showing top {topn})")
        print(self._format_table(grouped))
        return grouped

    def group_numeric_stats(self, group_cols, num_cols, topn_groups=10, topn_cols=3):
        for col in num_cols[:topn_cols]:
            if col in self.df.columns:
                stats = (self.df.group_by(group_cols).agg([pl.col(col).mean().alias("Mean"), pl.col(col).min().alias("Min"), 
                        pl.col(col).max().alias("Max"), pl.col(col).count().alias("Count")])
                        .filter(pl.col("Count") > 0).sort("Count", descending=True).head(topn_groups))
                if not stats.is_empty():
                    print(f"\n{col} statistics by {group_cols}:")
                    print(self._format_table(stats))

    def engagement_by_group(self, group_col, metrics):
        available_metrics = [m for m in metrics if m in self.df.columns]
        if available_metrics and group_col in self.df.columns:
            engagement_stats = (self.df.with_columns(pl.sum_horizontal([pl.col(m).fill_null(0) for m in available_metrics]).alias("total_engagement"))
                              .group_by(group_col).agg([pl.len().alias("Posts"), pl.col("total_engagement").sum().alias("TotalEng"), 
                                                       pl.col("total_engagement").mean().alias("AvgEng")])
                              .sort("TotalEng", descending=True).head(10))
            print(f"\nEngagement by {group_col}:")
            print(self._format_table(engagement_stats))

    def run_complete_analysis(self):
        import time
        total_start = time.time()
        print("\n=== POLARS TWITTER ANALYSIS START ===")
        step_start = time.time()
        self.load_and_clean_data()
        print(f"[TIMER] load_and_clean_data: {time.time() - step_start:.3f}s")
        step_start = time.time()
        self.compute_overall_statistics()
        print(f"[TIMER] compute_overall_statistics: {time.time() - step_start:.3f}s")
        if 'source' in self.df.columns:
            step_start = time.time()
            d = self.group_and_print(['source'])
            print(f"[TIMER] group_and_print ['source']: {time.time() - step_start:.3f}s")
            step_start = time.time()
            self.group_numeric_stats(['source'], self.numeric_columns)
            print(f"[TIMER] group_numeric_stats ['source']: {time.time() - step_start:.3f}s")
        if all(col in self.df.columns for col in ['id', 'source']):
            step_start = time.time()
            d = self.group_and_print(['id', 'source'])
            print(f"[TIMER] group_and_print ['id', 'source']: {time.time() - step_start:.3f}s")
            step_start = time.time()
            self.group_numeric_stats(['id', 'source'], self.numeric_columns)
            print(f"[TIMER] group_numeric_stats ['id', 'source']: {time.time() - step_start:.3f}s")
        if 'lang' in self.df.columns:
            step_start = time.time()
            d = self.group_and_print(['lang'])
            print(f"[TIMER] group_and_print ['lang']: {time.time() - step_start:.3f}s")
            step_start = time.time()
            self.group_numeric_stats(['lang'], self.numeric_columns)
            print(f"[TIMER] group_numeric_stats ['lang']: {time.time() - step_start:.3f}s")
        metrics = [c for c in ['retweetCount', 'replyCount', 'likeCount', 'quoteCount', 'bookmarkCount'] if c in self.df.columns]
        if metrics:
            if 'source' in self.df.columns:
                step_start = time.time()
                self.engagement_by_group('source', metrics)
                print(f"[TIMER] engagement_by_group 'source': {time.time() - step_start:.3f}s")
            if 'lang' in self.df.columns:
                step_start = time.time()
                self.engagement_by_group('lang', metrics)
                print(f"[TIMER] engagement_by_group 'lang': {time.time() - step_start:.3f}s")
        print(f"[TIMER] TOTAL: {time.time() - total_start:.3f}s\n=== POLARS TWITTER ANALYSIS END ===\n")

if __name__ == "__main__":
    TwitterPostsAnalyzer('../period_03/2024_tw_posts_president_scored_anon.csv').run_complete_analysis()