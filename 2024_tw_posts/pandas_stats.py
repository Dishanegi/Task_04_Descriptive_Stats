import pandas as pd

class TwitterPostsAnalyzer:
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path
        self.df = None
        self.numeric_columns = []
        self.text_columns = []

    def load_and_clean_data(self):
        print("╔═══════════════════════════════════════════════════╗\n║              LOADING AND CLEANING DATA            ║\n╚═══════════════════════════════════════════════════╝")
        na_values = ["", "na", "n/a", "none", "null", "nan", "#n/a", "#null!", "undefined"]
        self.df = pd.read_csv(self.csv_file_path, na_values=na_values, keep_default_na=True)
        initial_rows = len(self.df)
        self.df = self.df.drop_duplicates()
        self._identify_column_types()
        
        for col in self.numeric_columns:
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
        
        print(f"Dataset: {len(self.df):,} rows × {len(self.df.columns)} columns, Duplicates removed: {initial_rows - len(self.df)}, Nulls: {self.df.isnull().sum().sum()}")
        print(f"Numeric: {self.numeric_columns}\nText: {self.text_columns}")
        return self.df

    def _identify_column_types(self):
        self.numeric_columns, self.text_columns = [], []
        for col in self.df.columns:
            if self.df[col].dtype in ['int8', 'int16', 'int32', 'int64', 'float32', 'float64']:
                self.numeric_columns.append(col)
            else:
                sample = self.df[col].dropna().head(100)
                try:
                    numeric_ratio = pd.to_numeric(sample, errors='coerce').notna().sum() / len(sample) if len(sample) > 0 else 0
                    (self.numeric_columns if numeric_ratio > 0.8 else self.text_columns).append(col)
                except:
                    self.text_columns.append(col)

    def _format_table(self, df_result, max_width=20):
        if df_result.empty: return "No data to display"
        formatted_df = df_result.copy()
        for col in formatted_df.columns:
            if formatted_df[col].dtype == 'object':
                formatted_df[col] = formatted_df[col].astype(str).apply(lambda x: x[:max_width-3] + "..." if len(x) > max_width else x)
            elif formatted_df[col].dtype in ['float64', 'float32']:
                formatted_df[col] = formatted_df[col].apply(lambda x: f"{x:.2f}" if pd.notna(x) else str(x))
        
        headers, rows = list(formatted_df.columns), formatted_df.values.tolist()
        widths = [max(len(str(h)), max(len(str(r[i])) for r in rows) if rows else 0) for i, h in enumerate(headers)]
        sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
        header_row = "|" + "|".join(f" {h:<{widths[i]}} " for i, h in enumerate(headers)) + "|"
        data_rows = ["|" + "|".join(f" {str(row[i]):<{widths[i]}} " for i in range(len(row))) + "|" for row in rows]
        return "\n".join([sep, header_row, sep] + data_rows + [sep])

    def compute_overall_statistics(self):
        print(f"\nOVERALL DATASET STATISTICS:\nRows: {len(self.df):,}, Columns: {len(self.df.columns)}")
        
        if self.numeric_columns:
            numeric_stats = [{'Column': col, 'Count': int(self.df[col].count()), 'Mean': round(self.df[col].mean(), 2) if self.df[col].count() > 0 else 0,
                            'Min': self.df[col].min() if self.df[col].count() > 0 else 0, 'Max': self.df[col].max() if self.df[col].count() > 0 else 0,
                            'Std': round(self.df[col].std(), 2) if self.df[col].count() > 0 else 0} for col in self.numeric_columns]
            print("\nNumeric Column Statistics:")
            print(self._format_table(pd.DataFrame(numeric_stats)))
        
        if self.text_columns:
            text_stats = []
            for col in self.text_columns:
                col_data = self.df[col].dropna()
                vc = col_data.value_counts()
                text_stats.append({'Column': col, 'Count': len(col_data), 'Unique': col_data.nunique(),
                                 'Top': str(vc.index[0])[:20] if len(vc) > 0 else "N/A", 'TopCount': vc.iloc[0] if len(vc) > 0 else 0})
            print("\nText Column Statistics:")
            print(self._format_table(pd.DataFrame(text_stats)))

    def group_and_print(self, by_cols, topn=10):
        grouped = self.df.groupby(by_cols).size().reset_index(name='len').sort_values('len', ascending=False).head(topn)
        print(f"\nGROUP BY {by_cols}: {self.df.groupby(by_cols).ngroups} total groups (showing top {topn})")
        print(self._format_table(grouped))
        return grouped

    def group_numeric_stats(self, group_cols, num_cols, topn_groups=10, topn_cols=3):
        for col in num_cols[:topn_cols]:
            if col in self.df.columns:
                stats = self.df.groupby(group_cols)[col].agg(['mean', 'min', 'max', 'count']).reset_index()
                stats.columns = list(group_cols) + ['Mean', 'Min', 'Max', 'Count']
                stats = stats[stats['Count'] > 0].sort_values('Count', ascending=False).head(topn_groups)
                if not stats.empty:
                    print(f"\n{col} statistics by {group_cols}:")
                    print(self._format_table(stats))

    def engagement_by_group(self, group_col, metrics):
        available_metrics = [m for m in metrics if m in self.df.columns]
        if available_metrics and group_col in self.df.columns:
            df_eng = self.df.copy()
            for metric in available_metrics: df_eng[metric] = df_eng[metric].fillna(0)
            df_eng['total_engagement'] = df_eng[available_metrics].sum(axis=1)
            engagement_stats = df_eng.groupby(group_col)['total_engagement'].agg(['count', 'sum', 'mean']).reset_index()
            engagement_stats.columns = [group_col, 'Posts', 'TotalEng', 'AvgEng']
            print(f"\nEngagement by {group_col}:")
            print(self._format_table(engagement_stats.sort_values('TotalEng', ascending=False).head(10)))

    def run_complete_analysis(self):
        import time
        total_start = time.time()
        print("\n=== PANDAS TWITTER ANALYSIS START ===")
        step_start = time.time()
        self.load_and_clean_data()
        print(f"[TIMER] load_and_clean_data: {time.time() - step_start:.3f}s")
        step_start = time.time()
        self.compute_overall_statistics()
        print(f"[TIMER] compute_overall_statistics: {time.time() - step_start:.3f}s")
        
        # Fixed: Check if all columns in each col_set exist
        for col_set in [['source'], ['id', 'source'], ['lang']]:
            if all(col in self.df.columns for col in col_set):  # Fixed this line
                step_start = time.time()
                self.group_and_print(col_set)
                print(f"[TIMER] group_and_print {col_set}: {time.time() - step_start:.3f}s")
                step_start = time.time()
                self.group_numeric_stats(col_set, self.numeric_columns)
                print(f"[TIMER] group_numeric_stats {col_set}: {time.time() - step_start:.3f}s")
        
        metrics = [c for c in ['retweetCount', 'replyCount', 'likeCount', 'quoteCount', 'bookmarkCount'] if c in self.df.columns]
        if metrics:
            for group_col in ['source', 'lang']:
                if group_col in self.df.columns:
                    step_start = time.time()
                    self.engagement_by_group(group_col, metrics)
                    print(f"[TIMER] engagement_by_group {group_col}: {time.time() - step_start:.3f}s")
        print(f"[TIMER] TOTAL: {time.time() - total_start:.3f}s\n=== PANDAS TWITTER ANALYSIS END ===\n")

if __name__ == "__main__":
    TwitterPostsAnalyzer('../period_03/2024_tw_posts_president_scored_anon.csv').run_complete_analysis()