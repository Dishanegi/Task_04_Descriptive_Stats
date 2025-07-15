import csv, statistics
from collections import Counter, defaultdict

class TwitterPostsAnalyzer:
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path
        self.data, self.headers, self.numeric_columns, self.text_columns = [], [], [], []

    def load_and_clean_data(self):
        print("╔" + "═" * 50 + "╗\n║" + " LOADING AND CLEANING DATA ".center(50) + "║\n╚" + "═" * 50 + "╝")
        null_set = {"", "na", "n/a", "none", "null", "nan", "#n/a", "#null!", "undefined"}
        with open(self.csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            self.headers = reader.fieldnames
            # Advanced null mapping
            cleaned = []
            for row in reader:
                cleaned_row = {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
                for k, v in cleaned_row.items():
                    if isinstance(v, str) and v.strip().lower() in null_set:
                        cleaned_row[k] = None
                cleaned.append(cleaned_row)
            # Remove duplicates
            seen = set()
            unique = []
            for row in cleaned:
                row_tuple = tuple(row.get(h) for h in self.headers)
                if row_tuple not in seen:
                    seen.add(row_tuple)
                    unique.append(row)
            self.data = unique
        self._identify_column_types()
        # Data type conversion for numeric columns
        for row in self.data:
            for col in self.numeric_columns:
                v = row.get(col)
                if v is not None:
                    try:
                        row[col] = float(v)
                    except:
                        row[col] = None
        nulls = sum(1 for row in self.data for v in row.values() if v is None)
        print(f"Dataset loaded: {len(self.data):,} rows × {len(self.headers)} columns\nNulls: {nulls}\nNumeric: {self.numeric_columns}\nText: {self.text_columns}")
        return self.data

    def _identify_column_types(self):
        self.numeric_columns, self.text_columns = [], []
        for col in self.headers:
            vals = [row[col] for row in self.data[:100] if row[col]]
            try:
                if vals and sum(self._is_num(v) for v in vals)/len(vals) > 0.8:
                    self.numeric_columns.append(col)
                else:
                    self.text_columns.append(col)
            except: self.text_columns.append(col)
    def _is_num(self, v):
        try: float(v); return True
        except: return False
    def _get_num(self, v):
        try: return float(v)
        except: return None
    def _table(self, rows, headers):
        w = [max(len(str(x)) for x in [h]+[r[i] for r in rows]) for i, h in enumerate(headers)]
        s = lambda r: "|"+"|".join(f" {str(x):<{w[i]}} " for i, x in enumerate(r))+"|"
        sep = "+"+"+".join("-"*(x+2) for x in w)+"+"
        return '\n'.join([sep, s(headers), sep]+[s(r) for r in rows]+[sep])

    def compute_overall_statistics(self):
        print("\nOVERALL DATASET STATISTICS:")
        print(f"Rows: {len(self.data):,}, Columns: {len(self.headers)}")
        if self.numeric_columns:
            rows = []
            for col in self.numeric_columns:
                vals = [self._get_num(row[col]) for row in self.data if self._get_num(row[col]) is not None]
                if vals:
                    rows.append([col, len(vals), round(statistics.mean(vals),2), min(vals), max(vals), round(statistics.stdev(vals) if len(vals)>1 else 0,2)])
            print(self._table(rows, ['Col','Count','Mean','Min','Max','Std']))
        if self.text_columns:
            rows = []
            for col in self.text_columns:
                vals = [row[col] for row in self.data if row[col]]
                c = Counter(vals)
                if vals:
                    mf, mfc = c.most_common(1)[0]
                    rows.append([col, len(vals), len(set(vals)), mf[:20], mfc])
            print(self._table(rows, ['Col','Count','Unique','Top','TopCount']))

    def group_and_print(self, by_cols, topn=10):
        d = defaultdict(list)
        for row in self.data:
            key = tuple(row[c] for c in by_cols)
            d[key].append(row)
        print(f"\nGROUP BY {by_cols}: {len(d)} groups (top {topn})")
        rows = []
        for k, v in sorted(d.items(), key=lambda x: -len(x[1]))[:topn]:
            rows.append([*k, len(v)])
        print(self._table(rows, [*by_cols, 'Count']))
        return d

    def group_numeric_stats(self, d, num_cols, topn=3):
        for col in num_cols[:topn]:
            rows = []
            for k, v in list(d.items())[:10]:
                vals = [self._get_num(row[col]) for row in v if self._get_num(row[col]) is not None]
                if vals:
                    rows.append([*(k if isinstance(k, tuple) else (k,)), round(statistics.mean(vals),2), min(vals), max(vals)])
            if rows:
                key_headers = list(d.keys())[0] if isinstance(list(d.keys())[0], tuple) else [list(d.keys())[0]]
                print(f"\n{col} stats by group:")
                print(self._table(rows, [*key_headers, 'Mean','Min','Max']))

    def engagement_by_group(self, group_col, metrics):
        d = defaultdict(list)
        for row in self.data:
            d[row.get(group_col, 'N/A')].append(row)
        rows = []
        for k, v in sorted(d.items(), key=lambda x: -len(x[1]))[:10]:
            vals = [sum(self._get_num(row[m]) or 0 for m in metrics if m in row) for row in v]
            rows.append([k, len(v), sum(vals), round(sum(vals)/len(vals),1) if vals else 0])
        print(f"\nEngagement by {group_col}:")
        print(self._table(rows, [group_col, 'Posts', 'TotalEng', 'AvgEng']))

    def run_complete_analysis(self):
        import time
        total_start = time.time()
        print("\n=== PURE PYTHON TWITTER ANALYSIS START ===")
        step_start = time.time()
        self.load_and_clean_data()
        print(f"[TIMER] load_and_clean_data: {time.time() - step_start:.3f}s")
        step_start = time.time()
        self.compute_overall_statistics()
        print(f"[TIMER] compute_overall_statistics: {time.time() - step_start:.3f}s")
        # Group by source
        if 'source' in self.headers:
            step_start = time.time()
            d = self.group_and_print(['source'])
            print(f"[TIMER] group_and_print ['source']: {time.time() - step_start:.3f}s")
            step_start = time.time()
            self.group_numeric_stats(d, self.numeric_columns)
            print(f"[TIMER] group_numeric_stats ['source']: {time.time() - step_start:.3f}s")
        # Group by id+source
        if all(x in self.headers for x in ['id','source']):
            step_start = time.time()
            d = self.group_and_print(['id','source'])
            print(f"[TIMER] group_and_print ['id','source']: {time.time() - step_start:.3f}s")
            step_start = time.time()
            self.group_numeric_stats(d, self.numeric_columns)
            print(f"[TIMER] group_numeric_stats ['id','source']: {time.time() - step_start:.3f}s")
        # Group by lang
        if 'lang' in self.headers:
            step_start = time.time()
            d = self.group_and_print(['lang'])
            print(f"[TIMER] group_and_print ['lang']: {time.time() - step_start:.3f}s")
            step_start = time.time()
            self.group_numeric_stats(d, self.numeric_columns)
            print(f"[TIMER] group_numeric_stats ['lang']: {time.time() - step_start:.3f}s")
        # Engagement metrics
        metrics = [c for c in ['retweetCount','replyCount','likeCount','quoteCount','bookmarkCount'] if c in self.headers]
        if metrics:
            step_start = time.time()
            self.engagement_by_group('source', metrics)
            print(f"[TIMER] engagement_by_group 'source': {time.time() - step_start:.3f}s")
            step_start = time.time()
            self.engagement_by_group('lang', metrics)
            print(f"[TIMER] engagement_by_group 'lang': {time.time() - step_start:.3f}s")
        print(f"[TIMER] TOTAL: {time.time() - total_start:.3f}s\n=== PURE PYTHON TWITTER ANALYSIS END ===\n")

if __name__ == "__main__":
    TwitterPostsAnalyzer('../period_03/2024_tw_posts_president_scored_anon.csv').run_complete_analysis()