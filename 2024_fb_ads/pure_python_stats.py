import csv
import time
import json
from collections import Counter, defaultdict

class PythonAnalyzer:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.data, self.headers, self.numeric_columns, self.categorical_columns = [], [], [], []
    
    def _table(self, headers, data, borders=True):
        if not data: return
        w = [max(len(str(headers[i])), max(len(str(r[i])) for r in data)) + 2 for i in range(len(headers))]
        if borders:
            b = '+' + '+'.join('-' * x for x in w) + '+'
            print(f"{b}\n|{'|'.join(f' {str(headers[i]).ljust(w[i]-1)}' for i in range(len(headers)))}|")
            [print('|' + '|'.join(f' {str(r[i]).ljust(w[i]-1)}' for i in range(len(r))) + '|') for r in data]
            print(b)
        else:
            print('  '.join(str(headers[i]).ljust(w[i]) for i in range(len(headers))))
            [print('  '.join(str(r[i]).ljust(w[i]) for i in range(len(r)))) for r in data]

    def _header(self, title, level=1):
        print(f"\n{'='*70 if level==1 else '-'*50}\n{title}\n{'='*70 if level==1 else '-'*50}")

    def _fmt(self, v, dp=2):
        return 'None' if v is None else f"{int(v):,}" if isinstance(v, (int, float)) and v == int(v) else f"{v:,.{dp}f}" if isinstance(v, (int, float)) else str(v)

    def _box(self, title, stats):
        lw, vw = max(len(l) for l, _ in stats), max(len(str(v)) for _, v in stats)
        print(f"\n┌─{title}─{'─' * (lw + vw + 5)}┐")
        [print(f"│ {l.ljust(lw)} : {(self._fmt(v) if isinstance(v, (int, float)) else str(v)).rjust(vw)} │") for l, v in stats]
        print(f"└{'─' * (lw + vw + 10)}┘")

    def _parse_json_safe(self, value):
        """Fast JSON parsing with fallback"""
        if not value or value in ('{}', '[]', ''): return None
        if not ('{' in value or '[' in value): return value
        try: return json.loads(value)
        except: return value
        
    def load_and_clean_data(self):
        self._header("STEP 1: LOADING AND CLEANING DATASET")
        start = time.time()
        
        try:
            with open(self.filepath, 'r', encoding='utf-8') as f:
                # Auto-detect delimiter from first line
                first_line = f.readline()
                delim = '\t' if first_line.count('\t') > first_line.count(',') else ','
                f.seek(0)
                
                reader = csv.reader(f, delimiter=delim)
                self.headers = [h.strip() for h in next(reader)]
                
                # Fast data loading with minimal processing
                raw_data = []
                complex_fields = {'delivery_by_region', 'demographic_distribution', 'publisher_platforms'}
                
                for row_num, row in enumerate(reader):
                    if row_num % 10000 == 0 and row_num > 0:
                        print(f"  Loaded {row_num:,} rows...")
                    
                    # Ensure row has correct length
                    while len(row) < len(self.headers): row.append('')
                    row = row[:len(self.headers)]
                    
                    # Process row with minimal parsing
                    processed_row = []
                    for i, val in enumerate(row):
                        if not val or val.strip() in {'', 'null', 'NULL', 'None', 'N/A', 'NA'}:
                            processed_row.append(None)
                        elif self.headers[i] in complex_fields:
                            processed_row.append(self._parse_json_safe(val))
                        else:
                            processed_row.append(val.strip())
                    
                    raw_data.append(processed_row)
            
            print(f"Loaded {len(raw_data):,} rows in {time.time()-start:.1f}s")
            
            # Fast type detection - sample-based for large datasets
            sample_size = min(1000, len(raw_data))
            sample_data = raw_data[:sample_size] if len(raw_data) > 1000 else raw_data
            
            for i, header in enumerate(self.headers):
                if header in complex_fields:
                    self.categorical_columns.append(header)
                    continue
                
                # Check numeric conversion on sample
                vals = [row[i] for row in sample_data if row[i] is not None]
                if not vals:
                    self.categorical_columns.append(header)
                    continue
                
                numeric_count = 0
                for val in vals[:100]:  # Check max 100 values
                    try: 
                        float(val)
                        numeric_count += 1
                    except: pass
                
                if numeric_count / len(vals[:100]) >= 0.8:
                    self.numeric_columns.append(header)
                    # Convert entire column
                    for row in raw_data:
                        if row[i] is not None:
                            try: row[i] = float(row[i]) if '.' in str(row[i]) else int(row[i])
                            except: pass
                else:
                    self.categorical_columns.append(header)
            
            # Simple duplicate removal - compare string representations
            print("Removing duplicates...")
            seen = set()
            self.data = []
            for row in raw_data:
                key = '|'.join(str(cell) if cell is not None else '' for cell in row)
                if key not in seen:
                    seen.add(key)
                    self.data.append(row)
            
            self._box("LOADING SUMMARY", [
                ('Total Time (s)', round(time.time() - start, 1)),
                ('Final Rows', f"{len(self.data):,}"),
                ('Duplicates Removed', f"{len(raw_data) - len(self.data):,}"),
                ('Numeric Columns', len(self.numeric_columns)),
                ('Categorical Columns', len(self.categorical_columns))
            ])
            
        except Exception as e:
            print(f"Error: {e}")
    
    def _fast_stats(self, vals):
        """Optimized statistics calculation"""
        if not vals: return {'count': 0, 'mean': None, 'std': None, 'min': None, 'max': None, 'median': None}
        n = len(vals)
        if n == 1: return {'count': 1, 'mean': vals[0], 'std': 0, 'min': vals[0], 'max': vals[0], 'median': vals[0]}
        
        total = sum(vals)
        mean = total / n
        min_val, max_val = min(vals), max(vals)
        
        # Fast standard deviation
        variance = sum((x - mean) ** 2 for x in vals) / n
        std = variance ** 0.5
        
        # Fast median
        sorted_vals = sorted(vals)
        median = sorted_vals[n//2] if n % 2 else (sorted_vals[n//2-1] + sorted_vals[n//2]) / 2
        
        return {'count': n, 'mean': mean, 'std': std, 'min': min_val, 'max': max_val, 'median': median}
    
    def compute_column_statistics(self):
        self._header("STEP 2: COMPUTING STATISTICS")
        start = time.time()
        
        # Overall stats
        total_cells = len(self.data) * len(self.headers)
        missing = sum(1 for row in self.data for cell in row if cell is None)
        
        self._box("DATASET OVERVIEW", [
            ('Rows', len(self.data)), ('Columns', len(self.headers)),
            ('Numeric Cols', len(self.numeric_columns)), ('Categorical Cols', len(self.categorical_columns)),
            ('Missing Values', missing), ('Missing %', f"{100*missing/total_cells:.1f}%")
        ])
        
        # Numeric analysis
        if self.numeric_columns:
            self._header("NUMERIC STATISTICS", 2)
            num_data = []
            for col in self.numeric_columns:
                idx = self.headers.index(col)
                vals = [row[idx] for row in self.data if isinstance(row[idx], (int, float))]
                
                if vals:
                    s = self._fast_stats(vals)
                    num_data.append([
                        col[:20], self._fmt(s['count']), self._fmt(len(self.data) - s['count']),
                        self._fmt(s['mean'], 2), self._fmt(s['std'], 2),
                        self._fmt(s['min']), self._fmt(s['max'])
                    ])
                else:
                    num_data.append([col[:20], '0', str(len(self.data)), 'None', 'None', 'None', 'None'])
            
            self._table(['Column', 'Count', 'Missing', 'Mean', 'StdDev', 'Min', 'Max'], num_data)
        
        # Categorical analysis (top 5 only for speed)
        if self.categorical_columns:
            self._header("CATEGORICAL STATISTICS (Top 5)", 2)
            cat_data = []
            for col in self.categorical_columns[:5]:
                idx = self.headers.index(col)
                vals = [str(row[idx]) for row in self.data if row[idx] is not None]
                
                if vals:
                    counts = Counter(vals)
                    top = counts.most_common(1)[0]
                    cat_data.append([
                        col[:20], self._fmt(len(vals)), self._fmt(len(self.data) - len(vals)),
                        self._fmt(len(counts)), str(top[0])[:20], self._fmt(top[1])
                    ])
                else:
                    cat_data.append([col[:20], '0', str(len(self.data)), '0', 'None', '0'])
            
            self._table(['Column', 'Count', 'Missing', 'Unique', 'Top_Value', 'Top_Count'], cat_data)
        
        print(f"Statistics computed in {time.time()-start:.1f}s")
    
    def perform_groupby_analysis(self, group_cols, name="Group Analysis"):
        self._header(f"STEP 3: {name.upper()}")
        start = time.time()
        
        missing = [c for c in group_cols if c not in self.headers]
        if missing:
            print(f"Missing columns: {missing}")
            return
        
        # Fast grouping
        indices = [self.headers.index(c) for c in group_cols]
        groups = defaultdict(list)
        
        for row in self.data:
            key = tuple(str(row[i]) if row[i] is not None else 'None' for i in indices)
            groups[key].append(row)
        
        group_list = sorted(groups.items(), key=lambda x: len(x[1]), reverse=True)
        
        print(f"Created {len(group_list)} groups in {time.time()-start:.1f}s")
        
        # Group size stats
        sizes = [len(rows) for _, rows in group_list]
        if sizes:
            size_stats = self._fast_stats(sizes)
            self._box("GROUP SIZES", [
                ('Total Groups', len(group_list)), ('Mean Size', f"{size_stats['mean']:.1f}"),
                ('Min Size', int(size_stats['min'])), ('Max Size', int(size_stats['max']))
            ])
        
        # Top groups
        print(f"\nTop 10 Groups:")
        for i, (key, rows) in enumerate(group_list[:10]):
            group_name = key[0] if len(key) == 1 else f"{key[0]}+{len(key)-1}more"
            print(f"{i+1:2d}. {str(group_name)[:30]:30s} {len(rows):6,} rows")
        
        # Quick numeric stats for top 3 groups
        if self.numeric_columns and len(group_list) > 0:
            print(f"\nNumeric stats for top 3 groups:")
            for i, (key, rows) in enumerate(group_list[:3]):
                print(f"\nGroup {i+1}: {str(key[0])[:30]} ({len(rows)} rows)")
                for col in self.numeric_columns[:3]:  # Limit columns for speed
                    idx = self.headers.index(col)
                    vals = [row[idx] for row in rows if isinstance(row[idx], (int, float))]
                    if vals:
                        s = self._fast_stats(vals)
                        print(f"  {col[:15]:15s}: mean={s['mean']:8.1f} min={s['min']:8.1f} max={s['max']:8.1f}")
    
    def run_complete_analysis(self):
        import time
        total_start = time.time()
        print("\n=== PURE PYTHON ANALYSIS START ===")
        step_start = time.time()
        self.load_and_clean_data()
        print(f"[TIMER] load_and_clean_data: {time.time() - step_start:.3f}s")
        step_start = time.time()
        self.compute_column_statistics()
        print(f"[TIMER] compute_column_statistics: {time.time() - step_start:.3f}s")
        step_start = time.time()
        self.perform_groupby_analysis(['ad_id'], name="Group by ad_id")
        print(f"[TIMER] perform_groupby_analysis (ad_id): {time.time() - step_start:.3f}s")
        print(f"[TIMER] TOTAL: {time.time() - total_start:.3f}s\n=== PURE PYTHON ANALYSIS END ===\n")
    
    def run_complete_analysis(self):
        print("FAST PYTHON DATASET ANALYZER")
        print("="*70)
        
        try:
            self.load_and_clean_data()
            if not self.data: 
                print("No data loaded!")
                return
            
            self.compute_column_statistics()
            
            # Required groupby analyses
            if 'page_id' in self.headers:
                self.perform_groupby_analysis(['page_id'], "Analysis by PAGE_ID")
            
            if 'page_id' in self.headers and 'ad_id' in self.headers:
                self.perform_groupby_analysis(['page_id', 'ad_id'], "Analysis by PAGE_ID and AD_ID")
            
            # Find one additional grouping candidate quickly
            candidates = []
            for col in self.categorical_columns[:10]:  # Check only first 10
                if col not in ['page_id', 'ad_id']:
                    idx = self.headers.index(col)
                    vals = [str(row[idx]) for row in self.data[:1000] if row[idx]]  # Sample only
                    if vals:
                        unique = len(set(vals))
                        if 2 <= unique <= 20:  # Good grouping range
                            candidates.append((col, unique))
            
            if candidates:
                candidates.sort(key=lambda x: x[1])
                best_col = candidates[0][0]
                self.perform_groupby_analysis([best_col], f"Analysis by {best_col.upper()}")
            
            print(f"\n{'='*70}")
            print("ANALYSIS COMPLETED SUCCESSFULLY")
            print(f"{'='*70}")
            
        except Exception as e:
            print(f"Error: {e}")

def main():
    analyzer = PythonAnalyzer("../period_03/2024_fb_ads_president_scored_anon.csv")
    analyzer.run_complete_analysis()

if __name__ == "__main__":
    main()