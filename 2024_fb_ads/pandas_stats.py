import pandas as pd
import time
import json

class PandasAnalyzer:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.df = None
        self.numeric_columns = []
        self.categorical_columns = []
    
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
        return 'None' if pd.isna(v) else f"{int(v):,}" if isinstance(v, (int, float)) and v == int(v) else f"{v:,.{dp}f}" if isinstance(v, (int, float)) else str(v)

    def _box(self, title, stats):
        lw, vw = max(len(l) for l, _ in stats), max(len(str(v)) for _, v in stats)
        print(f"\n┌─{title}─{'─' * (lw + vw + 5)}┐")
        [print(f"│ {l.ljust(lw)} : {(self._fmt(v) if isinstance(v, (int, float)) else str(v)).rjust(vw)} │") for l, v in stats]
        print(f"└{'─' * (lw + vw + 10)}┘")

    def _parse_json_safe(self, value):
        """Fast JSON parsing with fallback"""
        if pd.isna(value) or value in ('{}', '[]', ''): return None
        if not ('{' in str(value) or '[' in str(value)): return value
        try: return json.loads(value)
        except: return value
        
    def load_and_clean_data(self):
        self._header("STEP 1: LOADING AND CLEANING DATASET")
        start = time.time()
        
        try:
            # Auto-detect delimiter
            with open(self.filepath, 'r', encoding='utf-8') as f:
                first_line = f.readline()
                delim = '\t' if first_line.count('\t') > first_line.count(',') else ','
            
            # Load data with pandas
            print(f"Loading data from {self.filepath}...")
            self.df = pd.read_csv(
                self.filepath, 
                delimiter=delim,
                dtype=str,  # Load everything as string first
                na_values=['', 'null', 'NULL', 'None', 'N/A', 'NA'],
                keep_default_na=True
            )
            
            print(f"Loaded {len(self.df):,} rows in {time.time()-start:.1f}s")
            
            # Clean column names
            self.df.columns = self.df.columns.str.strip()
            
            # Handle complex JSON fields
            complex_fields = {'delivery_by_region', 'demographic_distribution', 'publisher_platforms'}
            for col in complex_fields:
                if col in self.df.columns:
                    print(f"Processing JSON field: {col}")
                    self.df[col] = self.df[col].apply(self._parse_json_safe)
            
            # Fast type detection and conversion
            print("Detecting column types...")
            sample_size = min(1000, len(self.df))
            sample_df = self.df.head(sample_size)
            
            for col in self.df.columns:
                if col in complex_fields:
                    self.categorical_columns.append(col)
                    continue
                
                # Try numeric conversion on sample
                sample_non_null = sample_df[col].dropna()
                if len(sample_non_null) == 0:
                    self.categorical_columns.append(col)
                    continue
                
                # Check if numeric
                try:
                    pd.to_numeric(sample_non_null.head(100), errors='raise')
                    # If successful, convert entire column
                    self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                    self.numeric_columns.append(col)
                except:
                    self.categorical_columns.append(col)
            
            # Remove duplicates
            print("Removing duplicates...")
            initial_rows = len(self.df)
            self.df = self.df.drop_duplicates()
            duplicates_removed = initial_rows - len(self.df)
            
            self._box("LOADING SUMMARY", [
                ('Total Time (s)', round(time.time() - start, 1)),
                ('Final Rows', f"{len(self.df):,}"),
                ('Duplicates Removed', f"{duplicates_removed:,}"),
                ('Numeric Columns', len(self.numeric_columns)),
                ('Categorical Columns', len(self.categorical_columns))
            ])
            
        except Exception as e:
            print(f"Error: {e}")
    
    def compute_column_statistics(self):
        self._header("STEP 2: COMPUTING STATISTICS")
        start = time.time()
        
        # Overall stats
        total_cells = len(self.df) * len(self.df.columns)
        missing = self.df.isnull().sum().sum()
        
        self._box("DATASET OVERVIEW", [
            ('Rows', len(self.df)), ('Columns', len(self.df.columns)),
            ('Numeric Cols', len(self.numeric_columns)), ('Categorical Cols', len(self.categorical_columns)),
            ('Missing Values', missing), ('Missing %', f"{100*missing/total_cells:.1f}%")
        ])
        
        # Numeric analysis
        if self.numeric_columns:
            self._header("NUMERIC STATISTICS", 2)
            num_data = []
            
            for col in self.numeric_columns:
                stats = self.df[col].describe()
                count = int(stats['count'])
                missing_count = len(self.df) - count
                
                num_data.append([
                    col[:20], 
                    self._fmt(count), 
                    self._fmt(missing_count),
                    self._fmt(stats['mean'], 2), 
                    self._fmt(stats['std'], 2),
                    self._fmt(stats['min']), 
                    self._fmt(stats['max'])
                ])
            
            self._table(['Column', 'Count', 'Missing', 'Mean', 'StdDev', 'Min', 'Max'], num_data)
        
        # Categorical analysis (top 5 only for speed)
        if self.categorical_columns:
            self._header("CATEGORICAL STATISTICS (Top 5)", 2)
            cat_data = []
            
            for col in self.categorical_columns[:5]:
                non_null = self.df[col].dropna()
                
                if len(non_null) > 0:
                    value_counts = non_null.value_counts()
                    unique_count = len(value_counts)
                    top_value = str(value_counts.index[0])[:20]
                    top_count = value_counts.iloc[0]
                    
                    cat_data.append([
                        col[:20], 
                        self._fmt(len(non_null)), 
                        self._fmt(len(self.df) - len(non_null)),
                        self._fmt(unique_count), 
                        top_value, 
                        self._fmt(top_count)
                    ])
                else:
                    cat_data.append([col[:20], '0', str(len(self.df)), '0', 'None', '0'])
            
            self._table(['Column', 'Count', 'Missing', 'Unique', 'Top_Value', 'Top_Count'], cat_data)
        
        print(f"Statistics computed in {time.time()-start:.1f}s")
    
    def perform_groupby_analysis(self, group_cols, name="Group Analysis"):
        self._header(f"STEP 3: {name.upper()}")
        start = time.time()
        
        missing = [c for c in group_cols if c not in self.df.columns]
        if missing:
            print(f"Missing columns: {missing}")
            return
        
        # Create groups using pandas groupby
        grouped = self.df.groupby(group_cols, dropna=False)
        group_sizes = grouped.size().sort_values(ascending=False)
        
        print(f"Created {len(group_sizes)} groups in {time.time()-start:.1f}s")
        
        # Group size stats
        if len(group_sizes) > 0:
            size_stats = group_sizes.describe()
            self._box("GROUP SIZES", [
                ('Total Groups', len(group_sizes)), 
                ('Mean Size', f"{size_stats['mean']:.1f}"),
                ('Min Size', int(size_stats['min'])), 
                ('Max Size', int(size_stats['max']))
            ])
        
        # Top groups
        print(f"\nTop 10 Groups:")
        for i, (key, size) in enumerate(group_sizes.head(10).items()):
            if isinstance(key, tuple):
                group_name = f"{key[0]}+{len(key)-1}more" if len(key) > 1 else str(key[0])
            else:
                group_name = str(key)
            print(f"{i+1:2d}. {group_name[:30]:30s} {size:6,} rows")
        
        # Quick numeric stats for top 3 groups
        if self.numeric_columns and len(group_sizes) > 0:
            print(f"\nNumeric stats for top 3 groups:")
            top_3_keys = group_sizes.head(3).index
            
            for i, key in enumerate(top_3_keys):
                # Handle both single column and multi-column grouping
                if len(group_cols) == 1:
                    # For single column grouping, wrap key in tuple for get_group
                    group_key = (key,) if not isinstance(key, tuple) else key
                    key_str = str(key)
                else:
                    # For multi-column grouping, key is already a tuple
                    group_key = key
                    key_str = str(key[0]) if isinstance(key, tuple) else str(key)
                
                group_data = grouped.get_group(group_key)
                print(f"\nGroup {i+1}: {key_str[:30]} ({len(group_data)} rows)")
                
                for col in self.numeric_columns[:3]:  # Limit columns for speed
                    col_data = group_data[col].dropna()
                    if len(col_data) > 0:
                        stats = col_data.describe()
                        print(f"  {col[:15]:15s}: mean={stats['mean']:8.1f} min={stats['min']:8.1f} max={stats['max']:8.1f}")
    
    def run_complete_analysis(self):
        import time
        total_start = time.time()
        print("\n=== PANDAS ANALYSIS START ===")
        step_start = time.time()
        self.load_and_clean_data()
        print(f"[TIMER] load_and_clean_data: {time.time() - step_start:.3f}s")
        step_start = time.time()
        self.compute_column_statistics()
        print(f"[TIMER] compute_column_statistics: {time.time() - step_start:.3f}s")
        step_start = time.time()
        self.perform_groupby_analysis(['ad_id'], name="Group by ad_id")
        print(f"[TIMER] perform_groupby_analysis (ad_id): {time.time() - step_start:.3f}s")
        print(f"[TIMER] TOTAL: {time.time() - total_start:.3f}s\n=== PANDAS ANALYSIS END ===\n")
    
    def run_complete_analysis(self):
        print("FAST PANDAS DATASET ANALYZER")
        print("="*70)
        
        try:
            self.load_and_clean_data()
            if self.df is None or len(self.df) == 0: 
                print("No data loaded!")
                return
            
            self.compute_column_statistics()
            
            # Required groupby analyses
            if 'page_id' in self.df.columns:
                self.perform_groupby_analysis(['page_id'], "Analysis by PAGE_ID")
            
            if 'page_id' in self.df.columns and 'ad_id' in self.df.columns:
                self.perform_groupby_analysis(['page_id', 'ad_id'], "Analysis by PAGE_ID and AD_ID")
            
            # Find one additional grouping candidate quickly
            candidates = []
            for col in self.categorical_columns[:10]:  # Check only first 10
                if col not in ['page_id', 'ad_id']:
                    sample_data = self.df[col].dropna().head(1000)  # Sample only
                    if len(sample_data) > 0:
                        unique = sample_data.nunique()
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
    analyzer = PandasAnalyzer("../period_03/2024_fb_ads_president_scored_anon.csv")
    analyzer.run_complete_analysis()

if __name__ == "__main__":
    main()