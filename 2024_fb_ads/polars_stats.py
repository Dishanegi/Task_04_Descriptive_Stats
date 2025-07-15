import polars as pl
import time
import json

class PolarsAnalyzer:
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
        if v is None:
            return 'None'
        # Handle NaN values
        if isinstance(v, float):
            import math
            if math.isnan(v):
                return 'None'
        return f"{int(v):,}" if isinstance(v, (int, float)) and v == int(v) else f"{v:,.{dp}f}" if isinstance(v, (int, float)) else str(v)

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
            # Auto-detect delimiter and load with Polars
            print("Auto-detecting delimiter...")
            with open(self.filepath, 'r', encoding='utf-8') as f:
                first_line = f.readline()
                delimiter = '\t' if first_line.count('\t') > first_line.count(',') else ','
            
            # Load data with Polars - much faster than pandas/csv
            print("Loading data with Polars...")
            self.df = pl.read_csv(
                self.filepath,
                separator=delimiter,
                ignore_errors=True,
                null_values=['', 'null', 'NULL', 'None', 'N/A', 'NA'],
                infer_schema_length=10000,  # Sample for schema inference
                try_parse_dates=True
            )
            
            print(f"Loaded {self.df.height:,} rows and {self.df.width} columns in {time.time()-start:.1f}s")
            
            # Clean column names
            self.df = self.df.rename({col: col.strip() for col in self.df.columns})
            
            # Handle complex JSON fields
            complex_fields = {'delivery_by_region', 'demographic_distribution', 'publisher_platforms'}
            for field in complex_fields:
                if field in self.df.columns:
                    # Convert JSON strings to parsed objects where possible
                    self.df = self.df.with_columns([
                        pl.col(field).map_elements(
                            lambda x: self._parse_json_safe(x) if x is not None else None,
                            return_dtype=pl.String
                        )
                    ])
            
            # Remove duplicates efficiently
            print("Removing duplicates...")
            initial_rows = self.df.height
            self.df = self.df.unique()
            duplicates_removed = initial_rows - self.df.height
            
            # Classify columns by type
            self.numeric_columns = []
            self.categorical_columns = []
            
            for col in self.df.columns:
                dtype = self.df[col].dtype
                if dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float32, pl.Float64]:
                    self.numeric_columns.append(col)
                else:
                    self.categorical_columns.append(col)
            
            self._box("LOADING SUMMARY", [
                ('Total Time (s)', round(time.time() - start, 1)),
                ('Final Rows', f"{self.df.height:,}"),
                ('Columns', self.df.width),
                ('Duplicates Removed', f"{duplicates_removed:,}"),
                ('Numeric Columns', len(self.numeric_columns)),
                ('Categorical Columns', len(self.categorical_columns))
            ])
            
        except Exception as e:
            print(f"Error loading data: {e}")
            raise
    
    def compute_column_statistics(self):
        self._header("STEP 2: COMPUTING STATISTICS")
        start = time.time()
        
        # Overall dataset statistics
        total_cells = self.df.height * self.df.width
        missing_count = self.df.null_count().sum_horizontal().item()
        
        self._box("DATASET OVERVIEW", [
            ('Rows', self.df.height), 
            ('Columns', self.df.width),
            ('Numeric Cols', len(self.numeric_columns)), 
            ('Categorical Cols', len(self.categorical_columns)),
            ('Missing Values', missing_count), 
            ('Missing %', f"{100*missing_count/total_cells:.1f}%")
        ])
        
        # Numeric statistics - much faster with Polars
        if self.numeric_columns:
            self._header("NUMERIC STATISTICS", 2)
            
            # Get all numeric stats in one operation
            numeric_stats = self.df.select(self.numeric_columns).describe()
            
            num_data = []
            for col in self.numeric_columns:
                try:
                    count = numeric_stats.filter(pl.col("statistic") == "count")[col].item()
                    mean = numeric_stats.filter(pl.col("statistic") == "mean")[col].item()
                    std = numeric_stats.filter(pl.col("statistic") == "std")[col].item()
                    min_val = numeric_stats.filter(pl.col("statistic") == "min")[col].item()
                    max_val = numeric_stats.filter(pl.col("statistic") == "max")[col].item()
                    missing = self.df.height - count
                except:
                    # Fallback to individual column stats if describe() format is different
                    col_stats = self.df[col].drop_nulls()
                    count = col_stats.len()
                    mean = col_stats.mean()
                    std = col_stats.std()
                    min_val = col_stats.min()
                    max_val = col_stats.max()
                    missing = self.df.height - count
                
                num_data.append([
                    col[:20], 
                    self._fmt(count), 
                    self._fmt(missing),
                    self._fmt(mean, 2), 
                    self._fmt(std, 2),
                    self._fmt(min_val), 
                    self._fmt(max_val)
                ])
            
            self._table(['Column', 'Count', 'Missing', 'Mean', 'StdDev', 'Min', 'Max'], num_data)
        
        # Categorical statistics (top 5 for speed)
        if self.categorical_columns:
            self._header("CATEGORICAL STATISTICS (Top 5)", 2)
            cat_data = []
            
            for col in self.categorical_columns[:5]:
                # Get value counts efficiently
                value_counts = self.df[col].value_counts().sort("count", descending=True)
                
                non_null_count = self.df[col].drop_nulls().len()
                missing_count = self.df.height - non_null_count
                unique_count = value_counts.height
                
                if unique_count > 0:
                    top_value = value_counts[0, col]
                    top_count = value_counts[0, "count"]
                else:
                    top_value, top_count = 'None', 0
                
                cat_data.append([
                    col[:20], 
                    self._fmt(non_null_count), 
                    self._fmt(missing_count),
                    self._fmt(unique_count), 
                    str(top_value)[:20], 
                    self._fmt(top_count)
                ])
            
            self._table(['Column', 'Count', 'Missing', 'Unique', 'Top_Value', 'Top_Count'], cat_data)
        
        print(f"Statistics computed in {time.time()-start:.1f}s")
    
    def perform_groupby_analysis(self, group_cols, name="Group Analysis"):
        self._header(f"STEP 3: {name.upper()}")
        start = time.time()
        
        # Check if columns exist
        missing = [c for c in group_cols if c not in self.df.columns]
        if missing:
            print(f"Missing columns: {missing}")
            return
        
        # Polars groupby is extremely fast
        try:
            # Get group sizes
            group_sizes = self.df.group_by(group_cols).len().sort("len", descending=True)
            
            print(f"Created {group_sizes.height} groups in {time.time()-start:.1f}s")
            
            # Group size statistics
            if group_sizes.height > 0:
                sizes = group_sizes["len"]
                mean_size = sizes.mean()
                min_size = sizes.min()
                max_size = sizes.max()
                
                self._box("GROUP SIZES", [
                    ('Total Groups', group_sizes.height), 
                    ('Mean Size', f"{mean_size:.1f}"),
                    ('Min Size', int(min_size)), 
                    ('Max Size', int(max_size))
                ])
            
            # Top groups
            print(f"\nTop 10 Groups:")
            top_groups = group_sizes.head(10).to_dicts()
            for i, row in enumerate(top_groups):
                if len(group_cols) == 1:
                    group_name = str(row[group_cols[0]])
                else:
                    group_name = f"{row[group_cols[0]]}+{len(group_cols)-1}more"
                
                print(f"{i+1:2d}. {group_name[:30]:30s} {row['len']:6,} rows")
            
            # Quick numeric stats for top 3 groups
            if self.numeric_columns and group_sizes.height > 0:
                print(f"\nNumeric stats for top 3 groups:")
                top_3_groups = group_sizes.head(3).to_dicts()
                
                for i, group_info in enumerate(top_3_groups):
                    # Create filter condition for this group
                    filter_conditions = []
                    for col in group_cols:
                        filter_conditions.append(pl.col(col) == group_info[col])
                    
                    # Combine conditions with AND
                    combined_filter = filter_conditions[0]
                    for condition in filter_conditions[1:]:
                        combined_filter = combined_filter & condition
                    
                    group_data = self.df.filter(combined_filter)
                    
                    group_name = str(group_info[group_cols[0]])
                    print(f"\nGroup {i+1}: {group_name[:30]} ({group_info['len']} rows)")
                    
                    # Get stats for top 3 numeric columns
                    for col in self.numeric_columns[:3]:
                        col_data = group_data[col].drop_nulls()
                        if col_data.len() > 0:
                            mean_val = col_data.mean()
                            min_val = col_data.min()
                            max_val = col_data.max()
                            
                            print(f"  {col[:15]:15s}: mean={mean_val:8.1f} min={min_val:8.1f} max={max_val:8.1f}")
        
        except Exception as e:
            print(f"Error in groupby analysis: {e}")
    
    def run_complete_analysis(self):
        try:
            total_start = time.time()
            print("\n=== POLARS ANALYSIS START ===")
            step_start = time.time()
            self.load_and_clean_data()
            print(f"[TIMER] load_and_clean_data: {time.time() - step_start:.3f}s")
            step_start = time.time()
            self.compute_column_statistics()
            print(f"[TIMER] compute_column_statistics: {time.time() - step_start:.3f}s")
            step_start = time.time()
            self.perform_groupby_analysis(['ad_id'], name="Group by ad_id")
            print(f"[TIMER] perform_groupby_analysis (ad_id): {time.time() - step_start:.3f}s")
            print(f"[TIMER] TOTAL: {time.time() - total_start:.3f}s\n=== POLARS ANALYSIS END ===\n")
                
            # Required groupby analyses
            if 'page_id' in self.df.columns:
                self.perform_groupby_analysis(['page_id'], "Analysis by PAGE_ID")
            
            if 'page_id' in self.df.columns and 'ad_id' in self.df.columns:
                self.perform_groupby_analysis(['page_id', 'ad_id'], "Analysis by PAGE_ID and AD_ID")
            
            # Find additional grouping candidate
            candidates = []
            for col in self.categorical_columns[:10]:
                if col not in ['page_id', 'ad_id']:
                    try:
                        unique_count = self.df[col].n_unique()
                        if 2 <= unique_count <= 20:
                            candidates.append((col, unique_count))
                    except:
                        continue
            
            if candidates:
                candidates.sort(key=lambda x: x[1])
                best_col = candidates[0][0]
                self.perform_groupby_analysis([best_col], f"Analysis by {best_col.upper()}")
            
            print(f"\n{'='*70}")
            print("ANALYSIS COMPLETED SUCCESSFULLY")
            print(f"{'='*70}")
        except Exception as e:
            print(f"Error in analysis: {e}")
            import traceback
            traceback.print_exc()

def main():
    analyzer = PolarsAnalyzer("../period_03/2024_fb_ads_president_scored_anon.csv")
    analyzer.run_complete_analysis()

if __name__ == "__main__":
    main()