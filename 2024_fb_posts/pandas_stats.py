import pandas as pd

class SocialMediaPostsAnalyzer:
    def __init__(self, csv_file_path: str):
        self.csv_file_path = csv_file_path
        self.df = None
        self.numeric_columns = []
        self.text_columns = []
        
    def _table(self, data, title=""):
        if not data: return
        if title: print(f"\n{title}")
        keys = list(data[0].keys())
        w = {k: max(len(str(k)), max(len(str(r.get(k, ''))) for r in data)) for k in keys}
        s = "+" + "+".join("-" * (w[k] + 2) for k in keys) + "+"
        print(s + f"\n| " + " | ".join(str(k).ljust(w[k]) for k in keys) + " |\n" + s)
        for r in data: print("| " + " | ".join(str(r.get(k, '')).ljust(w[k]) for k in keys) + " |")
        print(s)
    
    def load_data(self):
        print("╔═══════════════════════════════════════════════════╗\n║           LOADING AND CLEANING DATA              ║\n╚═══════════════════════════════════════════════════╝")
        self.df = pd.read_csv(self.csv_file_path)
        print(f"Dataset: {self.df.shape[0]:,} rows × {self.df.shape[1]} columns")
        print(f"Null values in {sum(1 for c in self.df.columns if self.df[c].isnull().sum() > 0)} columns")
        
        # Use pandas-only methods for dtype detection
        self.numeric_columns = list(self.df.select_dtypes(include=['int64', 'float64', 'int32', 'float32']).columns)
        self.text_columns = list(self.df.select_dtypes(include=['object', 'string']).columns)
        
        # Check if text columns can be converted to numeric
        for c in self.text_columns[:]:
            try:
                sample = self.df[c].dropna().head(100)
                if len(pd.to_numeric(sample, errors='coerce').dropna()) / len(sample) > 0.8:
                    self.numeric_columns.append(c)
                    self.text_columns.remove(c)
            except: pass
        
        print(f"Numeric: {len(self.numeric_columns)} | Text: {len(self.text_columns)}")
        return self.df
    
    def overall_stats(self):
        print("\n╔════════════════════════════════════════════════════════════╗\n║                PART 1: OVERALL DATASET ANALYSIS           ║\n╚════════════════════════════════════════════════════════════╝")
        
        if self.numeric_columns:
            stats = []
            for c in self.numeric_columns:
                d = pd.to_numeric(self.df[c], errors='coerce').dropna()
                stats.append({'Column': c, 'Count': f"{len(d):,}", 'Mean': f"{d.mean():.2f}" if len(d) > 0 else "N/A",
                            'Min': f"{d.min()}" if len(d) > 0 else "N/A", 'Max': f"{d.max()}" if len(d) > 0 else "N/A", 
                            'Std': f"{d.std():.2f}" if len(d) > 1 else "0.00" if len(d) == 1 else "N/A"})
            self._table(stats, "NUMERIC COLUMNS STATISTICS")
        
        if self.text_columns:
            stats = []
            for c in self.text_columns:
                d = self.df[c].dropna()
                if len(d) > 0:
                    vc = d.value_counts()
                    top_val, top_cnt = (vc.index[0], vc.iloc[0]) if len(vc) > 0 else ("N/A", 0)
                    stats.append({'Column': c, 'Count': f"{len(d):,}", 'Unique': f"{d.nunique():,}",
                                'Top Value': str(top_val)[:20] + ('...' if len(str(top_val)) > 20 else ''), 'Top Count': f"{top_cnt:,}"})
                else:
                    stats.append({'Column': c, 'Count': "0", 'Unique': "0", 'Top Value': "N/A", 'Top Count': "0"})
            self._table(stats, "TEXT COLUMNS STATISTICS")
    
    def analyze_facebook_id(self):
        print("\n╔═══════════════════════════════════════════════════╗\n║         PART 2A: GROUPING BY FACEBOOK_ID         ║\n╚═══════════════════════════════════════════════════╝")
        if 'Facebook_Id' not in self.df.columns: 
            print("WARNING: 'Facebook_Id' column not found"); return None
        
        print(f"Unique Facebook pages: {self.df['Facebook_Id'].nunique():,}")
        
        if self.numeric_columns:
            stats = []
            for fb_id in self.df['Facebook_Id'].unique()[:10]:
                gd = self.df[self.df['Facebook_Id'] == fb_id]
                stat = {'Facebook_Id': str(fb_id)[:15] + '...'}
                for c in self.numeric_columns[:3]:
                    cd = pd.to_numeric(gd[c], errors='coerce').dropna()
                    stat[f'{c}_Mean'] = f"{cd.mean():.2f}" if len(cd) > 0 else "N/A"
                    stat[f'{c}_Count'] = f"{len(cd)}"
                stats.append(stat)
            if stats: self._table(stats, "SAMPLE: Top 10 Pages - Numeric Statistics")
        return self.df.groupby('Facebook_Id')
    
    def analyze_facebook_post_id(self):
        print("\n╔═══════════════════════════════════════════════════════════╗\n║       PART 2B: GROUPING BY FACEBOOK_ID + POST_ID         ║\n╚═══════════════════════════════════════════════════════════╝")
        missing = [c for c in ['Facebook_Id', 'post_id'] if c not in self.df.columns]
        if missing: print(f"WARNING: Missing columns: {missing}"); return None
        
        unique_combinations = self.df[['Facebook_Id', 'post_id']].drop_duplicates()
        print(f"Unique combinations: {len(unique_combinations):,}")
        
        if self.numeric_columns:
            stats = []
            for _, row in unique_combinations.head(10).iterrows():
                fb_id, post_id = row['Facebook_Id'], row['post_id']
                gd = self.df[(self.df['Facebook_Id'] == fb_id) & (self.df['post_id'] == post_id)]
                stat = {'Facebook_Id': str(fb_id)[:10] + '...', 'Post_ID': str(post_id)[:10] + '...', 'Rows': len(gd)}
                for c in self.numeric_columns[:2]:
                    cd = pd.to_numeric(gd[c], errors='coerce').dropna()
                    stat[f'{c}_Mean'] = f"{cd.mean():.2f}" if len(cd) > 0 else "N/A"
                stats.append(stat)
            if stats: self._table(stats, "SAMPLE: First 10 Combinations - Statistics")
        return self.df.groupby(['Facebook_Id', 'post_id'])
    
    def analyze_page_category(self):
        print("\n╔═══════════════════════════════════════════════════╗\n║       PART 2C: GROUPING BY PAGE CATEGORY         ║\n╚═══════════════════════════════════════════════════╝")
        if 'Page Category' not in self.df.columns: 
            print("WARNING: 'Page Category' column not found"); return None
        
        print(f"Unique page categories: {self.df['Page Category'].nunique():,}")
        
        cat_counts = self.df['Page Category'].value_counts()
        cat_data = [{'Rank': i, 'Category': cat, 'Posts': f"{count:,}", 
                    'Percentage': f"{(count / len(self.df)) * 100:.1f}%"} 
                   for i, (cat, count) in enumerate(cat_counts.items(), 1)]
        self._table(cat_data, "CATEGORIES BY POST COUNT")
        
        if self.numeric_columns:
            stats = []
            for cat in self.df['Page Category'].unique():
                gd = self.df[self.df['Page Category'] == cat]
                stat = {'Category': cat}
                for c in self.numeric_columns[:4]:
                    cd = pd.to_numeric(gd[c], errors='coerce').dropna()
                    stat[f'{c}_Mean'] = f"{cd.mean():.2f}" if len(cd) > 0 else "N/A"
                    stat[f'{c}_Count'] = f"{len(cd)}"
                stats.append(stat)
            if stats: self._table(stats, "STATISTICS BY CATEGORY")
        return self.df.groupby('Page Category')
    
    def compare_groups(self):
        print("\n╔═══════════════════════════════════════════════════╗\n║           PART 2D: COMPARING GROUPS              ║\n╚═══════════════════════════════════════════════════╝")
        
        eng_cols = [c for c in ['Likes', 'Comments', 'Shares', 'Love', 'Wow', 'Haha', 'Sad', 'Angry', 'Care'] if c in self.df.columns]
        
        if 'Page Category' in self.df.columns and eng_cols:
            for c in eng_cols[:3]:
                cat_totals = self.df.groupby('Page Category')[c].apply(lambda x: pd.to_numeric(x, errors='coerce').sum()).nlargest(5)
                eng_data = [{'Rank': i, 'Category': cat, f'{c}': f"{total:,.0f}"} 
                           for i, (cat, total) in enumerate(cat_totals.items(), 1)]
                self._table(eng_data, f"{c} Rankings:")
        
        if 'Facebook_Id' in self.df.columns and 'Likes' in self.df.columns:
            metrics = [c for c in ['Likes', 'Comments', 'Shares'] if c in self.df.columns]
            if metrics:
                page_totals = self.df.groupby('Facebook_Id')[metrics].apply(
                    lambda x: x.apply(lambda col: pd.to_numeric(col, errors='coerce').sum())
                )
                page_totals['Total_Engagement'] = page_totals[metrics].sum(axis=1)
                page_totals = page_totals.nlargest(10, 'Total_Engagement')
                
                page_data = [{'Rank': i, 'Page_ID': str(fb_id)[:15] + '...', 
                             'Total_Engagement': f"{row['Total_Engagement']:,.0f}",
                             'Likes': f"{row.get('Likes', 0):,.0f}", 
                             'Comments': f"{row.get('Comments', 0):,.0f}", 
                             'Shares': f"{row.get('Shares', 0):,.0f}"} 
                            for i, (fb_id, row) in enumerate(page_totals.iterrows(), 1)]
                self._table(page_data, "TOP FACEBOOK PAGES BY TOTAL ENGAGEMENT")
    
    def run_analysis(self):
        import time
        total_start = time.time()
        print("\n=== PANDAS FB POSTS ANALYSIS START ===")
        step_start = time.time()
        self.load_data()
        print(f"[TIMER] load_data: {time.time() - step_start:.3f}s")
        step_start = time.time()
        self.overall_stats()
        print(f"[TIMER] overall_stats: {time.time() - step_start:.3f}s")
        step_start = time.time()
        self.analyze_facebook_id()
        print(f"[TIMER] analyze_facebook_id: {time.time() - step_start:.3f}s")
        step_start = time.time()
        self.analyze_facebook_post_id()
        print(f"[TIMER] analyze_facebook_post_id: {time.time() - step_start:.3f}s")
        step_start = time.time()
        self.analyze_page_category()
        print(f"[TIMER] analyze_page_category: {time.time() - step_start:.3f}s")
        step_start = time.time()
        self.compare_groups()
        print(f"[TIMER] compare_groups: {time.time() - step_start:.3f}s")
        print(f"[TIMER] TOTAL: {time.time() - total_start:.3f}s\n=== PANDAS FB POSTS ANALYSIS END ===\n")

if __name__ == "__main__":
    analyzer = SocialMediaPostsAnalyzer('../period_03/2024_fb_posts_president_scored_anon.csv')
    analyzer.run_analysis()