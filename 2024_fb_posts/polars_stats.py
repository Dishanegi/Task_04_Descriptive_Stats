import polars as pl

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
        self.df = pl.read_csv(self.csv_file_path)
        print(f"Dataset: {self.df.height:,} rows × {self.df.width} columns")
        nulls = sum(1 for c in self.df.columns if self.df[c].null_count() > 0)
        print(f"Null values in {nulls} columns")
        
        self.numeric_columns = [c for c in self.df.columns if self.df[c].dtype in [pl.Int64, pl.Float64, pl.Int32, pl.Float32]]
        self.text_columns = [c for c in self.df.columns if c not in self.numeric_columns]
        
        for c in self.text_columns[:]:
            try:
                sample = self.df[c].drop_nulls().head(100).cast(pl.Float64, strict=False).drop_nulls()
                if len(sample) / len(self.df[c].drop_nulls().head(100)) > 0.8:
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
                d = self.df[c].cast(pl.Float64, strict=False).drop_nulls()
                if len(d) > 0:
                    stats.append({'Column': c, 'Count': f"{len(d):,}", 'Mean': f"{d.mean():.2f}",
                                'Min': f"{d.min()}", 'Max': f"{d.max()}", 'Std': f"{d.std():.2f}" if len(d) > 1 else "0.00"})
                else:
                    stats.append({'Column': c, 'Count': "0", 'Mean': "N/A", 'Min': "N/A", 'Max': "N/A", 'Std': "N/A"})
            self._table(stats, "📊 NUMERIC COLUMNS STATISTICS")
        
        if self.text_columns:
            stats = []
            for c in self.text_columns:
                d = self.df[c].drop_nulls()
                if len(d) > 0:
                    vc = d.value_counts()
                    top_val, top_cnt = (vc[0, 0], vc[0, 1]) if len(vc) > 0 else ("N/A", 0)
                    stats.append({'Column': c, 'Count': f"{len(d):,}", 'Unique': f"{d.n_unique():,}",
                                'Top Value': str(top_val)[:20] + ('...' if len(str(top_val)) > 20 else ''), 'Top Count': f"{top_cnt:,}"})
                else:
                    stats.append({'Column': c, 'Count': "0", 'Unique': "0", 'Top Value': "N/A", 'Top Count': "0"})
            self._table(stats, "📝 TEXT COLUMNS STATISTICS")
    
    def analyze_facebook_id(self):
        print("\n╔═══════════════════════════════════════════════════╗\n║         PART 2A: GROUPING BY FACEBOOK_ID         ║\n╚═══════════════════════════════════════════════════╝")
        if 'Facebook_Id' not in self.df.columns: print("⚠️  'Facebook_Id' column not found"); return None
        
        print(f"📄 Unique Facebook pages: {self.df['Facebook_Id'].n_unique():,}")
        
        if self.numeric_columns:
            stats = []
            for fb_id in self.df['Facebook_Id'].unique().head(10):
                gd = self.df.filter(pl.col('Facebook_Id') == fb_id)
                stat = {'Facebook_Id': str(fb_id)[:15] + '...'}
                for c in self.numeric_columns[:3]:
                    if c in gd.columns:
                        cd = gd[c].cast(pl.Float64, strict=False).drop_nulls()
                        stat[f'{c}_Mean'] = f"{cd.mean():.2f}" if len(cd) > 0 else "N/A"
                        stat[f'{c}_Count'] = f"{len(cd)}"
                stats.append(stat)
            if stats: self._table(stats, "📊 SAMPLE: Top 10 Pages - Numeric Statistics")
        return self.df.group_by('Facebook_Id')
    
    def analyze_facebook_post_id(self):
        print("\n╔═══════════════════════════════════════════════════════════╗\n║       PART 2B: GROUPING BY FACEBOOK_ID + POST_ID         ║\n╚═══════════════════════════════════════════════════════════╝")
        missing = [c for c in ['Facebook_Id', 'post_id'] if c not in self.df.columns]
        if missing: print(f"⚠️  Missing columns: {missing}"); return None
        
        print(f"📄 Unique combinations: {self.df.select(['Facebook_Id', 'post_id']).n_unique():,}")
        
        if self.numeric_columns:
            stats = []
            for row in self.df.select(['Facebook_Id', 'post_id']).unique().head(10).iter_rows(named=True):
                fb_id, post_id = row['Facebook_Id'], row['post_id']
                gd = self.df.filter((pl.col('Facebook_Id') == fb_id) & (pl.col('post_id') == post_id))
                stat = {'Facebook_Id': str(fb_id)[:10] + '...', 'Post_ID': str(post_id)[:10] + '...', 'Rows': gd.height}
                for c in self.numeric_columns[:2]:
                    if c in gd.columns:
                        cd = gd[c].cast(pl.Float64, strict=False).drop_nulls()
                        stat[f'{c}_Mean'] = f"{cd.mean():.2f}" if len(cd) > 0 else "N/A"
                stats.append(stat)
            if stats: self._table(stats, "📊 SAMPLE: First 10 Combinations - Statistics")
        return self.df.group_by(['Facebook_Id', 'post_id'])
    
    def analyze_page_category(self):
        print("\n╔═══════════════════════════════════════════════════╗\n║       PART 2C: GROUPING BY PAGE CATEGORY         ║\n╚═══════════════════════════════════════════════════╝")
        if 'Page Category' not in self.df.columns: print("⚠️  'Page Category' column not found"); return None
        
        print(f"📁 Unique page categories: {self.df['Page Category'].n_unique():,}")
        
        cat_counts = self.df['Page Category'].value_counts().sort('count', descending=True)
        cat_data = [{'Rank': i, 'Category': r['Page Category'], 'Posts': f"{r['count']:,}", 
                    'Percentage': f"{(r['count'] / self.df.height) * 100:.1f}%"} 
                   for i, r in enumerate(cat_counts.iter_rows(named=True), 1)]
        self._table(cat_data, "📊 CATEGORIES BY POST COUNT")
        
        if self.numeric_columns:
            stats = []
            for cat in self.df['Page Category'].unique():
                gd = self.df.filter(pl.col('Page Category') == cat)
                stat = {'Category': cat}
                for c in self.numeric_columns[:4]:
                    if c in gd.columns:
                        cd = gd[c].cast(pl.Float64, strict=False).drop_nulls()
                        stat[f'{c}_Mean'] = f"{cd.mean():.2f}" if len(cd) > 0 else "N/A"
                        stat[f'{c}_Count'] = f"{len(cd)}"
                stats.append(stat)
            if stats: self._table(stats, "📊 STATISTICS BY CATEGORY")
        return self.df.group_by('Page Category')
    
    def compare_groups(self):
        print("\n╔═══════════════════════════════════════════════════╗\n║           PART 2D: COMPARING GROUPS              ║\n╚═══════════════════════════════════════════════════╝")
        
        eng_cols = [c for c in ['Likes', 'Comments', 'Shares', 'Love', 'Wow', 'Haha', 'Sad', 'Angry', 'Care'] if c in self.df.columns]
        
        if 'Page Category' in self.df.columns and eng_cols:
            for c in eng_cols[:3]:
                cat_totals = (self.df.group_by('Page Category')
                             .agg(pl.col(c).cast(pl.Float64, strict=False).sum().alias('total'))
                             .sort('total', descending=True).head(5))
                eng_data = [{'Rank': i, 'Category': r['Page Category'], f'{c}': f"{r['total']:,.0f}"} 
                           for i, r in enumerate(cat_totals.iter_rows(named=True), 1)]
                self._table(eng_data, f"📊 {c} Rankings:")
        
        if 'Facebook_Id' in self.df.columns and 'Likes' in self.df.columns:
            metrics = [c for c in ['Likes', 'Comments', 'Shares'] if c in self.df.columns]
            if metrics:
                page_totals = (self.df.group_by('Facebook_Id')
                              .agg([pl.col(m).cast(pl.Float64, strict=False).sum().alias(m) for m in metrics])
                              .with_columns((sum(pl.col(m) for m in metrics)).alias('Total_Engagement'))
                              .sort('Total_Engagement', descending=True).head(10))
                
                page_data = [{'Rank': i, 'Page_ID': str(r['Facebook_Id'])[:15] + '...', 
                             'Total_Engagement': f"{r['Total_Engagement']:,.0f}",
                             'Likes': f"{r.get('Likes', 0):,.0f}", 'Comments': f"{r.get('Comments', 0):,.0f}", 
                             'Shares': f"{r.get('Shares', 0):,.0f}"} 
                            for i, r in enumerate(page_totals.iter_rows(named=True), 1)]
                self._table(page_data, "🏆 TOP FACEBOOK PAGES BY TOTAL ENGAGEMENT")
    
    def run_analysis(self):
        import time
        total_start = time.time()
        print("\n=== POLARS FB POSTS ANALYSIS START ===")
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
        print(f"[TIMER] TOTAL: {time.time() - total_start:.3f}s\n=== POLARS FB POSTS ANALYSIS END ===\n")

if __name__ == "__main__":
    analyzer = SocialMediaPostsAnalyzer('../period_03/2024_fb_posts_president_scored_anon.csv')
    analyzer.run_analysis()