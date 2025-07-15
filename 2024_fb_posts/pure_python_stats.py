import csv
import statistics
from collections import defaultdict, Counter

class SocialMediaPostsAnalyzer:
    def __init__(self, csv_file_path: str):
        self.csv_file_path = csv_file_path
        self.data, self.headers, self.numeric_columns, self.text_columns = [], [], [], []
        
    def _to_num(self, v): 
        try: return float(v) if v and '.' in v else int(v) if v else None
        except: return None
    
    def _print_table(self, data, title=""):
        if not data: return
        if title: print(f"\n{title}")
        keys = list(set().union(*(row.keys() for row in data)))
        widths = {k: max(len(str(k)), max(len(str(row.get(k, ''))) for row in data)) for k in keys}
        sep = "+" + "+".join("-" * (widths[k] + 2) for k in keys) + "+"
        print(sep)
        print("| " + " | ".join(str(k).ljust(widths[k]) for k in keys) + " |")
        print(sep)
        for row in data: print("| " + " | ".join(str(row.get(k, '')).ljust(widths[k]) for k in keys) + " |")
        print(sep)
    
    def load_and_clean_data(self):
        print("╔═══════════════════════════════════════════════════╗\n║           LOADING AND CLEANING DATA              ║\n╚═══════════════════════════════════════════════════╝")
        with open(self.csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            self.headers, self.data = reader.fieldnames, list(reader)
        
        print(f"Dataset: {len(self.data):,} rows × {len(self.headers)} columns")
        nulls = sum(1 for h in self.headers if sum(1 for row in self.data if not row[h]) > 0)
        print(f"Null values in {nulls} columns")
        
        for h in self.headers:
            samples = [row[h] for row in self.data[:100] if row[h]]
            numeric_ratio = sum(1 for v in samples if self._to_num(v) is not None) / len(samples) if samples else 0
            (self.numeric_columns if numeric_ratio > 0.8 else self.text_columns).append(h)
        
        print(f"Numeric: {len(self.numeric_columns)} | Text: {len(self.text_columns)}")
        return self.data
    
    def compute_overall_statistics(self):
        print("\n╔════════════════════════════════════════════════════════════╗\n║                PART 1: OVERALL DATASET ANALYSIS           ║\n╚════════════════════════════════════════════════════════════╝")
        
        if self.numeric_columns:
            stats = []
            for col in self.numeric_columns:
                vals = [self._to_num(row[col]) for row in self.data if self._to_num(row[col]) is not None]
                if vals:
                    stats.append({'Column': col, 'Count': f"{len(vals):,}", 'Mean': f"{statistics.mean(vals):.2f}",
                                'Min': f"{min(vals)}", 'Max': f"{max(vals)}", 'Std': f"{statistics.stdev(vals) if len(vals) > 1 else 0:.2f}"})
                else:
                    stats.append({'Column': col, 'Count': "0", 'Mean': "N/A", 'Min': "N/A", 'Max': "N/A", 'Std': "N/A"})
            self._print_table(stats, "📊 NUMERIC COLUMNS STATISTICS")
        
        if self.text_columns:
            stats = []
            for col in self.text_columns:
                vals = [row[col] for row in self.data if row[col]]
                if vals:
                    top_val, top_cnt = Counter(vals).most_common(1)[0]
                    stats.append({'Column': col, 'Count': f"{len(vals):,}", 'Unique': f"{len(set(vals)):,}",
                                'Top Value': str(top_val)[:20] + ('...' if len(str(top_val)) > 20 else ''), 'Top Count': f"{top_cnt:,}"})
                else:
                    stats.append({'Column': col, 'Count': "0", 'Unique': "0", 'Top Value': "N/A", 'Top Count': "0"})
            self._print_table(stats, "📝 TEXT COLUMNS STATISTICS")
    
    def analyze_by_facebook_id(self):
        print("\n╔═══════════════════════════════════════════════════╗\n║         PART 2A: GROUPING BY FACEBOOK_ID         ║\n╚═══════════════════════════════════════════════════╝")
        if 'Facebook_Id' not in self.headers: print("⚠️  'Facebook_Id' column not found"); return None
        
        groups = defaultdict(list)
        for row in self.data: groups[row['Facebook_Id']].append(row)
        print(f"📄 Unique Facebook pages: {len(groups):,}")
        
        if self.numeric_columns:
            stats = []
            for fb_id, group_data in list(groups.items())[:10]:
                stat = {'Facebook_Id': str(fb_id)[:15] + '...'}
                for col in self.numeric_columns[:3]:
                    vals = [self._to_num(row[col]) for row in group_data if self._to_num(row[col]) is not None]
                    stat[f'{col}_Mean'] = f"{statistics.mean(vals):.2f}" if vals else "N/A"
                    stat[f'{col}_Count'] = f"{len(vals)}"
                stats.append(stat)
            if stats: self._print_table(stats, "📊 SAMPLE: Top 10 Pages - Numeric Statistics")
        return groups
    
    def analyze_by_facebook_and_post_id(self):
        print("\n╔═══════════════════════════════════════════════════════════╗\n║       PART 2B: GROUPING BY FACEBOOK_ID + POST_ID         ║\n╚═══════════════════════════════════════════════════════════╝")
        missing = [col for col in ['Facebook_Id', 'post_id'] if col not in self.headers]
        if missing: print(f"⚠️  Missing columns: {missing}"); return None
        
        groups = defaultdict(list)
        for row in self.data: groups[(row['Facebook_Id'], row['post_id'])].append(row)
        print(f"📄 Unique combinations: {len(groups):,}")
        
        if self.numeric_columns:
            stats = []
            for (fb_id, post_id), group_data in list(groups.items())[:10]:
                stat = {'Facebook_Id': str(fb_id)[:10] + '...', 'Post_ID': str(post_id)[:10] + '...', 'Rows': len(group_data)}
                for col in self.numeric_columns[:2]:
                    vals = [self._to_num(row[col]) for row in group_data if self._to_num(row[col]) is not None]
                    stat[f'{col}_Mean'] = f"{statistics.mean(vals):.2f}" if vals else "N/A"
                stats.append(stat)
            if stats: self._print_table(stats, "📊 SAMPLE: First 10 Combinations - Statistics")
        return groups
    
    def analyze_by_page_category(self):
        print("\n╔═══════════════════════════════════════════════════╗\n║       PART 2C: GROUPING BY PAGE CATEGORY         ║\n╚═══════════════════════════════════════════════════╝")
        if 'Page Category' not in self.headers: print("⚠️  'Page Category' column not found"); return None
        
        groups = defaultdict(list)
        for row in self.data: groups[row['Page Category']].append(row)
        print(f"📁 Unique page categories: {len(groups):,}")
        
        cat_counts = sorted([(cat, len(data)) for cat, data in groups.items()], key=lambda x: x[1], reverse=True)
        cat_data = [{'Rank': i, 'Category': cat, 'Posts': f"{cnt:,}", 'Percentage': f"{(cnt / len(self.data)) * 100:.1f}%"} 
                   for i, (cat, cnt) in enumerate(cat_counts, 1)]
        self._print_table(cat_data, "📊 CATEGORIES BY POST COUNT")
        
        if self.numeric_columns:
            stats = []
            for cat, group_data in groups.items():
                stat = {'Category': cat}
                for col in self.numeric_columns[:4]:
                    vals = [self._to_num(row[col]) for row in group_data if self._to_num(row[col]) is not None]
                    stat[f'{col}_Mean'] = f"{statistics.mean(vals):.2f}" if vals else "N/A"
                    stat[f'{col}_Count'] = f"{len(vals)}"
                stats.append(stat)
            if stats: self._print_table(stats, "📊 STATISTICS BY CATEGORY")
        return groups
    
    def compare_groups_analysis(self):
        print("\n╔═══════════════════════════════════════════════════╗\n║           PART 2D: COMPARING GROUPS              ║\n╚═══════════════════════════════════════════════════╝")
        
        eng_cols = [c for c in ['Likes', 'Comments', 'Shares', 'Love', 'Wow', 'Haha', 'Sad', 'Angry', 'Care'] if c in self.headers]
        
        if 'Page Category' in self.headers:
            cat_groups = defaultdict(list)
            for row in self.data: cat_groups[row['Page Category']].append(row)
            
            for col in eng_cols[:3]:
                cat_totals = {cat: sum(self._to_num(row[col]) or 0 for row in data) for cat, data in cat_groups.items()}
                sorted_cats = sorted(cat_totals.items(), key=lambda x: x[1], reverse=True)
                eng_data = [{'Rank': i, 'Category': cat, f'{col}': f"{eng:,.0f}"} for i, (cat, eng) in enumerate(sorted_cats[:5], 1)]
                self._print_table(eng_data, f"📊 {col} Rankings:")
        
        if 'Facebook_Id' in self.headers and 'Likes' in self.headers:
            page_groups = defaultdict(list)
            for row in self.data: page_groups[row['Facebook_Id']].append(row)
            
            metrics = [c for c in ['Likes', 'Comments', 'Shares'] if c in self.headers]
            if metrics:
                page_totals = {}
                for page_id, data in page_groups.items():
                    totals = {m: sum(self._to_num(row[m]) or 0 for row in data) for m in metrics}
                    page_totals[page_id] = {**totals, 'Total_Engagement': sum(totals.values())}
                
                sorted_pages = sorted(page_totals.items(), key=lambda x: x[1]['Total_Engagement'], reverse=True)
                page_data = [{'Rank': i, 'Page_ID': str(pid)[:15] + '...', 'Total_Engagement': f"{tots['Total_Engagement']:,.0f}",
                            'Likes': f"{tots.get('Likes', 0):,.0f}", 'Comments': f"{tots.get('Comments', 0):,.0f}", 
                            'Shares': f"{tots.get('Shares', 0):,.0f}"} for i, (pid, tots) in enumerate(sorted_pages[:10], 1)]
                self._print_table(page_data, "🏆 TOP FACEBOOK PAGES BY TOTAL ENGAGEMENT")
    
    def run_complete_analysis(self):
        import time
        total_start = time.time()
        print("\n=== PURE PYTHON FB POSTS ANALYSIS START ===")
        step_start = time.time()
        self.load_and_clean_data()
        print(f"[TIMER] load_and_clean_data: {time.time() - step_start:.3f}s")
        step_start = time.time()
        self.compute_overall_statistics()
        print(f"[TIMER] compute_overall_statistics: {time.time() - step_start:.3f}s")
        step_start = time.time()
        self.analyze_by_facebook_id()
        print(f"[TIMER] analyze_by_facebook_id: {time.time() - step_start:.3f}s")
        step_start = time.time()
        self.analyze_by_facebook_and_post_id()
        print(f"[TIMER] analyze_by_facebook_and_post_id: {time.time() - step_start:.3f}s")
        step_start = time.time()
        self.analyze_by_page_category()
        print(f"[TIMER] analyze_by_page_category: {time.time() - step_start:.3f}s")
        step_start = time.time()
        self.compare_groups_analysis()
        print(f"[TIMER] compare_groups_analysis: {time.time() - step_start:.3f}s")
        print(f"[TIMER] TOTAL: {time.time() - total_start:.3f}s\n=== PURE PYTHON FB POSTS ANALYSIS END ===\n")

if __name__ == "__main__":
    analyzer = SocialMediaPostsAnalyzer('../period_03/2024_fb_posts_president_scored_anon.csv')
    analyzer.run_complete_analysis()