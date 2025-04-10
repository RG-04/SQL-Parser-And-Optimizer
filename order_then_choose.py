import psycopg2
from itertools import combinations
from collections import defaultdict
import math

# === DB Config ===
DB_CONFIG = {
    'dbname': 'my_db',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432
}

# === Structures ===

class TableStats:
    def __init__(self, name, row_count, ndv, mcv, histograms):
        self.name = name
        self.row_count = row_count
        self.ndv = ndv  # dict: column -> NDV
        self.mcv = mcv  # dict: column -> most common value frequency
        self.histograms = histograms  # dict: column -> histogram

class JoinPredicate:
    def __init__(self, left_table, right_table, left_col, right_col):
        self.left_table = left_table
        self.right_table = right_table
        self.left_col = left_col
        self.right_col = right_col

class Plan:
    def __init__(self, tables, cost, cardinality, plan_repr, left=None, right=None, predicate=None):
        self.tables = frozenset(tables)
        self.cost = cost
        self.cardinality = cardinality
        self.plan_repr = plan_repr
        self.left = left
        self.right = right
        self.predicate = predicate
        self.join_strategy = None  # To be decided later


# === Metadata Extraction ===

def get_table_stats(table_names):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    stats = {}

    for name in table_names:
        cur.execute("SELECT reltuples FROM pg_class WHERE relname = %s", (name,))
        row = cur.fetchone()
        if not row:
            continue
        row_count = row[0]

        ndv, mcv, hists = {}, {}, {}
        cur.execute("SELECT attname, n_distinct, most_common_freqs, histogram_bounds FROM pg_stats WHERE tablename = %s", (name,))
        for attname, n_distinct, mcf, hist in cur.fetchall():
            ndv[attname] = abs(n_distinct) * row_count if n_distinct < 0 else n_distinct
            mcv[attname] = mcf[0] if mcf else 0
            hists[attname] = hist

        stats[name] = TableStats(name, row_count, ndv, mcv, hists)

    cur.close()
    conn.close()
    return stats


# === Histogram Utils ===

def estimate_histogram_overlap(hist1, hist2):
    if not hist1 or not hist2:
        return 0.0
    min1, max1 = hist1[0], hist1[-1]
    min2, max2 = hist2[0], hist2[-1]
    overlap_min = max(min1, min2)
    overlap_max = min(max1, max2)
    range1 = max1 - min1
    overlap_range = overlap_max - overlap_min
    if range1 == 0 or overlap_range <= 0:
        return 0.0
    return max(0.0, min(1.0, overlap_range / range1))


# === Cost Estimators ===

class Estimator:
    def estimate(self, left_stats, right_stats, predicate, left_card, right_card):
        raise NotImplementedError

class FixedSelectivityEstimator(Estimator):
    def estimate(self, left, right, pred, lc, rc):
        return lc * rc * 0.1

class NDVEstimator(Estimator):
    def estimate(self, left, right, pred, lc, rc):
        l_ndv = left.ndv.get(pred.left_col, lc)
        r_ndv = right.ndv.get(pred.right_col, rc)
        return lc * rc / max(l_ndv, r_ndv, 1)

class MCVEstimator(Estimator):
    def estimate(self, left, right, pred, lc, rc):
        mcv1 = left.mcv.get(pred.left_col, 0.01)
        mcv2 = right.mcv.get(pred.right_col, 0.01)
        return lc * rc * mcv1 * mcv2


# === Join Order Optimization via DP ===

def find_predicate(left_plan, right_plan, predicates):
    for p in predicates:
        if p.left_table in left_plan.tables and p.right_table in right_plan.tables or \
           p.right_table in left_plan.tables and p.left_table in right_plan.tables:
            return p
    return None

def dp_join_optimizer(table_stats, predicates, estimators):
    tables = list(table_stats.values())
    dp = {}

    for t in tables:
        dp[frozenset([t.name])] = Plan([t.name], cost=0, cardinality=t.row_count, plan_repr=t.name)

    for size in range(2, len(tables)+1):
        for subset in combinations(tables, size):
            subset_names = frozenset(t.name for t in subset)
            best_plan = None

            for i in range(1, size):
                for left_part in combinations(subset, i):
                    right_part = [t for t in subset if t not in left_part]
                    left_names = frozenset(t.name for t in left_part)
                    right_names = frozenset(t.name for t in right_part)

                    if left_names not in dp or right_names not in dp:
                        continue

                    left_plan = dp[left_names]
                    right_plan = dp[right_names]
                    predicate = find_predicate(left_plan, right_plan, predicates)
                    if not predicate:
                        continue

                    left_stats = table_stats[predicate.left_table]
                    right_stats = table_stats[predicate.right_table]

                    estimates = [est.estimate(left_stats, right_stats, predicate, left_plan.cardinality, right_plan.cardinality) for est in estimators]
                    avg_card = sum(estimates) / len(estimates)
                    cost = left_plan.cost + right_plan.cost + avg_card
                    plan_repr = f"({left_plan.plan_repr} ⋈ {right_plan.plan_repr})"

                    new_plan = Plan(left_plan.tables | right_plan.tables, cost, avg_card, plan_repr,
                                    left=left_plan, right=right_plan, predicate=predicate)

                    if best_plan is None or new_plan.cost < best_plan.cost:
                        best_plan = new_plan

            if best_plan:
                dp[subset_names] = best_plan

    full = frozenset(t.name for t in tables)
    return dp.get(full)


def choose_best_join_strategy(plan, l_card, r_card, overlap):
    if l_card < 1000 or r_card < 1000:
        plan.join_strategy = 'Nested Loop'
    elif overlap > 0.3:
        plan.join_strategy = 'Hash Join'
    else:
        plan.join_strategy = 'Merge Join'



# === Physical Join Strategy Assignment ===

def assign_join_strategies(plan, table_stats):
    if not plan.left or not plan.right:
        return

    assign_join_strategies(plan.left, table_stats)
    assign_join_strategies(plan.right, table_stats)

    l_stat = table_stats[plan.predicate.left_table]
    r_stat = table_stats[plan.predicate.right_table]
    l_card = plan.left.cardinality
    r_card = plan.right.cardinality

    overlap = estimate_histogram_overlap(
        l_stat.histograms.get(plan.predicate.left_col, []),
        r_stat.histograms.get(plan.predicate.right_col, [])
    )

    choose_best_join_strategy(plan, l_card, r_card, overlap)


# === Final Plan Printer ===

def print_execution_plan(plan, indent=0):
    if not plan.left or not plan.right:
        print(" " * indent + f"Scan {list(plan.tables)[0]}")
        return
    print(" " * indent + f"{plan.join_strategy} on {plan.predicate.left_table}.{plan.predicate.left_col} = {plan.predicate.right_table}.{plan.predicate.right_col}")
    print_execution_plan(plan.left, indent + 2)
    print_execution_plan(plan.right, indent + 2)


# === Main ===

if __name__ == "__main__":
    table_names = ['a', 'b', 'c']
    predicates = [
        JoinPredicate('a', 'b', 'id', 'id'),
        JoinPredicate('b', 'c', 'id', 'id')
    ]

    table_stats = get_table_stats(table_names)

    estimators = [FixedSelectivityEstimator(), NDVEstimator(), MCVEstimator()]
    plan = dp_join_optimizer(table_stats, predicates, estimators)
    print("\nInitial Execution Plan:")
    print_execution_plan(plan)
    assign_join_strategies(plan, table_stats)

    print("\nFinal Execution Plan:")
    print_execution_plan(plan)
    print(f"\nTotal Cost: {plan.cost:.2f}, Cardinality: {plan.cardinality:.2f}")
