import psycopg2
import math
from itertools import combinations
from collections import defaultdict


# === Configuration ===
DB_CONFIG = {
    'dbname': 'my_db',
    'user': 'postgres',
    'password': 'password',
    'host': 'localhost',
    'port': 5432
}


# === Metadata and Structures ===

class TableStats:
    def __init__(self, name, row_count, has_index):
        self.name = name
        self.row_count = row_count
        self.has_index = has_index

class JoinPredicate:
    def __init__(self, left_table, right_table, left_col, right_col):
        self.left_table = left_table
        self.right_table = right_table
        self.left_col = left_col
        self.right_col = right_col

class Plan:
    def __init__(self, tables, cost, cardinality, plan_repr):
        self.tables = frozenset(tables)
        self.cost = cost
        self.cardinality = cardinality
        self.plan_repr = plan_repr


# === Metadata Extraction ===

def get_table_stats(table_names):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    stats = {}
    for name in table_names:
        cur.execute("""
            SELECT reltuples, relhasindex
            FROM pg_class
            WHERE relname = %s
        """, (name,))
        row = cur.fetchone()
        if row:
            stats[name] = TableStats(name, row[0], row[1])
        else:
            print(f"Warning: Table {name} not found in pg_class.")
    cur.close()
    conn.close()
    return stats

def get_column_stats(table_columns):
    """
    table_columns = [('a', 'a_id'), ('b', 'b_id'), ...]
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    stats = {}

    for table, column in table_columns:
        cur.execute("""
            SELECT n_distinct
            FROM pg_stats
            WHERE tablename = %s AND attname = %s
        """, (table, column))
        row = cur.fetchone()
        if row:
            stats[(table, column)] = abs(row[0])  # n_distinct can be negative as a fraction
        else:
            print(f"Warning: No stats for {table}.{column}")
            stats[(table, column)] = 1000  # fallback

    cur.close()
    conn.close()
    return stats



# === Cost Models ===

class CostModel:
    def estimate_join(self, left: Plan, right: Plan, predicate: JoinPredicate) -> Plan:
        raise NotImplementedError

class NestedLoopCost(CostModel):
    def estimate_join(self, left: Plan, right: Plan, predicate: JoinPredicate) -> Plan:
        selectivity = 0.1  # naive assumption
        cardinality = left.cardinality * right.cardinality * selectivity
        cost = left.cost + right.cost + (left.cardinality * right.cardinality)
        plan_repr = f"({left.plan_repr} ⋈ {right.plan_repr})"
        return Plan(tables=left.tables | right.tables, cost=cost, cardinality=cardinality, plan_repr=plan_repr)
    
class HashJoinCost(CostModel):
    def estimate_join(self, left: Plan, right: Plan, predicate: JoinPredicate) -> Plan:
        selectivity = 0.1  # naive
        cardinality = left.cardinality * right.cardinality * selectivity
        # Assume hashing cost is proportional to size, plus small overhead
        cost = left.cost + right.cost + left.cardinality + right.cardinality
        plan_repr = f"(HashJoin {left.plan_repr} ⨝ {right.plan_repr})"
        return Plan(tables=left.tables | right.tables, cost=cost, cardinality=cardinality, plan_repr=plan_repr)


class IndexNestedLoopCost(CostModel):
    def __init__(self, table_stats_dict):
        self.table_stats = table_stats_dict

    def estimate_join(self, left: Plan, right: Plan, predicate: JoinPredicate) -> Plan:
        # Use index only if right table has one
        has_index = self.table_stats[predicate.right_table].has_index
        selectivity = 0.1
        cardinality = left.cardinality * right.cardinality * selectivity
        if has_index:
            cost = left.cost + right.cost + left.cardinality * 10  # index lookup cost
            plan_repr = f"(IndexNL {left.plan_repr} ⨝ {right.plan_repr})"
        else:
            cost = float('inf')
            plan_repr = "InvalidPlan"
        return Plan(tables=left.tables | right.tables, cost=cost, cardinality=cardinality, plan_repr=plan_repr)


class SortMergeJoinCost(CostModel):
    def estimate_join(self, left: Plan, right: Plan, predicate: JoinPredicate) -> Plan:
        selectivity = 0.1
        cardinality = left.cardinality * right.cardinality * selectivity
        # Assume sorting costs n*log(n)
        cost = (
            left.cost + right.cost +
            left.cardinality * (math.log(left.cardinality + 1)) +
            right.cardinality * (math.log(right.cardinality + 1))
        )
        plan_repr = f"(SortMerge {left.plan_repr} ⨝ {right.plan_repr})"
        return Plan(tables=left.tables | right.tables, cost=cost, cardinality=cardinality, plan_repr=plan_repr)


class PredicateAwareHashJoinCost(CostModel):
    def __init__(self, col_stats):
        self.col_stats = col_stats

    def estimate_selectivity(self, predicate: JoinPredicate):
        left_ndv = self.col_stats.get((predicate.left_table, predicate.left_col), 1000)
        right_ndv = self.col_stats.get((predicate.right_table, predicate.right_col), 1000)
        return 1 / max(left_ndv, right_ndv)

    def estimate_join(self, left: Plan, right: Plan, predicate: JoinPredicate) -> Plan:
        selectivity = self.estimate_selectivity(predicate)
        cardinality = left.cardinality * right.cardinality * selectivity
        cost = left.cost + right.cost + left.cardinality + right.cardinality
        plan_repr = f"(SmartHashJoin {left.plan_repr} ⨝ {right.plan_repr})"
        return Plan(tables=left.tables | right.tables, cost=cost, cardinality=cardinality, plan_repr=plan_repr)
    

# === Join Order Optimization (DP) ===

def find_predicate_between(left_plan, right_plan, predicates):
    for p in predicates:
        if (p.left_table in left_plan.tables and p.right_table in right_plan.tables) or \
           (p.right_table in left_plan.tables and p.left_table in right_plan.tables):
            return p
    return None

def dp_join_optimizer(table_stats_dict, predicates, cost_model):
    tables = list(table_stats_dict.values())
    dp = {}

    # Base cases
    for t in tables:
        dp[frozenset([t.name])] = Plan([t.name], cost=0, cardinality=t.row_count, plan_repr=t.name)

    # DP joins
    for size in range(2, len(tables) + 1):
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

                    predicate = find_predicate_between(left_plan, right_plan, predicates)
                    if not predicate:
                        continue

                    new_plan = cost_model.estimate_join(left_plan, right_plan, predicate)
                    if best_plan is None or new_plan.cost < best_plan.cost:
                        best_plan = new_plan

            if best_plan:
                dp[subset_names] = best_plan

    full_set = frozenset(t.name for t in tables)
    return dp.get(full_set)


# === Example ===

if __name__ == '__main__':
    # List your table names
    table_names = ['a', 'b', 'c']  # Must exist in your PostgreSQL database
    stats = get_table_stats(table_names)

    # Define join predicates (adjust column names to match your schema)
    predicates = [
        JoinPredicate("a", "b", "a_id", "b_id"),
        JoinPredicate("b", "c", "b_id", "c_id")
    ]

    cost_models = {
        "nested_loop": NestedLoopCost(),
        "hash_join": HashJoinCost(),
        "index_nested_loop": IndexNestedLoopCost(stats),
        "sort_merge": SortMergeJoinCost()
    }
    for name, model in cost_models.items():
        print(f"Using {name} cost model.")
        final_plan = dp_join_optimizer(stats, predicates, model)

        if final_plan:
            print("\n✅ Best Join Plan:")
            print("Plan:", final_plan.plan_repr)
            print("Cost:", final_plan.cost)
            print("Cardinality:", final_plan.cardinality)
        else:
            print("❌ No valid join order found.")

    column_stats = get_column_stats([("a", "a_id"), ("b", "b_id"), ("c", "c_id")])
    smart_hash_model = PredicateAwareHashJoinCost(column_stats)
    print("\nUsing Predicate Aware Hash Join cost model.")
    final_plan = dp_join_optimizer(stats, predicates, smart_hash_model)
    if final_plan:
        print("\n✅ Best Join Plan:")
        print("Plan:", final_plan.plan_repr)
        print("Cost:", final_plan.cost)
        print("Cardinality:", final_plan.cardinality)
    else:
        print("❌ No valid join order found.")
