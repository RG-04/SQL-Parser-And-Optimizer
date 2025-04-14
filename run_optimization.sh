cd final_parser/
make test
cd ..
python3 predicate_pushdown_trial.py
python3 join_optimization.py
python3 subsequence.py -i optimized_out.json -o subseq_plan.json --pretty