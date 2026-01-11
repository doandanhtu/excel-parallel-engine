import yaml
import time
from datetime import datetime
from pathlib import Path
from multiprocessing import Pool
from utils import df_to_records, load_inputs, build_jobs, chunk_jobs, worker_run_with_retry, write_per_policy, write_portfolio


def load_config(config_file="config.yaml"):
    with open(config_file) as f:
        config = yaml.safe_load(f)
    
    # Convert path strings to Path objects
    config["models_dir"] = Path(config["models_dir"])
    config["scenario_dir"] = Path(config["scenario_dir"])
    config["policy_dir"] = Path(config["policy_dir"])
    config["outputs_dir"] = Path(config["outputs_dir"])
    
    return config


def main():
    start_time = time.time()
    now = datetime.now().strftime("%I:%M %p %d %b %Y").lower()

    cfg = load_config()
    
    scen_df, pol_df = load_inputs(
        cfg["scenario_dir"],
        cfg["scenario_csv"],
        cfg["policy_dir"],
        cfg["policy_csv"]
    )
    
    scenarios = df_to_records(scen_df)
    policies = df_to_records(pol_df)

    jobs = build_jobs(scenarios, policies)
    chunks = chunk_jobs(jobs, cfg["num_workers"])
    
    num_jobs = len(jobs)
    
    print(f"\nStarting engine run with {num_jobs} jobs ({len(scenarios)} scenarios × {len(policies)} policies) at {now}")
    print(f"Using {cfg['num_workers']} workers\n")
    
    # Prepare worker args with config values
    worker_args = [
        (i, chunk, cfg["models_dir"], cfg["model_name"], 
         cfg["scenario_input_range"], cfg["policy_input_range"],
         cfg["scalar_output_range"], cfg["ps_output_range"],
         cfg["vba_macro"], cfg["mode"], 
         cfg.get("max_retries", 3), cfg.get("initial_delay", 2))
        for i, chunk in enumerate(chunks)
    ]
    
    try:
        with Pool(cfg["num_workers"]) as pool:
            results = pool.map(worker_run_with_retry, worker_args)
        
        flat_results = [r for worker_res in results for r in worker_res]
        
        # Sort by ScenarioID (column 0) then PolicyID (column 1)
        flat_results.sort(key=lambda x: (x[0], x[1]))
        
        if cfg["mode"] == "per_policy":
            write_per_policy(flat_results, cfg["outputs_dir"], cfg["run_name"])
        else:
            write_portfolio(flat_results, cfg["outputs_dir"], cfg["run_name"])
        
        # Calculate and display timing statistics
        total_time = time.time() - start_time
        avg_time_per_job = total_time / num_jobs
        
        print(f"\n{'='*60}")
        print(f"Execution Complete")
        print(f"{'='*60}")
        print(f"Total time:           {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        print(f"Number of jobs:       {num_jobs}")
        print(f"Average time/job:     {avg_time_per_job:.4f} seconds")
        print(f"{'='*60}\n")
    
    except KeyboardInterrupt:
        print("\n" + "="*60)
        print("Interrupted by user (Ctrl+C)")
        print("Shutting down workers and Excel instances...")
        print("="*60)
        total_time = time.time() - start_time
        print(f"Elapsed time before interruption: {total_time:.2f} seconds ({total_time/60:.2f} minutes)\n")
        raise


if __name__ == "__main__":
    main()
