import csv
import numpy as np
from pathlib import Path
import pandas as pd
import shutil
import win32com.client as win32
import time


def load_inputs(scenario_dir, scenario_csv, policy_dir, policy_csv):
    scen_df = pd.read_csv(scenario_dir / scenario_csv)
    pol_df = pd.read_csv(policy_dir / policy_csv)
    return scen_df, pol_df

def df_to_records(df):
    ids = df.iloc[:, 0].astype(int).to_list()
    params = df.iloc[:, 1:].to_numpy().tolist()
    return list(zip(ids, params))

def build_jobs(scenarios, policies):
    return [
        (scen_id, scen_params, pol_id, pol_params)
        for scen_id, scen_params in scenarios
        for pol_id, pol_params in policies
    ]

def prepare_model(worker_id, models_dir, model_name):
    src = models_dir / model_name
    dst = Path("worker_models").resolve() / f"worker_{worker_id}_{model_name}"
    shutil.copy(src, dst)
    return dst

def worker_run_with_retry(args, max_retries=3, initial_delay=2):
    """
    Wrapper that extracts retry config and passes to worker_run.
    Each individual job gets its own max_retries attempts.
    
    Args:
        args: Tuple with 10 base args + 2 retry config args (max_retries, initial_delay)
    
    Returns:
        Results from successful worker_run execution
    """
    # Extract retry config from args if present (last 2 elements)
    if len(args) >= 12:
        worker_args = args[:10]
        max_retries = int(args[10])
        initial_delay = int(args[11])
        return worker_run(worker_args, max_retries, initial_delay)
    else:
        return worker_run(args, max_retries, initial_delay)

def worker_run(args, max_retries=3, initial_delay=2):
    (worker_id, jobs, models_dir, model_name, scenario_input_range, policy_input_range, 
     scalar_output_range, ps_output_range, vba_macro, mode) = args

    model_path = prepare_model(worker_id, models_dir, model_name)

    excel = win32.DispatchEx("Excel.Application")
    excel.Visible = False
    excel.DisplayAlerts = False
    excel.ScreenUpdating = False
    excel.EnableEvents = False

    try:
        wb = excel.Workbooks.Open(str(model_path))
        wsInputs = wb.Worksheets("Inputs")
        wsScens = wb.Worksheets("Scens")

        scen_range = wsInputs.Range(scenario_input_range)
        pol_range  = wsInputs.Range(policy_input_range)
        scalar_range = wsInputs.Range(scalar_output_range)
        ps_range = wsScens.Range(ps_output_range)

        scen_n = scen_range.Columns.Count
        pol_n  = pol_range.Columns.Count
        
        results = []

        current_scen_id = None

        for scen_id, scen_params, pol_id, pol_params in jobs:
            # ===== Process this job with retries =====
            job_result = None
            last_error = None
            
            for attempt in range(max_retries):
                try:
                    # Change scenario only when needed
                    if scen_id != current_scen_id:
                        scen_range.Value = [scen_params[:scen_n]]
                        current_scen_id = scen_id

                    # Write policy
                    pol_range.Value = [pol_params[:pol_n]]

                    # Run VBA
                    excel.Run(vba_macro)

                    # Read outputs
                    scalar_vals = scalar_range.Value
                    pvfp, pvfprem, pm, irr = scalar_vals[0]

                    if mode == "per_policy":
                        job_result = [scen_id, pol_id, pvfp, pm, irr]
                    else:
                        ps = [row[0] for row in ps_range.Value]  # expect 1081x1
                        job_result = [scen_id, pol_id, pvfp, pvfprem, ps]
                    
                    print(f"Worker {worker_id}: Completed Scenario {scen_id}, Policy {pol_id}")
                    break  # Success - exit retry loop
                    
                except Exception as e:
                    last_error = e
                    # Check if it's a COM error that's worth retrying
                    is_com_error = "com_error" in str(type(e)).lower() or "Call was rejected" in str(e)
                    
                    if attempt < max_retries - 1 and is_com_error:
                        delay = initial_delay * (2 ** attempt)  # Exponential backoff
                        print(f"Worker {worker_id}: Scenario {scen_id}, Policy {pol_id} - "
                              f"COM error on attempt {attempt + 1}/{max_retries}. "
                              f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        # Last attempt or non-COM error - re-raise
                        if is_com_error:
                            print(f"Worker {worker_id}: Scenario {scen_id}, Policy {pol_id} - "
                                  f"COM error on final attempt {attempt + 1}/{max_retries}. Giving up.")
                        raise

            if job_result is not None:
                results.append(job_result)

        return results

    finally:
        # Ensure Excel is closed even if interrupted or error occurs
        try:
            wb.Close(SaveChanges=False)
        except Exception:
            pass
        try:
            excel.Quit()
        except Exception:
            pass

def chunk_jobs(jobs, n):
    return [jobs[i::n] for i in range(n)]

def write_per_policy(results, outputs_dir, run_name):
    out_dir = outputs_dir / run_name
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / f"{run_name}_results_per_pol.csv"

    with open(out_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ScenarioID", "PolicyID", "PVFP", "PM", "IRR"])
        for r in results:
            writer.writerow(r)

def write_portfolio(results, outputs_dir, run_name):
    """
    results: list of
        [ScenarioID, PolicyID, PVFP, PVFPrem, ProfitSignature(list[float])]
    """

    # Root output folder: outputs/test_1
    out_root = outputs_dir / run_name
    out_root.mkdir(parents=True, exist_ok=True)

    # Build DataFrame
    df = pd.DataFrame(
        results,
        columns=["ScenarioID", "PolicyID", "PVFP", "PVFPrem", "ProfitSignature"]
    )

    summary_cols = {}

    # Process scenario by scenario
    for scen_id, g in df.groupby("ScenarioID", sort=True):
        scen_dir = out_root / f"scenario_{scen_id}"
        scen_dir.mkdir(exist_ok=True)

        # --- Write per-policy portfolio results ---
        g.to_csv(
            scen_dir / f"{run_name}_scenario_{scen_id}_portfolio_results.csv",
            index=False
        )

        # --- Aggregate scalars ---
        total_pvfp = g["PVFP"].sum()
        total_pvfprem = g["PVFPrem"].sum()

        # --- Aggregate profit signature (NumPy, element-wise) ---
        # Shape: (n_policies, 1081)
        ps_matrix = np.vstack(g["ProfitSignature"].values)

        # Shape: (1081,)
        ps_sum = ps_matrix.sum(axis=0)

        # --- Build summary column ---
        # Row 1: PVFP
        # Row 2: PVFPrem
        # Row 3..1083: Profit Signature (1081 rows)
        summary_cols[f"Scenario_{scen_id}"] = (
            [total_pvfp, total_pvfprem] + ps_sum.tolist()
        )

    # --- Write portfolio summary ---
    summary_df = pd.DataFrame(summary_cols)

    summary_df.to_csv(
        out_root / f"{run_name}_portfolio_results_summary.csv",
        index=False
    )