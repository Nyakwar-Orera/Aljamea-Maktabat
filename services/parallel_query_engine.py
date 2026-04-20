# services/parallel_query_engine.py
import logging
from concurrent.futures import ThreadPoolExecutor, wait
from typing import List, Dict, Any, Callable
from config import Config

logger = logging.getLogger(__name__)

def execute_parallel_queries(target_codes: List[str], 
                            query_func: Callable, 
                            timeout: int = 8, 
                            *args, **kwargs) -> Dict[str, Any]:
    """
    Generic parallel query engine for multi-campus data retrieval.
    """
    results: Dict[str, Any] = {}
    
    with ThreadPoolExecutor(max_workers=len(target_codes)) as executor:
        future_map = {
            executor.submit(query_func, code, *args, **kwargs): code
            for code in target_codes
        }
        done, not_done = wait(future_map.keys(), timeout=timeout)

        for future in done:
            code = future_map[future]
            try:
                results[code] = future.result()
            except Exception as e:
                logger.error(f"Parallel query failed for {code}: {e}")
                results[code] = {"status": "error", "error": str(e)}

        for future in not_done:
            code = future_map[future]
            logger.warning(f"Parallel query timed out for {code}")
            results[code] = {"status": "timeout"}
            
    return results
