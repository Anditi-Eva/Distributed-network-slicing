import requests
import time
import random

MASTER_URL = "http://127.0.0.1:5000/api/slice/request"

def run_benchmark(num_requests=20):
    print(f"=== Starting Workload Simulation ({num_requests} requests) ===")
    
    start_bench_time = time.time()
    successful_requests = 0
    failed_requests = 0
    latencies = []

    for i in range(1, num_requests + 1):
        req_start = time.time()
        
        # Introduce artificial packet drop chance (5%) for testing
        simulate_drop = random.random() < 0.05
        
        payload = {
            "slice_type": random.choice(["eMBB", "URLLC", "mMTC"]),
            "cpu": 0.2,
            "memory": 128,
            "sequence_id": i,
            "simulate_drop": simulate_drop
        }

        try:
            res = requests.post(MASTER_URL, json=payload, timeout=2)
            rtt = (time.time() - req_start) * 1000  # Latency in ms
            
            if res.status_code == 201:
                successful_requests += 1
                latencies.append(rtt)
                print(f"[Req {i:02d}] Delivered - Latency: {rtt:.2f} ms")
            else:
                failed_requests += 1
                print(f"[Req {i:02d}] Failed ({res.status_code})")
        except requests.exceptions.RequestException:
            failed_requests += 1
            print(f"[Req {i:02d}] Connection Timeout / Dropped")

        time.sleep(0.05)  # Short pause between requests

    total_duration = time.time() - start_bench_time
    throughput = successful_requests / total_duration  # Requests per second

    print("\n================ BENCHMARK RESULTS ================")
    print(f"Total Duration:        {total_duration:.2f} seconds")
    print(f"Throughput:            {throughput:.2f} requests/sec")
    print(f"Successful Requests:   {successful_requests}")
    print(f"Failed / Lost Packets: {failed_requests}")
    print(f"Packet Loss Rate:      {(failed_requests / num_requests) * 100:.2f}%")
    
    if latencies:
        avg_lat = sum(latencies) / len(latencies)
        jitter = sum(abs(latencies[j] - latencies[j-1]) for j in range(1, len(latencies))) / max(1, len(latencies)-1)
        print(f"Average Latency:       {avg_lat:.2f} ms")
        print(f"Jitter:                {jitter:.2f} ms")
    print("===================================================\n")

if __name__ == '__main__':
    run_benchmark(30)
