"""
Live demo dashboard.

Serves a browser page that polls whichever controller's CSV log is
currently being written and renders live-updating charts -- so the demo
shows a clean dashboard instead of scrolling terminal text. No changes
to producer.py, worker.py, or any controller are needed; this just reads
the same CSV files they already write.

Usage:
    python dashboard.py reactive_scaling_log.csv
    python dashboard.py predictive_scaling_log.csv
    (run it pointed at whichever strategy you're currently demoing)
"""

import csv
import sys

from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

LOG_FILE = sys.argv[1] if len(sys.argv) > 1 else "reactive_scaling_log.csv"

PAGE = """
<!doctype html>
<html>
<head>
<title>Adaptive Resource Management -- Live Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  body { font-family: -apple-system, Arial, sans-serif; background:#f8fafc; color:#1e293b; margin:0; padding:28px; }
  h1 { font-size: 22px; margin-bottom: 4px; color:#0f172a; }
  .subtitle { color:#64748b; margin-bottom:24px; font-size:14px; }
  .card { background:#ffffff; border-radius:12px; padding:20px; margin-bottom:20px; border:1px solid #e2e8f0; box-shadow:0 1px 3px rgba(0,0,0,0.04); }
  .stats { display:flex; gap:20px; margin-bottom:20px; }
  .stat { background:#ffffff; border-radius:12px; padding:16px 24px; flex:1; border:1px solid #e2e8f0; box-shadow:0 1px 3px rgba(0,0,0,0.04); }
  .stat .label { color:#64748b; font-size:12px; text-transform:uppercase; letter-spacing:0.05em; }
  .stat .value { font-size:30px; font-weight:700; margin-top:4px; color:#0f172a; }
</style>
</head>
<body>
<h1>Adaptive Resource Management -- Live Dashboard</h1>
<div class="subtitle">Watching: {{ log_file }}</div>

<div class="stats">
  <div class="stat"><div class="label">Current lag</div><div class="value" id="curLag">--</div></div>
  <div class="stat"><div class="label">Active workers</div><div class="value" id="curWorkers">--</div></div>
  <div class="stat"><div class="label">Elapsed</div><div class="value" id="curTime">--</div></div>
</div>

<div class="card"><canvas id="lagChart" height="90"></canvas></div>
<div class="card"><canvas id="workerChart" height="55"></canvas></div>

<script>
const lagCtx = document.getElementById('lagChart').getContext('2d');
const workerCtx = document.getElementById('workerChart').getContext('2d');

const lagChart = new Chart(lagCtx, {
  type: 'line',
  data: { labels: [], datasets: [{ label: 'Consumer lag', data: [], borderColor: '#ea580c', backgroundColor: 'rgba(234,88,12,0.08)', fill:true, tension: 0.2, pointRadius: 0 }] },
  options: { responsive:true, animation:false,
    scales: { x: { ticks: { color:'#64748b' }, grid:{ color:'#e2e8f0' } }, y: { ticks: { color:'#64748b' }, grid:{ color:'#e2e8f0' }, beginAtZero:true } },
    plugins:{ legend:{ labels:{ color:'#1e293b' } } } }
});

const workerChart = new Chart(workerCtx, {
  type: 'line',
  data: { labels: [], datasets: [{ label: 'Active workers', data: [], borderColor: '#0284c7', backgroundColor: 'rgba(2,132,199,0.08)', fill:true, stepped:true, pointRadius:0 }] },
  options: { responsive:true, animation:false,
    scales: { x:{ ticks:{ color:'#64748b' }, grid:{ color:'#e2e8f0' } }, y: { ticks: { color:'#64748b', stepSize:1 }, grid:{ color:'#e2e8f0' }, min:0 } },
    plugins:{ legend:{ labels:{ color:'#1e293b' } } } }
});

async function poll() {
  try {
    const res = await fetch('/data');
    const rows = await res.json();
    if (rows.length === 0) return;

    const labels = rows.map(r => r.elapsed_seconds);
    lagChart.data.labels = labels;
    lagChart.data.datasets[0].data = rows.map(r => r.total_lag);
    lagChart.update();

    workerChart.data.labels = labels;
    workerChart.data.datasets[0].data = rows.map(r => r.num_workers);
    workerChart.update();

    const last = rows[rows.length - 1];
    document.getElementById('curLag').innerText = last.total_lag;
    document.getElementById('curWorkers').innerText = last.num_workers;
    document.getElementById('curTime').innerText = last.elapsed_seconds + 's';
  } catch (e) {
    console.error(e);
  }
}
setInterval(poll, 1500);
poll();
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(PAGE, log_file=LOG_FILE)


@app.route("/data")
def data():
    try:
        with open(LOG_FILE) as f:
            rows = list(csv.DictReader(f))
        return jsonify(rows)
    except FileNotFoundError:
        return jsonify([])


if __name__ == "__main__":
    print(f"Dashboard running at http://localhost:5000  (watching {LOG_FILE})")
    app.run(port=5000, debug=False)
