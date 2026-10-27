// Herd routing dashboard: polls the admin API and makes each routing
// decision legible. No build step, no dependencies.
(function () {
  'use strict';

  const POLL_MS = 1000;
  const WINDOW = 500; // decisions kept client-side for percentiles and the chart
  const CHART_MS = 2 * 60 * 1000;
  const COLORS = ['#2dd4bf', '#60a5fa', '#f472b6', '#a78bfa', '#f2b544', '#34d399', '#fb923c', '#e879f9', '#38bdf8', '#facc15'];

  const seen = new Map(); // id -> decision
  const order = []; // ids in arrival order, for trimming
  const colorOf = new Map();
  let openId = null;

  const $ = (id) => document.getElementById(id);

  function color(backend) {
    if (!colorOf.has(backend)) colorOf.set(backend, COLORS[colorOf.size % COLORS.length]);
    return colorOf.get(backend);
  }

  function pct(arr, p) {
    if (!arr.length) return null;
    const s = [...arr].sort((a, b) => a - b);
    return s[Math.min(s.length - 1, Math.floor(p * s.length))];
  }

  function fmtMs(v) {
    if (v == null) return '–';
    return v >= 1000 ? (v / 1000).toFixed(2) + ' s' : Math.round(v) + ' ms';
  }

  function reasonFamily(reason, status) {
    if (status === 503 || /shed|no_budget|no_backend/.test(reason)) return 'danger';
    if (/spillover|cloud_only|critical_least/.test(reason)) return 'warn';
    if (/prefix_hit/.test(reason)) return 'accent';
    return '';
  }

  function esc(s) {
    return String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  }

  // ---- backends --------------------------------------------------------

  function renderBackends(data) {
    $('policy').textContent = data.policy || 'policy';
    const locals = data.backends.filter((b) => b.kind !== 'cloud');
    const clouds = data.backends.filter((b) => b.kind === 'cloud');
    $('replicas').innerHTML = locals.map(replicaCard).join('') || '<div class="empty">No local replicas configured.</div>';
    $('providers-wrap').classList.toggle('hidden', clouds.length === 0);
    $('providers').innerHTML = clouds.map(providerCard).join('');
    const spend = Object.entries(data.spend_usd || {});
    const total = spend.reduce((s, [, v]) => s + v, 0);
    $('st-spend').textContent = '$' + total.toFixed(2);
    $('st-spend').title = spend.map(([t, v]) => `${t}: $${v.toFixed(4)}`).join('\n');
  }

  function fillClass(x) {
    return x >= 0.9 ? 'danger' : x >= 0.7 ? 'warn' : '';
  }

  function replicaCard(b) {
    const kv = Math.max(0, Math.min(1, b.kv_estimate || 0));
    const kvScraped = Math.max(0, Math.min(1, b.kv_scraped || 0));
    const score = b.score || 0;
    return `<div class="replica">
      <div class="row">
        <span class="name"><span class="dot ${b.healthy ? '' : 'down'}"></span>${esc(b.name)}<span class="tag">${esc(b.kind)}</span></span>
        <span class="num small muted">score ${score.toFixed(2)}</span>
      </div>
      <div class="kv-row"><span>KV</span>
        <div class="bar"><div class="fill ${fillClass(kv)}" style="width:${(kv * 100).toFixed(1)}%"></div><div class="tick" style="left:${(kvScraped * 100).toFixed(1)}%" title="scraped ${(kvScraped * 100).toFixed(0)}%"></div></div>
        <span class="num">${(kv * 100).toFixed(0)}%</span></div>
      <div class="metrics">
        <div><span class="k">In flight</span><span class="v num">${b.inflight}/${b.max_batch}</span></div>
        <div><span class="k">Waiting</span><span class="v num">${b.waiting}</span></div>
        <div><span class="k">TTFT ewma</span><span class="v num">${fmtMs(b.ewma_ttft_ms || null)}</span></div>
        <div><span class="k">Prefix blocks</span><span class="v num">${b.prefix_blocks_indexed}</span></div>
      </div>
      ${b.scrape_error ? `<div class="scrape-err">scrape: ${esc(b.scrape_error)}</div>` : ''}
    </div>`;
  }

  function providerCard(b) {
    return `<div class="replica">
      <div class="row">
        <span class="name"><span class="dot ${b.healthy ? '' : 'down'}"></span>${esc(b.name)}<span class="tag cloud">cloud</span></span>
        <span class="num small muted">${b.completed} done</span>
      </div>
      <div class="metrics">
        <div><span class="k">In flight</span><span class="v num">${b.inflight}</span></div>
        <div><span class="k">TTFT ewma</span><span class="v num">${fmtMs(b.ewma_ttft_ms || null)}</span></div>
        <div><span class="k">Billing</span><span class="v">per token</span></div>
        <div><span class="k">Role</span><span class="v">spillover</span></div>
      </div>
    </div>`;
  }

  // ---- decisions -------------------------------------------------------

  function ingest(list) {
    for (let i = list.length - 1; i >= 0; i--) { // oldest first so order is chronological
      const d = list[i];
      if (!d || !d.id) continue;
      if (!seen.has(d.id)) order.push(d.id);
      seen.set(d.id, d);
    }
    while (order.length > WINDOW) seen.delete(order.shift());
  }

  function renderStats() {
    const all = order.map((id) => seen.get(id));
    const ok = all.filter((d) => d.status === 200 && d.ttft_ms > 0);
    const ttfts = ok.map((d) => d.ttft_ms);
    $('st-total').textContent = all.length;
    $('st-p50').textContent = fmtMs(pct(ttfts, 0.5));
    $('st-p99').textContent = fmtMs(pct(ttfts, 0.99));
    $('st-shed').textContent = all.filter((d) => d.status === 503).length;
  }

  function renderTable(latest) {
    const tbody = document.querySelector('#decisions tbody');
    const rows = latest.slice(0, 40);
    $('decisions-empty').classList.toggle('hidden', rows.length > 0);
    tbody.innerHTML = rows.map((d) => {
      const fam = reasonFamily(d.reason, d.status);
      const t = new Date(d.time);
      const time = t.toLocaleTimeString(undefined, { hour12: false }) + '.' + String(t.getMilliseconds()).padStart(3, '0');
      const backend = d.backend ? `<span style="color:${color(d.backend)}">${esc(d.backend)}</span>` : '<span class="muted">–</span>';
      const main = `<tr class="decision ${openId === d.id ? 'open' : ''}" data-id="${esc(d.id)}">
        <td class="num">${time}</td>
        <td class="id">${esc(d.id)}</td>
        <td>${esc(d.tenant)}</td>
        <td><span class="chip crit-${esc(d.criticality)}">${esc(d.criticality)}</span></td>
        <td>${backend}</td>
        <td><span class="chip ${fam}">${esc(d.reason)}${d.status === 503 ? ' · 503' : ''}</span></td>
        <td class="num">${d.prefix_blocks || 0}</td>
        <td class="num">${d.prompt_tokens}</td>
        <td class="num">${d.status === 200 ? fmtMs(d.ttft_ms) : '–'}</td>
        <td class="num">${d.status === 200 ? fmtMs(d.total_ms) : '–'}</td>
        <td class="num">${d.cost_usd ? '$' + d.cost_usd.toFixed(4) : '–'}</td>
      </tr>`;
      return openId === d.id ? main + detailRow(d) : main;
    }).join('');
  }

  function detailRow(d) {
    const scores = Object.entries(d.scores || {}).sort((a, b) => a[1] - b[1]);
    const body = scores.length
      ? `<div class="score-list">${scores.map(([name, s]) => `<div class="score-row ${name === d.backend ? 'chosen' : ''}">
            <span>${esc(name)}</span>
            <div class="bar"><div class="fill ${fillClass(s)}" style="width:${(Math.min(1, s) * 100).toFixed(1)}%"></div></div>
            <span class="num">${s.toFixed(2)}</span></div>`).join('')}</div>
         <div class="muted small" style="margin-top:8px">Load score = 0.6 × KV estimate + 0.4 × in-flight/batch. Lower is idler. ${d.prefix_blocks ? `${d.prefix_blocks} prompt blocks expected cached on ${esc(d.backend)}.` : ''}</div>`
      : `<div class="muted small">No per-backend scores for this decision (${esc(d.reason)}).</div>`;
    return `<tr class="detail"><td colspan="11">${body}</td></tr>`;
  }

  document.querySelector('#decisions tbody').addEventListener('click', (e) => {
    const tr = e.target.closest('tr.decision');
    if (!tr) return;
    openId = openId === tr.dataset.id ? null : tr.dataset.id;
    renderTable(latestList);
  });

  // ---- chart -----------------------------------------------------------

  function renderChart() {
    const svg = $('chart');
    const now = Date.now();
    const pts = order.map((id) => seen.get(id))
      .filter((d) => d.status === 200 && d.ttft_ms > 0 && now - new Date(d.time).getTime() <= CHART_MS);
    const wrap = svg.parentElement;
    wrap.classList.toggle('has-data', pts.length > 0);
    const W = 800, H = 220, L = 44, R = 8, T = 10, B = 24;
    if (!pts.length) { svg.innerHTML = ''; $('legend').innerHTML = ''; return; }
    const maxY = Math.max(50, ...pts.map((d) => d.ttft_ms)) * 1.1;
    const x = (t) => L + ((W - L - R) * (1 - (now - t) / CHART_MS));
    const y = (v) => T + (H - T - B) * (1 - v / maxY);
    let out = '';
    const ticks = 4;
    for (let i = 0; i <= ticks; i++) {
      const v = (maxY / ticks) * i;
      out += `<line x1="${L}" x2="${W - R}" y1="${y(v)}" y2="${y(v)}" stroke="#262b33" stroke-width="1"/>`;
      out += `<text x="${L - 6}" y="${y(v) + 4}" fill="#9aa3b2" font-size="10" text-anchor="end" font-family="Inter, sans-serif">${v >= 1000 ? (v / 1000).toFixed(1) + 's' : Math.round(v) + 'ms'}</text>`;
    }
    for (let i = 0; i <= 4; i++) {
      const t = now - CHART_MS + (CHART_MS / 4) * i;
      out += `<text x="${x(t)}" y="${H - 6}" fill="#9aa3b2" font-size="10" text-anchor="middle" font-family="Inter, sans-serif">-${Math.round((now - t) / 1000)}s</text>`;
    }
    for (const d of pts) {
      const c = color(d.backend);
      out += `<circle cx="${x(new Date(d.time).getTime()).toFixed(1)}" cy="${y(d.ttft_ms).toFixed(1)}" r="3" fill="${c}" opacity="0.85"><title>${esc(d.backend)} ${Math.round(d.ttft_ms)} ms (${esc(d.reason)})</title></circle>`;
    }
    svg.innerHTML = out;
    const backends = [...new Set(pts.map((d) => d.backend))];
    $('legend').innerHTML = backends.map((b) => `<span><i style="background:${color(b)}"></i>${esc(b)}</span>`).join('');
  }

  // ---- polling ---------------------------------------------------------

  let latestList = [];
  let failures = 0;

  async function poll() {
    try {
      const [b, d] = await Promise.all([
        fetch('/admin/backends').then((r) => r.json()),
        fetch('/admin/decisions').then((r) => r.json())
      ]);
      failures = 0;
      renderBackends(b);
      latestList = Array.isArray(d) ? d : [];
      ingest(latestList);
      renderStats();
      renderTable(latestList);
      renderChart();
      $('replica-note').textContent = 'live · ' + new Date().toLocaleTimeString(undefined, { hour12: false });
    } catch (err) {
      failures++;
      $('replica-note').textContent = 'admin API unreachable (' + failures + ')';
    } finally {
      setTimeout(poll, POLL_MS);
    }
  }

  // ---- demo traffic ----------------------------------------------------

  const SYSTEM_PROMPTS = [
    'You are a concise assistant for a concert discovery app. Answer with the venue, date and one reason to go. ',
    'You are a support agent for a payments company. Be precise, cite the relevant policy section, and never guess at amounts. ',
    'You are a code reviewer. Point out correctness bugs first, then simplifications, then style; keep each finding to one sentence. '
  ].map((s) => s.repeat(24)); // long enough to span several prefix blocks
  const USER_TURNS = ['What is playing this weekend?', 'Summarize the refund policy.', 'Is this loop off by one?', 'Rank these three options.', 'Explain in two sentences.', 'What changed since yesterday?'];
  const CRITS = ['standard', 'standard', 'standard', 'standard', 'standard', 'standard', 'standard', 'sheddable', 'sheddable', 'critical'];

  function pick(a) { return a[Math.floor(Math.random() * a.length)]; }

  async function fireOne(i) {
    const body = {
      model: 'llama-3-8b', stream: true, max_tokens: 24,
      messages: [{ role: 'system', content: pick(SYSTEM_PROMPTS) }, { role: 'user', content: `${pick(USER_TURNS)} (#${i}-${Date.now()})` }]
    };
    const res = await fetch('/v1/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer demo-key', 'X-Criticality': pick(CRITS) },
      body: JSON.stringify(body)
    });
    const backend = res.headers.get('X-Herd-Backend') || '–';
    if (res.body) { const reader = res.body.getReader(); while (!(await reader.read()).done) { /* drain */ } }
    return { status: res.status, backend };
  }

  async function fire(n, btn) {
    const status = $('fire-status');
    const buttons = [$('fire-20'), $('fire-100')];
    buttons.forEach((b) => (b.disabled = true));
    let done = 0, shed = 0;
    const counts = {};
    status.textContent = `sending ${n}…`;
    const jobs = [];
    for (let i = 0; i < n; i++) {
      jobs.push(new Promise((resolve) => setTimeout(resolve, i * (n > 50 ? 15 : 60))).then(() => fireOne(i)).then((r) => {
        done++;
        if (r.status === 503) shed++;
        else counts[r.backend] = (counts[r.backend] || 0) + 1;
        status.textContent = `${done}/${n} done${shed ? `, ${shed} shed` : ''}`;
      }).catch(() => { done++; status.textContent = `${done}/${n} done (errors)`; }));
    }
    await Promise.all(jobs);
    const share = Object.entries(counts).map(([b, c]) => `${b} ${c}`).join(' · ');
    status.textContent = `${n} sent${shed ? `, ${shed} shed` : ''}${share ? ' · ' + share : ''}`;
    buttons.forEach((b) => (b.disabled = false));
    void btn;
  }

  $('fire-20').addEventListener('click', (e) => fire(20, e.currentTarget));
  $('fire-100').addEventListener('click', (e) => fire(100, e.currentTarget));

  poll();
})();
