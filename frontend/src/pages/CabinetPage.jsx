import { useState, useEffect } from 'react';
import { formatCartPrice } from '../utils/api';
import t from '../i18n/uz.json';

const API = '/api/cabinet';
const FINANCE_API = '/api/finance';

function getTelegramUserId() {
  return window.Telegram?.WebApp?.initDataUnsafe?.user?.id || 0;
}

const STATUS_LABELS = {
  submitted: t.order_status_submitted,
  confirmed: t.order_status_confirmed,
  delivered: t.order_status_delivered,
  completed: t.order_status_completed,
};

const STATUS_ICONS = {
  submitted: '📤',
  confirmed: '✅',
  delivered: '🚚',
  completed: '✔️',
};

const MONTHS = t.balance_month_short || ['Yan', 'Fev', 'Mar', 'Apr', 'May', 'Iyun', 'Iyul', 'Avg', 'Sen', 'Okt', 'Noy', 'Dek'];

function formatUzs(amount) {
  if (!amount && amount !== 0) return '0';
  const num = Math.round(Math.abs(amount));
  return num.toLocaleString('ru-RU').replace(/,/g, ' ');
}

function formatUsd(v) {
  return '$' + Math.abs(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatPeriod(start) {
  if (!start) return '';
  try {
    const d = new Date(start);
    return `${MONTHS[d.getMonth()]} ${d.getFullYear()}`;
  } catch {
    return start;
  }
}

function formatDate(dateStr) {
  if (!dateStr) return '';
  try {
    const d = new Date(dateStr + 'Z');
    return d.toLocaleDateString('uz-UZ', {
      day: 'numeric',
      month: 'short',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  } catch {
    return dateStr;
  }
}

// Format "YYYY-MM" → "MM/YY" for chart x-axis labels
function fmtMonthLabel(m) {
  if (!m || m.length < 7) return m || '';
  return m.slice(5, 7) + '/' + m.slice(2, 4);
}

function fmtMonthFull(m) {
  if (!m || m.length < 7) return m || '';
  const monthIdx = parseInt(m.slice(5, 7), 10) - 1;
  const names = ['Yan', 'Fev', 'Mar', 'Apr', 'May', 'Iyun', 'Iyul', 'Avg', 'Sen', 'Okt', 'Noy', 'Dek'];
  return `${names[monthIdx]} ${m.slice(2, 4)}`;
}

// ── Mini SVG line chart (pure, no external lib) ──
function SpendChart({ data, valueKey, color, label, formatValue, header, comparisons }) {
  const [tappedIdx, setTappedIdx] = useState(null);
  if (!data || data.length === 0) return null;
  const values = data.map(d => d[valueKey] || 0);
  const maxVal = Math.max(...values, 1);
  const W = 300, H = 120, PAD_TOP = 8, PAD_BOT = 8, PAD_LEFT = 4, PAD_RIGHT = 4;
  const chartW = W - PAD_LEFT - PAD_RIGHT;
  const chartH = H - PAD_TOP - PAD_BOT;
  const step = data.length > 1 ? chartW / (data.length - 1) : 0;

  const points = values.map((v, i) => {
    const x = PAD_LEFT + i * step;
    const y = PAD_TOP + chartH - (v / maxVal) * chartH;
    return { x, y, v };
  });

  const linePath = points.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x},${p.y}`).join(' ');
  const areaPath = linePath + ` L${points[points.length - 1].x},${PAD_TOP + chartH} L${points[0].x},${PAD_TOP + chartH} Z`;

  const handleDotTap = (i) => {
    setTappedIdx(tappedIdx === i ? null : i);
  };

  return (
    <div className="mt-2">
      {/* Header: last closed month label + amount */}
      <div className="flex items-baseline justify-between mb-1">
        <span className="text-[10px] text-tg-hint">{label}</span>
        {header && (
          <span className="text-xs font-semibold" style={{ color }}>
            {header.month}: {header.value}
          </span>
        )}
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ maxHeight: '120px' }}>
        <defs>
          <linearGradient id={`grad-${color}`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity="0.3" />
            <stop offset="100%" stopColor={color} stopOpacity="0.02" />
          </linearGradient>
        </defs>
        {/* Grid lines */}
        {[0, 0.25, 0.5, 0.75, 1].map(frac => {
          const y = PAD_TOP + chartH - frac * chartH;
          return <line key={frac} x1={PAD_LEFT} y1={y} x2={W - PAD_RIGHT} y2={y} stroke="var(--tg-theme-hint-color, #999)" strokeOpacity="0.15" strokeWidth="0.5" />;
        })}
        {/* Area fill */}
        {data.length > 1 && <path d={areaPath} fill={`url(#grad-${color})`} />}
        {/* Line */}
        {data.length > 1 && <path d={linePath} fill="none" stroke={color} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />}
        {/* Dots — tappable, no x-axis labels */}
        {points.map((p, i) => (
          <g key={i} onClick={() => handleDotTap(i)} style={{ cursor: 'pointer' }}>
            <circle cx={p.x} cy={p.y} r="12" fill="transparent" />
            <circle cx={p.x} cy={p.y} r={tappedIdx === i ? 5 : 3} fill={color} />
            {/* Tooltip on tap */}
            {tappedIdx === i && (
              <>
                <rect x={Math.max(2, Math.min(p.x - 50, W - 102))} y={Math.max(2, p.y - 32)} width="100" height="26" rx="4" fill="var(--tg-theme-bg-color, #fff)" stroke={color} strokeWidth="0.5" />
                <text x={Math.max(52, Math.min(p.x, W - 52))} y={Math.max(13, p.y - 21)} textAnchor="middle" fontSize="8" fill="var(--tg-theme-hint-color, #999)">
                  {fmtMonthFull(data[i].month)}
                </text>
                <text x={Math.max(52, Math.min(p.x, W - 52))} y={Math.max(25, p.y - 10)} textAnchor="middle" fontSize="9" fontWeight="600" fill={color}>
                  {formatValue ? formatValue(p.v) : p.v.toLocaleString('ru-RU')}
                </text>
              </>
            )}
          </g>
        ))}
      </svg>
      {/* Comparison lines */}
      {comparisons && comparisons.length > 0 && (
        <div className="mt-1 space-y-0.5">
          {comparisons.map((c, ci) => (
            <div key={ci} className="text-[10px] text-tg-hint text-center">
              {c.vsLabel}{' '}
              <span className={c.diff > 0 ? 'text-green-500 font-medium' : c.diff < 0 ? 'text-red-400 font-medium' : ''}>
                {c.diff > 0 ? '↑' : c.diff < 0 ? '↓' : '→'}{' '}
                {c.diff !== 0 ? c.diffStr : '—'}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function OrderIssueForm({ order, onDone, t }) {
  const [comment, setComment] = useState('');
  const [files, setFiles] = useState([]);
  const [submitting, setSubmitting] = useState(false);
  const [sent, setSent] = useState(false);

  const submit = async () => {
    if (submitting) return;
    if (!comment.trim() && files.length === 0) {
      onDone();
      return;
    }
    setSubmitting(true);
    try {
      const fd = new FormData();
      fd.append('order_id', order.id);
      fd.append('telegram_id', getTelegramUserId());
      fd.append('comment', comment.trim());
      fd.append('order_doc_number', order.doc_number || '');
      fd.append('order_date', order.date || '');
      files.forEach(f => fd.append('files', f));
      const res = await fetch('/api/feedback/order-issue', { method: 'POST', body: fd });
      const data = await res.json();
      if (data.ok) {
        setSent(true);
        setTimeout(onDone, 900);
      } else {
        alert((t && t.akt_issue_error) || 'Xatolik');
        setSubmitting(false);
      }
    } catch (e) {
      alert(String(e).slice(0, 200));
      setSubmitting(false);
    }
  };

  if (sent) {
    return (
      <div className="mt-4 text-center text-sm text-emerald-600 font-medium py-2">
        ✅ {t.akt_issue_sent}
      </div>
    );
  }

  return (
    <div className="mt-4 border-t border-tg-hint/10 pt-4 space-y-2">
      <div className="text-xs text-tg-hint">
        {t.akt_issue_prompt}
      </div>
      <textarea
        value={comment}
        onChange={e => setComment(e.target.value)}
        placeholder={t.akt_issue_placeholder}
        rows={3}
        className="w-full text-sm bg-tg-secondary rounded-lg px-3 py-2 outline-none border border-tg-hint/10 focus:border-tg-link"
      />
      <div className="flex items-center gap-2">
        <label className="flex-1 cursor-pointer">
          <input
            type="file"
            accept="image/*"
            multiple
            onChange={e => setFiles(Array.from(e.target.files || []))}
            className="hidden"
          />
          <div className="py-2.5 rounded-xl bg-tg-secondary border border-dashed border-tg-hint/30 text-center text-xs font-medium">
            📎 {files.length > 0 ? `${files.length} ${t.akt_issue_photo_n}` : t.akt_issue_attach_photo}
          </div>
        </label>
        <button
          onClick={submit}
          disabled={submitting}
          className="flex-1 py-2.5 rounded-xl bg-tg-link text-white text-sm font-semibold disabled:opacity-60"
        >
          {submitting ? '…' : t.akt_issue_send}
        </button>
      </div>
      <button
        onClick={onDone}
        className="w-full py-2 rounded-xl bg-transparent text-tg-hint text-xs"
      >
        {t.reorder_cancel}
      </button>
    </div>
  );
}


function AktSheetItemsLoader({ orderId, onLoaded }) {
  useEffect(() => {
    const userId = getTelegramUserId();
    if (!orderId || !userId) { onLoaded([]); return; }
    let cancelled = false;
    fetch(`${API}/real-orders/${orderId}?telegram_id=${userId}`)
      .then(r => r.json())
      .then(data => {
        if (cancelled) return;
        onLoaded(data?.ok ? (data.items || []) : []);
      })
      .catch(() => { if (!cancelled) onLoaded([]); });
    return () => { cancelled = true; };
  }, [orderId]);
  return <div className="text-[11px] text-tg-hint py-1">…</div>;
}

export default function CabinetPage({ cart, onNavigateToCart }) {
  const [orders, setOrders] = useState([]);
  const [loading, setLoading] = useState(true);
  const [expandedId, setExpandedId] = useState(null);
  const [expandedItems, setExpandedItems] = useState([]);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [reorderDialog, setReorderDialog] = useState(null);
  const [reordering, setReordering] = useState(false);
  const [toast, setToast] = useState(null);
  const [balance, setBalance] = useState(null);
  const [payments, setPayments] = useState([]);
  const [balanceLoading, setBalanceLoading] = useState(true);
  const [realOrders, setRealOrders] = useState([]);
  const [realLoading, setRealLoading] = useState(true);
  const [expandedRealId, setExpandedRealId] = useState(null);
  const [expandedRealItems, setExpandedRealItems] = useState([]);
  const [loadingRealDetail, setLoadingRealDetail] = useState(false);

  // Hisob-kitob state (unified dual-currency)
  const [akt, setAkt] = useState(null);
  const [aktSheet, setAktSheet] = useState(null);  // payment or order detail
  const [aktSheetItems, setAktSheetItems] = useState(null);  // order items when sheet is an order

  // Rassvet Plus — business intelligence state
  const [spendTrend, setSpendTrend] = useState(null);
  const [topProducts, setTopProducts] = useState(null);
  const [activitySummary, setActivitySummary] = useState(null);
  const [bizLoading, setBizLoading] = useState(true);

  // Credit Score state (Phase 5)
  const [creditScore, setCreditScore] = useState(null);
  const [scoreLoading, setScoreLoading] = useState(true);

  // Location state
  const [userLocation, setUserLocation] = useState(null);

  const userId = getTelegramUserId();

  // Load orders and balance
  useEffect(() => {
    if (!userId) {
      setLoading(false);
      setBalanceLoading(false);
      setBizLoading(false);
      setScoreLoading(false);
      return;
    }
    fetch(`${API}/orders?telegram_id=${userId}`)
      .then(r => r.json())
      .then(data => {
        setOrders(data.orders || []);
        setLoading(false);
      })
      .catch(() => setLoading(false));

    fetch(`${FINANCE_API}/balance?telegram_id=${userId}`)
      .then(r => r.json())
      .then(data => {
        if (data.ok && data.has_balance) {
          setBalance(data.balance);
        }
        setBalanceLoading(false);
      })
      .catch(() => setBalanceLoading(false));

    fetch(`${API}/real-orders?telegram_id=${userId}&limit=20`)
      .then(r => r.json())
      .then(data => {
        if (data.ok) setRealOrders(data.orders || []);
        setRealLoading(false);
      })
      .catch(() => setRealLoading(false));

    fetch(`${API}/payments?telegram_id=${userId}&limit=10`)
      .then(r => r.json())
      .then(data => {
        if (data.ok) setPayments(data.payments || []);
      })
      .catch(() => {});

    // Hisob-kitob — unified dual-currency акт сверки
    fetch(`${API}/akt-sverki?telegram_id=${userId}&limit=80`)
      .then(r => r.json())
      .then(data => { if (data?.ok) setAkt(data); })
      .catch(() => {});

    // Rassvet Plus — fetch business intelligence data
    Promise.all([
      fetch(`${API}/spend-trend?telegram_id=${userId}&months=16`).then(r => r.json()),
      fetch(`${API}/top-products?telegram_id=${userId}&limit=5`).then(r => r.json()),
      fetch(`${API}/activity-summary?telegram_id=${userId}`).then(r => r.json()),
    ])
      .then(([trend, products, activity]) => {
        if (trend.ok && trend.months?.length > 0) setSpendTrend(trend.months);
        if (products.ok) setTopProducts({ uzs: products.top_uzs || [], usd: products.top_usd || [] });
        if (activity.ok && activity.summary) setActivitySummary(activity.summary);
        setBizLoading(false);
      })
      .catch(() => setBizLoading(false));

    // Credit Score — fetch from finance API
    fetch(`${FINANCE_API}/credit-score?telegram_id=${userId}`)
      .then(r => r.json())
      .then(data => {
        if (data.ok && data.has_score) setCreditScore(data.score);
        setScoreLoading(false);
      })
      .catch(() => setScoreLoading(false));

    // Fetch saved location
    fetch(`/api/client-location?telegram_id=${userId}`)
      .then(r => r.json())
      .then(data => {
        if (data.has_gps && data.gps) setUserLocation(data.gps);
      })
      .catch(() => {});
  }, [userId]);

  // Toggle expand real order detail
  const toggleExpandReal = async (realId) => {
    if (expandedRealId === realId) {
      setExpandedRealId(null);
      setExpandedRealItems([]);
      return;
    }
    setExpandedRealId(realId);
    setLoadingRealDetail(true);
    try {
      const res = await fetch(`${API}/real-orders/${realId}?telegram_id=${userId}`);
      const data = await res.json();
      setExpandedRealItems(data.items || []);
    } catch {
      setExpandedRealItems([]);
    }
    setLoadingRealDetail(false);
  };

  // Toggle expand order detail
  const toggleExpand = async (orderId) => {
    if (expandedId === orderId) {
      setExpandedId(null);
      setExpandedItems([]);
      return;
    }
    setExpandedId(orderId);
    setLoadingDetail(true);
    try {
      const res = await fetch(`${API}/orders/${orderId}?telegram_id=${userId}`);
      const data = await res.json();
      setExpandedItems(data.items || []);
    } catch {
      setExpandedItems([]);
    }
    setLoadingDetail(false);
  };

  // Reorder flow
  const handleReorderTap = (orderId) => {
    if (cart.items.length > 0) {
      setReorderDialog(orderId);
    } else {
      doReorder(orderId, 'replace');
    }
  };

  const doReorder = async (orderId, mode) => {
    setReorderDialog(null);
    setReordering(true);
    try {
      const res = await fetch(
        `${API}/orders/${orderId}/reorder?telegram_id=${userId}&mode=${mode}`,
        { method: 'POST' }
      );
      const data = await res.json();
      if (data.ok) {
        await cart.reloadCart();
        let msg = t.reorder_success;
        if (data.skipped > 0) {
          msg += ` (${data.skipped} ${t.reorder_partial})`;
        }
        setToast(msg);
        setTimeout(() => {
          setToast(null);
          onNavigateToCart?.();
        }, 1500);
      }
    } catch {
      setToast('Xatolik yuz berdi');
      setTimeout(() => setToast(null), 2000);
    }
    setReordering(false);
  };

  if (loading) {
    return <div className="text-center py-16 text-tg-hint">{t.loading}</div>;
  }

  // ── Helper: balance status label ──
  const getBalanceLabel = (bal) => {
    if (bal > 0) return t.balance_debt;
    if (bal < 0) return t.balance_overpayment;
    return t.balance_settled;
  };

  // ── Balance Card — shows debt from дебиторка or fallback to оборотка ──
  const BalanceCard = () => {
    if (balanceLoading || !balance) return null;

    const debtUzs = balance.debt_uzs ?? null;
    const debtUsd = balance.debt_usd ?? null;
    const isDebtSource = debtUzs !== null;

    // Debt source (дебиторка) — simple debt display
    if (isDebtSource) {
      const hasUzs = debtUzs > 0;
      const hasUsd = debtUsd > 0;
      const isSettled = !hasUzs && !hasUsd;

      return (
        <div className="mb-4">
          <div className="text-sm text-tg-hint mb-2">{t.balance_title}</div>
          <div className="bg-tg-secondary rounded-xl p-4">
            <div className="text-[10px] text-tg-hint text-center mb-1">{t.balance_current}</div>

            {isSettled ? (
              <div className="text-center mb-2">
                <div className="text-xl font-bold text-tg-hint">{t.balance_settled}</div>
              </div>
            ) : (
              <div className="flex items-baseline justify-center gap-4 mb-2">
                {hasUzs && (
                  <div className="text-center">
                    <div className="text-xl font-bold text-red-500">
                      {formatUzs(debtUzs)} {t.balance_currency || "so'm"}
                    </div>
                  </div>
                )}
                {hasUzs && hasUsd && (
                  <div className="text-tg-hint/30 text-lg font-light">│</div>
                )}
                {hasUsd && (
                  <div className="text-center">
                    <div className="text-xl font-bold text-red-500">
                      {formatUsd(debtUsd)}
                    </div>
                  </div>
                )}
              </div>
            )}

            {!isSettled && (
              <div className="text-center text-xs text-red-400 mb-1">
                {t.balance_debt}
              </div>
            )}

            <div className="text-[10px] text-tg-hint text-center mt-2">
              {t.balance_updated}: {formatDate(balance.imported_at)}
            </div>
          </div>
        </div>
      );
    }

    // Fallback: оборотка-based balance (legacy)
    const currencies = balance.balances_by_currency || {};
    const uzs = currencies.UZS || {
      balance: balance.balance || 0,
      period_debit: balance.period_debit || 0,
      period_credit: balance.period_credit || 0,
      period_start: balance.period_start,
    };
    const usd = currencies.USD;
    const hasBoth = !!usd;

    const balUzs = uzs.balance || 0;
    const balUsd = usd ? (usd.balance || 0) : 0;

    const balColor = (v) => v === 0 ? 'text-tg-hint' : v > 0 ? 'text-red-500' : 'text-green-500';
    const fmtBal = (v, fn, suffix) => {
      if (v === 0) return <span className="text-tg-hint">0 {suffix}</span>;
      return <>{v > 0 ? '' : '−'}{fn(v)} {suffix}</>;
    };

    const hasAnyActivity =
      (uzs.period_debit || 0) > 0 || (uzs.period_credit || 0) > 0 ||
      (usd && ((usd.period_debit || 0) > 0 || (usd.period_credit || 0) > 0));

    return (
      <div className="mb-4">
        <div className="text-sm text-tg-hint mb-2">{t.balance_title}</div>
        <div className="bg-tg-secondary rounded-xl p-4">
          <div className="text-[10px] text-tg-hint text-center mb-1">{t.balance_current}</div>
          {hasBoth ? (
            <div className="flex items-baseline justify-center gap-4 mb-3">
              <div className="text-center">
                <div className={`text-xl font-bold ${balColor(balUzs)}`}>
                  {fmtBal(balUzs, formatUzs, t.balance_currency || "so'm")}
                </div>
              </div>
              <div className="text-tg-hint/30 text-lg font-light">│</div>
              <div className="text-center">
                <div className={`text-xl font-bold ${balColor(balUsd)}`}>
                  {fmtBal(balUsd, formatUsd, '')}
                </div>
              </div>
            </div>
          ) : (
            <div className="text-center mb-3">
              <div className={`text-xl font-bold ${balColor(balUzs)}`}>
                {fmtBal(balUzs, formatUzs, t.balance_currency || "so'm")}
              </div>
            </div>
          )}

          {hasAnyActivity ? (
            <div className="border-t border-tg-hint/20 pt-2">
              <table className="w-full text-xs">
                <thead>
                  <tr className="text-tg-hint">
                    <th className="text-left font-normal pb-1"></th>
                    <th className="text-right font-normal pb-1">UZS</th>
                    {hasBoth && <th className="text-right font-normal pb-1">USD</th>}
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td className="text-tg-hint py-0.5">{t.balance_shipped}</td>
                    <td className="text-right font-semibold py-0.5">{formatUzs(uzs.period_debit || 0)}</td>
                    {hasBoth && <td className="text-right font-semibold py-0.5">{formatUsd(usd.period_debit || 0)}</td>}
                  </tr>
                  <tr>
                    <td className="text-tg-hint py-0.5">{t.balance_paid}</td>
                    <td className="text-right font-semibold text-green-600 py-0.5">{formatUzs(uzs.period_credit || 0)}</td>
                    {hasBoth && <td className="text-right font-semibold text-green-600 py-0.5">{formatUsd(usd.period_credit || 0)}</td>}
                  </tr>
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-center text-xs text-tg-hint py-1">
              {t.balance_no_activity}
            </div>
          )}

          <div className="text-[10px] text-tg-hint text-center mt-2">
            {t.balance_updated}: {formatDate(balance.imported_at)}
          </div>
        </div>
      </div>
    );
  };

  // ── Last order only ──
  const lastOrder = orders.length > 0 ? orders[0] : null;
  const isExpanded = lastOrder && expandedId === lastOrder.id;

  if (!lastOrder && !balance && realOrders.length === 0) {
    return (
      <div className="text-center py-16">
        <div className="text-5xl mb-4">🏛️</div>
        <div className="text-lg font-medium">{t.no_orders}</div>
        <div className="text-sm text-tg-hint mt-1">{t.no_orders_desc}</div>
      </div>
    );
  }

  // Format a 1C doc date (YYYY-MM-DD) to short locale
  const formatDocDate = (s) => {
    if (!s) return '';
    try {
      const d = new Date(s);
      return d.toLocaleDateString('uz-UZ', { day: 'numeric', month: 'short', year: 'numeric' });
    } catch {
      return s;
    }
  };

  // ── Credit Score Card (Phase 5 — soft launch) ──
  const CreditScoreCard = () => {
    if (scoreLoading || !creditScore) return null;

    const score = creditScore.value;
    const tier = creditScore.tier;
    const limitUzs = creditScore.credit_limit_uzs;
    const bucket = creditScore.volume_bucket;
    const hints = creditScore.hints || [];

    // Tier badge colors (subtle, muted)
    const tierColors = {
      'Yangi': { bg: 'bg-gray-100', text: 'text-gray-600', border: 'border-gray-200' },
      'Oddiy': { bg: 'bg-blue-50', text: 'text-blue-600', border: 'border-blue-100' },
      'Yaxshi': { bg: 'bg-green-50', text: 'text-green-600', border: 'border-green-100' },
      "A'lo": { bg: 'bg-purple-50', text: 'text-purple-600', border: 'border-purple-100' },
      'VIP': { bg: 'bg-amber-50', text: 'text-amber-600', border: 'border-amber-100' },
    };
    const tc = tierColors[tier] || tierColors['Oddiy'];

    // Score arc: subtle progress indicator
    const pct = Math.max(0, Math.min(100, score));
    const arcRadius = 38;
    const arcCircumference = 2 * Math.PI * arcRadius;
    const arcOffset = arcCircumference * (1 - pct / 100);

    return (
      <div className="mb-4">
        <div className="text-sm text-tg-hint mb-2">
          ⭐ {t.credit_score_title}
        </div>
        <div className="bg-tg-secondary rounded-xl p-3 flex items-center gap-3">
          <div className="relative flex-shrink-0" style={{ width: 72, height: 72 }}>
            <svg viewBox="0 0 80 80" className="w-full h-full">
              <circle cx="40" cy="40" r={arcRadius} fill="none"
                stroke="var(--tg-theme-hint-color, #ccc)" strokeOpacity="0.15"
                strokeWidth="5" />
              <circle cx="40" cy="40" r={arcRadius} fill="none"
                stroke={score >= 71 ? '#8B5CF6' : score >= 51 ? '#10B981' : '#6B7280'}
                strokeWidth="5" strokeLinecap="round"
                strokeDasharray={arcCircumference}
                strokeDashoffset={arcOffset}
                transform="rotate(-90 40 40)" />
              <text x="40" y="38" textAnchor="middle" fontSize="18" fontWeight="700"
                fill="var(--tg-theme-text-color, #333)">{score}</text>
              <text x="40" y="52" textAnchor="middle" fontSize="7"
                fill="var(--tg-theme-hint-color, #999)">/ 100</text>
            </svg>
          </div>
          {hints.length > 0 && (
            <div className="flex-1 min-w-0 space-y-0.5">
              {hints.map((h, i) => (
                <div key={i} className="text-[10px] text-tg-hint flex items-start gap-1 leading-tight">
                  <span className="text-tg-hint/40 mt-0.5">•</span>
                  <span>{h}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    );
  };

  // ── Hisob-kitob (unified dual-currency timeline) ──
  const fmtUzs = (v) => `${formatUzs(v)} ${t.balance_currency || "so'm"}`;
  const fmtUsd = (v) => formatUsd(v);

  const renderMiniStatus = (state, currency) => {
    if (!state) return null;
    const fmt = currency === 'USD' ? fmtUsd : fmtUzs;
    let bg, icon, title, sub;
    if (state.code === 'clean') {
      bg = 'bg-green-50 border-green-200 text-green-800';
      icon = '✅';
      title = `${currency}: ${t.akt_clean_title}`;
      sub = t.akt_clean_sub;
    } else if (state.code === 'advance') {
      bg = 'bg-emerald-50 border-emerald-200 text-emerald-800';
      icon = '💚';
      title = `${t.akt_advance_title}: ${fmt(state.advance)}`;
      sub = t.akt_advance_sub;
    } else if (state.code === 'debt_0_14') {
      bg = 'bg-amber-50 border-amber-200 text-amber-800';
      icon = '🟡';
      title = `${t.akt_debt_title}: ${fmt(state.debt)}`;
      sub = `${state.days_overdue} ${t.akt_days} · ${t.akt_days_to_call.replace('{n}', Math.max(0, 15 - state.days_overdue))}`;
    } else if (state.code === 'debt_15_29') {
      bg = 'bg-orange-50 border-orange-300 text-orange-900';
      icon = '⚠️';
      title = `${t.akt_debt_title}: ${fmt(state.debt)}`;
      sub = `${state.days_overdue} ${t.akt_days} · ${t.akt_will_call_soon}`;
    } else if (state.code === 'debt_30_plus') {
      bg = 'bg-red-50 border-red-300 text-red-900';
      icon = '🔴';
      title = `${t.akt_debt_overdue}: ${fmt(state.debt)}`;
      sub = `${t.akt_overdue_by} ${state.days_overdue} ${t.akt_days}`;
    } else return null;
    return (
      <div className={`rounded-xl p-3 border ${bg} flex-1 min-w-0`}>
        <div className="flex items-start gap-2">
          <span className="text-base flex-shrink-0 leading-none mt-0.5">{icon}</span>
          <div className="flex-1 min-w-0">
            <div className="text-[13px] font-semibold truncate">{title}</div>
            <div className="text-[10px] mt-0.5 opacity-80 leading-tight">{sub}</div>
          </div>
        </div>
      </div>
    );
  };

  const AktSverkiSection = () => {
    if (!akt || !akt.linked) return null;
    const events = akt.events || [];
    if (events.length === 0) return null;

    // newest first, capped at 8 orders + 8 payments, interleaved chronologically
    const reversed = [...events].reverse();
    const orderRows = reversed.filter(e => e.type === 'order').slice(0, 8);
    const payRows = reversed.filter(e => e.type === 'payment').slice(0, 8);
    const rowsSet = new Set([...orderRows, ...payRows]);
    const rows = reversed.filter(e => rowsSet.has(e));

    return (
      <div className="mb-4">
        <div className="text-sm text-tg-hint mb-2">
          📒 {t.akt_title}
        </div>

        <div className="bg-tg-secondary rounded-xl overflow-hidden divide-y divide-tg-hint/10">
          {rows.map((e) => {
            const isOrder = e.type === 'order';
            const sign = isOrder ? '−' : '+';
            const rowIcon = isOrder ? '🚚' : '💳';
            const uzsAmt = e.uzs_amount || 0;
            const usdAmt = e.usd_amount || 0;
            return (
              <button
                key={`${e.type}-${e.id}`}
                onClick={() => { setAktSheet({ ...e }); setAktSheetItems(null); }}
                className="w-full px-4 py-2.5 flex items-center gap-3 text-left active:bg-tg-bg/30"
              >
                <span className="text-base flex-shrink-0">{rowIcon}</span>
                <div className="flex-1 min-w-0 flex items-center gap-2">
                  <div className="text-sm font-medium whitespace-nowrap">
                    {formatDocDate(e.date)}
                  </div>
                  {isOrder && (
                    <span className="px-2 py-0.5 rounded-md bg-tg-link/10 text-tg-link text-[10px] font-semibold">
                      {t.akt_details_pill}
                    </span>
                  )}
                </div>
                <div className="text-right flex-shrink-0">
                  {uzsAmt > 0 && (
                    <div className={`text-[13px] font-semibold ${isOrder ? 'text-red-500' : 'text-emerald-600'}`}>
                      {sign}{fmtUzs(uzsAmt)}
                    </div>
                  )}
                  {usdAmt > 0 && (
                    <div className={`text-[13px] font-semibold ${isOrder ? 'text-red-500' : 'text-emerald-600'}`}>
                      {sign}{fmtUsd(usdAmt)}
                    </div>
                  )}
                </div>
              </button>
            );
          })}
        </div>
      </div>
    );
  };

  // ── My Business with Rassvet section ──
  const hasBusinessData = spendTrend || (topProducts && (topProducts.uzs?.length > 0 || topProducts.usd?.length > 0)) || activitySummary;

  // For the chart we show only the last 12 months; full data (up to 24)
  // is used for YoY comparison lookups.
  // Exclude current (not yet closed) month from the chart
  const chartData = (() => {
    if (!spendTrend) return null;
    const now = new Date();
    const currentMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    const closed = spendTrend.filter(m => m.month < currentMonth);
    return closed.slice(-15);
  })();

  const MyBusinessSection = () => {
    if (bizLoading || !hasBusinessData) return null;

    const hasUzsTrend = chartData?.some(m => m.total_uzs > 0);
    const hasUsdTrend = chartData?.some(m => m.total_usd > 0);

    // Build chart header + comparisons based on last CLOSED month
    const buildChartInfo = (key, fmtFn, suffix) => {
      if (!chartData || chartData.length < 2) return { header: null, comparisons: [] };

      const now = new Date();
      const currentMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
      const lastEntry = chartData[chartData.length - 1];
      const isCurrentMonth = lastEntry.month === currentMonth;
      const closedIdx = isCurrentMonth ? chartData.length - 2 : chartData.length - 1;
      const prevIdx = closedIdx - 1;

      if (closedIdx < 0) return { header: null, comparisons: [] };
      const closed = chartData[closedIdx];
      const closedVal = closed[key] || 0;
      const closedLabel = fmtMonthFull(closed.month);

      const header = { month: closedLabel, value: `${fmtFn(closedVal)} ${suffix}`.trim() };
      const comparisons = [];

      // Line 1: vs previous month
      if (prevIdx >= 0) {
        const prev = chartData[prevIdx];
        const prevVal = prev[key] || 0;
        const momDiff = closedVal - prevVal;
        comparisons.push({
          vsLabel: `vs ${fmtMonthFull(prev.month)}`,
          diff: momDiff,
          diffStr: fmtFn(Math.abs(momDiff)),
        });
      }

      // Line 2: vs same month last year
      if (spendTrend && closed.month) {
        const mm = closed.month.slice(5, 7);
        const yy = parseInt(closed.month.slice(0, 4), 10);
        const yoyMonth = `${yy - 1}-${mm}`;
        const yoyEntry = spendTrend.find(m => m.month === yoyMonth);
        if (yoyEntry) {
          const yoyDiff = closedVal - (yoyEntry[key] || 0);
          comparisons.push({
            vsLabel: `vs ${fmtMonthFull(yoyMonth)}`,
            diff: yoyDiff,
            diffStr: fmtFn(Math.abs(yoyDiff)),
          });
        }
      }

      return { header, comparisons };
    };

    const uzsInfo = hasUzsTrend ? buildChartInfo('total_uzs', formatUzs, t.balance_currency) : { header: null, comparisons: [] };
    const usdInfo = hasUsdTrend ? buildChartInfo('total_usd', (v) => formatUsd(v), '') : { header: null, comparisons: [] };

    return (
      <div className="mb-4">
        <div className="text-sm text-tg-hint mb-2">
          📊 {t.my_business_title}
        </div>

        {/* Monthly Spend Trend Chart */}
        {chartData && chartData.length > 0 && (
          <div className="bg-tg-secondary rounded-xl p-3 mb-2">
            <div className="text-xs font-semibold mb-1">{t.my_business_spend_trend}</div>
            {hasUzsTrend && (
              <SpendChart data={chartData} valueKey="total_uzs" color="#3B82F6" label={`UZS (${t.balance_currency})`} formatValue={(v) => formatUzs(v)} header={uzsInfo.header} comparisons={uzsInfo.comparisons} />
            )}
            {hasUsdTrend && (
              <SpendChart data={chartData} valueKey="total_usd" color="#10B981" label="USD ($)" formatValue={(v) => formatUsd(v)} header={usdInfo.header} comparisons={usdInfo.comparisons} />
            )}
          </div>
        )}

        {/* Top Products — ranked by spend, split by currency */}
        {topProducts && (topProducts.uzs?.length > 0 || topProducts.usd?.length > 0) && (
          <div className="bg-tg-secondary rounded-xl p-3 mb-2">
            <div className="text-xs font-semibold mb-2">{t.my_business_top_products}</div>

            {topProducts.uzs?.length > 0 && (
              <div className="mb-3">
                <div className="text-[10px] text-tg-hint font-medium mb-1.5">UZS</div>
                <div className="space-y-2">
                  {topProducts.uzs.map((p, i) => {
                    const maxVal = topProducts.uzs[0].total_uzs || 1;
                    const barPct = Math.max(5, Math.round((p.total_uzs / maxVal) * 100));
                    return (
                      <div key={`uzs-${i}`}>
                        <div className="flex items-center justify-between text-xs">
                          <span className="truncate flex-1 mr-2 font-medium">{p.name}</span>
                        </div>
                        <div className="h-1.5 bg-tg-hint/10 rounded-full mt-0.5">
                          <div className="h-1.5 bg-blue-400 rounded-full" style={{ width: `${barPct}%` }} />
                        </div>
                        <div className="flex items-center justify-between text-[10px] text-tg-hint mt-0.5">
                          <span>{formatUzs(p.total_uzs)} {t.balance_currency}</span>
                          <span>{p.order_count} {t.my_business_orders} · {p.total_qty} {t.my_business_items}</span>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            {topProducts.usd?.length > 0 && (
              <div>
                <div className="text-[10px] text-tg-hint font-medium mb-1.5">USD</div>
                <div className="space-y-2">
                  {topProducts.usd.map((p, i) => {
                    const maxVal = topProducts.usd[0].total_usd || 1;
                    const barPct = Math.max(5, Math.round((p.total_usd / maxVal) * 100));
                    return (
                      <div key={`usd-${i}`}>
                        <div className="flex items-center justify-between text-xs">
                          <span className="truncate flex-1 mr-2 font-medium">{p.name}</span>
                        </div>
                        <div className="h-1.5 bg-tg-hint/10 rounded-full mt-0.5">
                          <div className="h-1.5 bg-green-400 rounded-full" style={{ width: `${barPct}%` }} />
                        </div>
                        <div className="flex items-center justify-between text-[10px] text-tg-hint mt-0.5">
                          <span>{formatUsd(p.total_usd)}</span>
                          <span>{p.order_count} {t.my_business_orders} · {p.total_qty} {t.my_business_items}</span>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
          </div>
        )}

        {/* Activity Summary */}
        {activitySummary && (
          <div className="bg-tg-secondary rounded-xl p-3 mb-2">
            <div className="text-xs font-semibold mb-2">{t.my_business_activity}</div>
            {activitySummary.last_active_month ? (
              /* No recent activity — show last active month instead */
              <div className="bg-tg-bg rounded-lg p-3">
                <div className="text-[10px] text-tg-hint mb-1">So'nggi faol oy</div>
                <div className="text-sm font-bold mb-1">{fmtMonthFull(activitySummary.last_active_month.month)}</div>
                <div className="text-[11px] text-tg-hint">
                  {activitySummary.last_active_month.doc_count} {t.my_business_orders}
                  {activitySummary.last_active_month.total_uzs > 0 && (
                    <> · {formatUzs(activitySummary.last_active_month.total_uzs)} {t.balance_currency}</>
                  )}
                  {activitySummary.last_active_month.total_usd > 0 && (
                    <> · {formatUsd(activitySummary.last_active_month.total_usd)}</>
                  )}
                </div>
              </div>
            ) : (
              <div className="grid grid-cols-2 gap-2 text-center">
                {/* This month */}
                <div className="bg-tg-bg rounded-lg p-2">
                  <div className="text-[10px] text-tg-hint">{t.my_business_this_month}</div>
                  <div className="text-lg font-bold">{activitySummary.this_month.doc_count}</div>
                  <div className="text-[10px] text-tg-hint">{t.my_business_orders}</div>
                  {activitySummary.prev_month.doc_count > 0 && (() => {
                    const curr = activitySummary.this_month.doc_count;
                    const prev = activitySummary.prev_month.doc_count;
                    const diff = curr - prev;
                    if (diff === 0) return <div className="text-[10px] text-tg-hint">→ {t.my_business_orders_same}</div>;
                    const arrow = diff > 0 ? '↑' : '↓';
                    const color = diff > 0 ? 'text-green-500' : 'text-red-400';
                    return <div className={`text-[10px] ${color}`}>{arrow} {Math.abs(diff)} {diff > 0 ? t.my_business_orders_up : t.my_business_orders_down}</div>;
                  })()}
                </div>
                {/* Previous month */}
                <div className="bg-tg-bg rounded-lg p-2">
                  <div className="text-[10px] text-tg-hint">{t.my_business_prev_month}</div>
                  <div className="text-lg font-bold">{activitySummary.prev_month.doc_count}</div>
                  <div className="text-[10px] text-tg-hint">{t.my_business_orders}</div>
                </div>
              </div>
            )}

            {/* Lifetime stats */}
            {activitySummary.lifetime.total_orders > 0 && (
              <div className="border-t border-tg-hint/15 mt-2 pt-2">
                <div className="text-[10px] text-tg-hint mb-1">{t.my_business_lifetime}</div>
                <div className="grid grid-cols-3 gap-1 text-center">
                  <div>
                    <div className="text-sm font-bold">{activitySummary.lifetime.total_orders}</div>
                    <div className="text-[9px] text-tg-hint">{t.my_business_total_orders}</div>
                  </div>
                  <div>
                    <div className="text-sm font-bold">
                      {formatUzs(activitySummary.lifetime.avg_order_uzs)}
                      {activitySummary.lifetime.avg_order_usd > 0 && (
                        <span className="text-tg-hint/60"> / </span>
                      )}
                      {activitySummary.lifetime.avg_order_usd > 0 && formatUsd(activitySummary.lifetime.avg_order_usd)}
                    </div>
                    <div className="text-[9px] text-tg-hint">{t.my_business_avg_order}</div>
                  </div>
                  <div>
                    <div className="text-sm font-bold">{activitySummary.lifetime.first_order ? fmtMonthLabel(activitySummary.lifetime.first_order.slice(0, 7)) : '—'}</div>
                    <div className="text-[9px] text-tg-hint">{t.my_business_history_start}</div>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    );
  };

  const handleShareLocation = () => {
    window.Telegram?.WebApp?.openTelegramLink('https://t.me/samrassvetbot?start=share_location');
    setTimeout(() => window.Telegram?.WebApp?.close(), 300);
  };

  return (
    <div>
      {/* Client 1C name — identifies which 1C account the Telegram user is linked to */}
      {akt?.client_1c_name && (
        <div className="mb-3 text-center">
          <div className="text-[10px] text-tg-hint uppercase tracking-wide">
            {t.akt_client_header}
          </div>
          <div className="text-base font-semibold mt-0.5">
            {akt.client_1c_name}
          </div>
        </div>
      )}

      {/* Location card */}
      {userLocation ? (
        <div className="bg-tg-secondary rounded-xl p-3 mb-3 flex items-center gap-2">
          <span className="text-base">📍</span>
          <div className="flex-1 min-w-0">
            <div className="text-xs font-medium truncate">{userLocation.address || "Joylashuv saqlangan"}</div>
          </div>
          <button
            onClick={handleShareLocation}
            className="text-[10px] px-2.5 py-1 rounded-lg bg-tg-bg text-tg-link whitespace-nowrap"
          >
            Yangilash
          </button>
        </div>
      ) : (
        <button
          onClick={handleShareLocation}
          className="w-full bg-tg-secondary rounded-xl p-3 mb-3 flex items-center gap-2 active:opacity-80 transition-opacity"
        >
          <span className="text-base">📍</span>
          <div className="flex-1 text-left">
            <div className="text-xs font-medium">Joylashuvni saqlash</div>
            <div className="text-[10px] text-tg-hint">Telegram orqali joylashuvingizni yuboring</div>
          </div>
          <span className="text-tg-link text-xs">→</span>
        </button>
      )}

      <MyBusinessSection />
      <BalanceCard />

      {lastOrder && (
        <>
          <div className="text-sm text-tg-hint mb-2">
            📝 {t.wishlist_orders_title}
            <span className="text-[10px] ml-1 opacity-60">· {t.wishlist_orders_subtitle}</span>
          </div>

          <div className="space-y-2">
            {orders.slice(0, 3).map((ord) => {
              const expanded = expandedId === ord.id;
              return (
                <div key={ord.id} className="bg-tg-secondary rounded-xl overflow-hidden">
                  <button
                    onClick={() => toggleExpand(ord.id)}
                    className="w-full text-left p-3 active:bg-tg-hint/10 transition-colors"
                  >
                    <div className="flex items-center justify-between">
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="text-sm font-semibold">#{ord.id}</span>
                          <span className="text-xs px-2 py-0.5 rounded-full bg-tg-bg text-tg-hint">
                            {STATUS_ICONS[ord.status]} {STATUS_LABELS[ord.status] || ord.status}
                          </span>
                        </div>
                        <div className="text-xs text-tg-hint mt-1">
                          {formatDate(ord.created_at)} · {ord.item_count} {t.items_count}
                        </div>
                        {/* Identifier — phone (preferred) or Telegram ID, so the client
                            can tell which account/employee placed this order */}
                        {(ord.client_phone || ord.telegram_id) && (
                          <div className="text-[10px] text-tg-hint/80 mt-0.5 truncate">
                            {ord.client_phone
                              ? `📞 ${ord.client_phone}`
                              : `🆔 ${ord.telegram_id}`}
                          </div>
                        )}
                      </div>
                      <div className="text-right ml-2">
                        {ord.total_usd > 0 && (
                          <div className="text-sm font-bold">{formatCartPrice(ord.total_usd, 'USD')}</div>
                        )}
                        {ord.total_uzs > 0 && (
                          <div className="text-sm font-bold">{formatCartPrice(ord.total_uzs, 'UZS')}</div>
                        )}
                        <div className="text-xs text-tg-hint mt-0.5">
                          {expanded ? '▲' : '▼'}
                        </div>
                      </div>
                    </div>
                  </button>

                  {expanded && (
                    <div className="border-t border-tg-hint/20 px-3 pb-3">
                      {loadingDetail ? (
                        <div className="text-center py-4 text-tg-hint text-sm">{t.loading}</div>
                      ) : (
                        <>
                          <div className="mt-2 space-y-1.5">
                            {expandedItems.map((item, idx) => (
                              <div key={idx} className="flex items-center gap-2 py-1">
                                <div className="flex-1 min-w-0">
                                  <div className="text-xs font-medium truncate">{item.product_name}</div>
                                  {item.producer_name && (
                                    <div className="text-[10px] text-tg-hint">{item.producer_name}</div>
                                  )}
                                </div>
                                <div className="text-xs text-tg-hint whitespace-nowrap">
                                  {item.quantity} {item.unit}
                                </div>
                                <div className="text-xs font-semibold whitespace-nowrap min-w-[50px] text-right">
                                  {formatCartPrice(item.price * item.quantity, item.currency)}
                                </div>
                              </div>
                            ))}
                          </div>

                          <div className="grid grid-cols-2 gap-2 mt-3">
                            <button
                              onClick={(e) => {
                                e.stopPropagation();
                                handleReorderTap(ord.id);
                              }}
                              disabled={reordering}
                              className="bg-tg-button text-tg-button-text rounded-xl py-2.5 font-semibold text-sm active:scale-95 transition-transform disabled:opacity-50"
                            >
                              {reordering ? t.loading : `🔄 ${t.reorder}`}
                            </button>
                            <button
                              onClick={(e) => {
                                e.stopPropagation();
                                openCompare({
                                  wishlistOrderId: ord.id,
                                  sourceLabel: `#${ord.id}`,
                                });
                              }}
                              className="bg-tg-secondary border border-tg-button text-tg-button rounded-xl py-2.5 font-semibold text-sm active:scale-95 transition-transform"
                            >
                              🔀 {t.compare_button}
                            </button>
                          </div>
                        </>
                      )}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </>
      )}

      {/* ── Акт сверки (unified timeline + hero status + FIFO links) ── */}
      <AktSverkiSection />

      {/* ── Legacy: Real orders 1C drill-down list (still shown under akt-sverki for the items view) ── */}
      {false && realOrders.length > 0 && (
        <div className="mb-4">
          <div className="text-sm text-tg-hint mb-2">
            🚚 {t.real_orders_title}
            <span className="text-[10px] ml-1 opacity-60">· {t.real_orders_subtitle}</span>
          </div>
          <div className="space-y-2">
            {realOrders.slice(0, 10).map((ro) => {
              const isOpen = expandedRealId === ro.id;
              return (
                <div key={ro.id} className="bg-tg-secondary rounded-xl overflow-hidden">
                  <button
                    onClick={() => toggleExpandReal(ro.id)}
                    className="w-full text-left p-3 active:bg-tg-hint/10 transition-colors"
                  >
                    <div className="flex items-center justify-between">
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="text-sm font-semibold truncate">
                            {t.real_order_doc} {ro.doc_number_1c}
                          </span>
                        </div>
                        <div className="text-xs text-tg-hint mt-1">
                          {formatDocDate(ro.doc_date)} · {ro.item_count} {t.real_order_items_count}
                        </div>
                        {ro.sale_agent && (
                          <div className="text-[10px] text-tg-hint mt-0.5 truncate">
                            {t.real_order_sale_agent}: {ro.sale_agent}
                          </div>
                        )}
                      </div>
                      <div className="text-right ml-2">
                        {/* Some 1C "Реализация" exports tag the doc currency
                            as USD (the contract currency) but record all
                            prices in UZS; other docs mix both sides across
                            line items. Show whichever side(s) have data:
                            both lines for mixed-currency docs, one line
                            for single-currency docs, '—' if nothing. */}
                        {(() => {
                          const hasUzs = (ro.total_sum || 0) > 0;
                          const hasUsd = (ro.total_sum_currency || 0) > 0;
                          if (!hasUzs && !hasUsd) {
                            return <div className="text-sm font-bold whitespace-nowrap">—</div>;
                          }
                          return (
                            <>
                              {hasUzs && (
                                <div className="text-sm font-bold whitespace-nowrap">
                                  {formatUzs(ro.total_sum)} {t.balance_currency}
                                </div>
                              )}
                              {hasUsd && (
                                <div className="text-sm font-bold whitespace-nowrap">
                                  {formatUsd(ro.total_sum_currency)}
                                </div>
                              )}
                            </>
                          );
                        })()}
                        <div className="text-xs text-tg-hint mt-0.5">
                          {isOpen ? '▲' : '▼'}
                        </div>
                      </div>
                    </div>
                  </button>

                  {isOpen && (
                    <div className="border-t border-tg-hint/20 px-3 pb-3">
                      {loadingRealDetail ? (
                        <div className="text-center py-4 text-tg-hint text-sm">{t.loading}</div>
                      ) : (
                        <>
                          <div className="mt-2 space-y-1.5">
                            {expandedRealItems.map((item, idx) => (
                              <div key={idx} className="flex items-center gap-2 py-1">
                                <div className="flex-1 min-w-0">
                                  <div className="text-xs font-medium truncate">
                                    {/* Session A policy: real-orders history is
                                        1C data — render the raw Cyrillic name so
                                        the sales team can reconcile against 1C.
                                        The in-app catalog/cart UI still shows the
                                        cleaned Latin name_display. Same rule also
                                        applies to wish-list orders after placement
                                        (see export.py commit 325b4cc + the
                                        backfill-order-item-names admin endpoint). */}
                                    {item.product_name_1c || item.name_display}
                                  </div>
                                </div>
                                <div className="text-xs text-tg-hint whitespace-nowrap">
                                  {item.quantity}
                                </div>
                                <div className="text-xs font-semibold whitespace-nowrap min-w-[60px] text-right">
                                  {/* Same currency-agnostic fallback as the
                                      header total — show whichever side has
                                      data, prefer UZS. */}
                                  {(item.total_local || 0) > 0
                                    ? formatUzs(item.total_local)
                                    : (item.total_currency || 0) > 0
                                      ? formatUsd(item.total_currency)
                                      : '—'}
                                </div>
                              </div>
                            ))}
                          </div>

                        </>
                      )}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Financial behaviour score — pinned to the bottom of the Cabinet,
          directly below real orders. Backend continues to compute via
          /api/finance/credit-score; the card shows only the arc + number +
          improvement hints per product-owner call. */}
      <CreditScoreCard />

      {/* Акт сверки — tap sheet (shows FIFO links for the tapped row) */}
      {aktSheet && (
        <>
          <div className="fixed inset-0 bg-black/40 z-[100]" onClick={() => setAktSheet(null)} />
          <div className="fixed bottom-0 left-0 right-0 z-[101] bg-tg-bg rounded-t-2xl p-5 pb-8 shadow-2xl max-h-[80vh] overflow-y-auto">
            <div className="w-10 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />
            {aktSheet.type === 'payment' ? (
              <>
                <div className="text-center mb-4">
                  <div className="text-[11px] text-tg-hint uppercase tracking-wide">{t.akt_payment}</div>
                  <div className="mt-1 space-y-0.5">
                    {(aktSheet.uzs_amount || 0) > 0 && (
                      <div className="text-xl font-bold text-emerald-600">+{fmtUzs(aktSheet.uzs_amount)}</div>
                    )}
                    {(aktSheet.usd_amount || 0) > 0 && (
                      <div className="text-xl font-bold text-emerald-600">+{fmtUsd(aktSheet.usd_amount)}</div>
                    )}
                  </div>
                  <div className="text-xs text-tg-hint mt-1">{formatDocDate(aktSheet.date)}</div>
                </div>
                {['uzs', 'usd'].map((ccy) => {
                  const covers = aktSheet[`${ccy}_covers`] || [];
                  const adv = aktSheet[`${ccy}_advance_created`] || 0;
                  const fmt = ccy === 'usd' ? fmtUsd : fmtUzs;
                  const amt = aktSheet[`${ccy}_amount`] || 0;
                  if (amt <= 0) return null;
                  return (
                    <div key={ccy} className="mb-3">
                      <div className="text-[11px] text-tg-hint uppercase tracking-wide mb-1.5">
                        {ccy.toUpperCase()} — {t.akt_covers_fifo}
                      </div>
                      {covers.length > 0 ? (
                        <div className="space-y-1.5">
                          {covers.map((c, i) => (
                            <div key={i} className="bg-tg-secondary rounded-lg px-3 py-2 flex items-center gap-2">
                              <span className="text-xs">{c.fully_closed ? '✅' : '🟡'}</span>
                              <div className="flex-1 min-w-0">
                                <div className="text-sm font-medium">
                                  {t.akt_order_doc} {c.order_doc || `#${c.order_id}`}
                                </div>
                                <div className="text-[11px] text-tg-hint">
                                  {formatDocDate(c.order_date)} · {c.fully_closed ? t.akt_fully_closed : t.akt_partially_closed}
                                </div>
                              </div>
                              <div className="text-sm font-semibold">{fmt(c.amount)}</div>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="text-[11px] text-tg-hint italic">{t.akt_no_links}</div>
                      )}
                      {adv > 0.01 && (
                        <div className="mt-2 bg-emerald-50 border border-emerald-200 rounded-lg px-3 py-2 text-xs text-emerald-800">
                          💚 {t.akt_created_advance_full}: <b>{fmt(adv)}</b>
                        </div>
                      )}
                    </div>
                  );
                })}
                <div className="mt-2 text-[10px] text-tg-hint opacity-70">
                  {t.akt_fifo_disclaimer}
                </div>
              </>
            ) : (
              <>
                <div className="text-center mb-4">
                  <div className="text-[11px] text-tg-hint uppercase tracking-wide">{t.akt_order_doc} {aktSheet.doc_number}</div>
                  <div className="mt-1 space-y-0.5">
                    {(aktSheet.uzs_amount || 0) > 0 && (
                      <div className="text-xl font-bold text-red-500">−{fmtUzs(aktSheet.uzs_amount)}</div>
                    )}
                    {(aktSheet.usd_amount || 0) > 0 && (
                      <div className="text-xl font-bold text-red-500">−{fmtUsd(aktSheet.usd_amount)}</div>
                    )}
                  </div>
                  <div className="text-xs text-tg-hint mt-1">{formatDocDate(aktSheet.date)}</div>
                </div>

                {/* Items */}
                <div className="mb-3">
                  <div className="text-xs text-tg-hint mb-2">{t.real_order_view_items}:</div>
                  {aktSheetItems === null ? (
                    <AktSheetItemsLoader orderId={aktSheet.id} onLoaded={setAktSheetItems} />
                  ) : aktSheetItems.length > 0 ? (
                    <div className="space-y-1 max-h-60 overflow-y-auto">
                      {aktSheetItems.map((item, idx) => (
                        <div key={idx} className="flex items-center gap-2 py-1 border-b border-tg-hint/10">
                          <div className="flex-1 min-w-0 text-[12px] truncate">
                            {item.product_name_1c || item.name_display}
                          </div>
                          <div className="text-[11px] text-tg-hint whitespace-nowrap">{item.quantity}</div>
                          <div className="text-[11px] font-semibold whitespace-nowrap min-w-[72px] text-right">
                            {(item.total_local || 0) > 0
                              ? formatUzs(item.total_local)
                              : (item.total_currency || 0) > 0
                                ? formatUsd(item.total_currency)
                                : '—'}
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="text-[11px] text-tg-hint">—</div>
                  )}
                </div>

                {/* Paid by — per currency */}
                {['uzs', 'usd'].map((ccy) => {
                  const paidBy = aktSheet[`${ccy}_paid_by`] || [];
                  const amt = aktSheet[`${ccy}_amount`] || 0;
                  const fmt = ccy === 'usd' ? fmtUsd : fmtUzs;
                  if (amt <= 0 || paidBy.length === 0) return null;
                  return (
                    <div key={ccy} className="mb-3">
                      <div className="text-[11px] text-tg-hint uppercase tracking-wide mb-1.5">
                        {ccy.toUpperCase()} — {t.akt_paid_by}
                      </div>
                      <div className="space-y-1.5">
                        {paidBy.map((p, i) => (
                          <div key={i} className="bg-tg-secondary rounded-lg px-3 py-2 flex items-center gap-2">
                            <span className="text-xs">{p.kind === 'advance' ? '💚' : '💳'}</span>
                            <div className="flex-1 min-w-0">
                              <div className="text-sm font-medium">
                                {p.kind === 'advance' ? t.akt_from_advance : t.akt_payment}
                              </div>
                              <div className="text-[11px] text-tg-hint">{formatDocDate(p.date)}</div>
                            </div>
                            <div className="text-sm font-semibold">{fmt(p.amount)}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  );
                })}
              </>
            )}

            {/* Comment + photo feedback form (only for real orders) */}
            {aktSheet.type === 'order' && (
              <OrderIssueForm
                order={aktSheet}
                onDone={() => setAktSheet(null)}
                t={t}
              />
            )}

            {aktSheet.type !== 'order' && (
              <button
                onClick={() => setAktSheet(null)}
                className="mt-5 w-full py-2.5 rounded-xl bg-tg-secondary text-sm font-medium"
              >
                {t.reorder_cancel}
              </button>
            )}
          </div>
        </>
      )}

      {/* Reorder confirmation dialog */}
      {reorderDialog && (
        <>
          <div
            className="fixed inset-0 bg-black/40 z-[100]"
            onClick={() => setReorderDialog(null)}
          />
          <div className="fixed bottom-0 left-0 right-0 z-[101] bg-tg-bg rounded-t-2xl p-5 pb-8 shadow-2xl">
            <div className="w-10 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />
            <div className="text-center mb-5">
              <div className="text-base font-semibold">{t.reorder_cart_has_items}</div>
              <div className="text-xs text-tg-hint mt-1">
                {t.cart}: {cart.totalCount} {t.products_count}
              </div>
            </div>
            <div className="space-y-2.5">
              <button
                onClick={() => doReorder(reorderDialog, 'replace')}
                className="w-full bg-tg-button text-tg-button-text rounded-xl py-3 font-semibold text-sm"
              >
                🔄 {t.reorder_replace}
              </button>
              <button
                onClick={() => doReorder(reorderDialog, 'merge')}
                className="w-full bg-tg-secondary text-tg-text rounded-xl py-3 font-semibold text-sm"
              >
                ➕ {t.reorder_merge}
              </button>
              <button
                onClick={() => setReorderDialog(null)}
                className="w-full text-tg-hint text-sm py-2"
              >
                {t.reorder_cancel}
              </button>
            </div>
          </div>
        </>
      )}

      {/* Toast notification */}
      {toast && (
        <div className="fixed top-16 left-4 right-4 z-[200] bg-green-600 text-white rounded-xl py-3 px-4 text-center text-sm font-medium shadow-lg">
          {toast}
        </div>
      )}
    </div>
  );
}
