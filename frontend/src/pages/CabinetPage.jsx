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

// ── Mini SVG line chart (pure, no external lib) ──
function SpendChart({ data, valueKey, color, label, formatValue, summaryLine }) {
  const [tappedIdx, setTappedIdx] = useState(null);
  if (!data || data.length === 0) return null;
  const values = data.map(d => d[valueKey] || 0);
  const maxVal = Math.max(...values, 1);
  const W = 300, H = 140, PAD_TOP = 28, PAD_BOT = 22, PAD_LEFT = 4, PAD_RIGHT = 4;
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
      {label && <div className="text-[10px] text-tg-hint mb-1">{label}</div>}
      <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ maxHeight: '140px' }}>
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
        {/* Dots + month labels — tappable */}
        {points.map((p, i) => (
          <g key={i} onClick={() => handleDotTap(i)} style={{ cursor: 'pointer' }}>
            {/* Invisible larger tap target */}
            <circle cx={p.x} cy={p.y} r="12" fill="transparent" />
            <circle cx={p.x} cy={p.y} r={tappedIdx === i ? 5 : 3} fill={color} />
            <text x={p.x} y={H - 4} textAnchor="middle" fontSize="8" fill="var(--tg-theme-hint-color, #999)">{fmtMonthLabel(data[i].month)}</text>
            {/* Tooltip on tap */}
            {tappedIdx === i && (
              <>
                <rect x={Math.max(2, Math.min(p.x - 40, W - 82))} y={Math.max(2, p.y - 22)} width="80" height="16" rx="4" fill="var(--tg-theme-bg-color, #fff)" stroke={color} strokeWidth="0.5" />
                <text x={Math.max(42, Math.min(p.x, W - 42))} y={Math.max(13, p.y - 10)} textAnchor="middle" fontSize="9" fontWeight="600" fill={color}>
                  {formatValue ? formatValue(p.v) : p.v.toLocaleString('ru-RU')}
                </text>
              </>
            )}
          </g>
        ))}
      </svg>
      {/* Per-chart summary line */}
      {summaryLine && <div className="text-[10px] text-tg-hint text-center mt-0.5">{summaryLine}</div>}
    </div>
  );
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
  const [balanceLoading, setBalanceLoading] = useState(true);
  const [realOrders, setRealOrders] = useState([]);
  const [realLoading, setRealLoading] = useState(true);
  const [expandedRealId, setExpandedRealId] = useState(null);
  const [expandedRealItems, setExpandedRealItems] = useState([]);
  const [loadingRealDetail, setLoadingRealDetail] = useState(false);
  const [compareModal, setCompareModal] = useState(null); // { kind, sourceLabel, orders, loading }

  // Rassvet Plus — business intelligence state
  const [spendTrend, setSpendTrend] = useState(null);
  const [topProducts, setTopProducts] = useState(null);
  const [activitySummary, setActivitySummary] = useState(null);
  const [bizLoading, setBizLoading] = useState(true);

  const userId = getTelegramUserId();

  // Load orders and balance
  useEffect(() => {
    if (!userId) {
      setLoading(false);
      setBalanceLoading(false);
      setBizLoading(false);
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

    // Rassvet Plus — fetch business intelligence data
    Promise.all([
      fetch(`${API}/spend-trend?telegram_id=${userId}&months=12`).then(r => r.json()),
      fetch(`${API}/top-products?telegram_id=${userId}&limit=5`).then(r => r.json()),
      fetch(`${API}/activity-summary?telegram_id=${userId}`).then(r => r.json()),
    ])
      .then(([trend, products, activity]) => {
        if (trend.ok && trend.months?.length > 0) setSpendTrend(trend.months);
        if (products.ok && products.products?.length > 0) setTopProducts(products.products);
        if (activity.ok && activity.summary) setActivitySummary(activity.summary);
        setBizLoading(false);
      })
      .catch(() => setBizLoading(false));
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

  // Open compare modal — find counterpart of a real order or wishlist order
  const openCompare = async ({ realOrderId, wishlistOrderId, sourceLabel }) => {
    setCompareModal({ kind: realOrderId ? 'real' : 'wishlist', sourceLabel, orders: [], loading: true });
    try {
      const param = realOrderId
        ? `real_order_id=${realOrderId}`
        : `wishlist_order_id=${wishlistOrderId}`;
      const res = await fetch(`${API}/compare?telegram_id=${userId}&${param}&days=5`);
      const data = await res.json();
      // Returned `kind` describes the result set: when looking up from a real
      // order we get wish-list orders back, and vice versa.
      const resultKind = realOrderId ? 'wishlist' : 'real';
      setCompareModal({
        kind: resultKind,
        sourceLabel,
        orders: data.orders || [],
        loading: false,
      });
    } catch {
      setCompareModal((prev) => prev && { ...prev, orders: [], loading: false });
    }
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

  // ── My Business with Rassvet section ──
  const hasBusinessData = spendTrend || topProducts || activitySummary;

  const MyBusinessSection = () => {
    if (bizLoading || !hasBusinessData) return null;

    const hasUzsTrend = spendTrend?.some(m => m.total_uzs > 0);
    const hasUsdTrend = spendTrend?.some(m => m.total_usd > 0);

    return (
      <div className="mb-4">
        <div className="text-sm text-tg-hint mb-2">
          📊 {t.my_business_title}
        </div>

        {/* Monthly Spend Trend Chart */}
        {spendTrend && spendTrend.length > 0 && (() => {
          // Build per-chart summary lines
          const makeSummary = (key, fmtFn, suffix) => {
            if (spendTrend.length < 2) return null;
            const last = spendTrend[spendTrend.length - 1];
            const prev = spendTrend[spendTrend.length - 2];
            const diff = (last[key] || 0) - (prev[key] || 0);
            const arrow = diff > 0 ? '↑' : diff < 0 ? '↓' : '→';
            return `${fmtMonthLabel(last.month)}: ${fmtFn(last[key] || 0)} ${suffix} ${arrow} ${diff !== 0 ? fmtFn(Math.abs(diff)) : t.my_business_orders_same}`;
          };
          const uzsSummary = hasUzsTrend ? makeSummary('total_uzs', formatUzs, t.balance_currency) : null;
          const usdSummary = hasUsdTrend ? makeSummary('total_usd', (v) => formatUsd(v), '') : null;

          return (
            <div className="bg-tg-secondary rounded-xl p-3 mb-2">
              <div className="text-xs font-semibold mb-1">{t.my_business_spend_trend}</div>
              {hasUzsTrend && (
                <SpendChart data={spendTrend} valueKey="total_uzs" color="#3B82F6" label={`UZS (${t.balance_currency})`} formatValue={(v) => formatUzs(v)} summaryLine={uzsSummary} />
              )}
              {hasUsdTrend && (
                <SpendChart data={spendTrend} valueKey="total_usd" color="#10B981" label="USD ($)" formatValue={(v) => formatUsd(v)} summaryLine={usdSummary} />
              )}
            </div>
          );
        })()}

        {/* Top Products */}
        {topProducts && topProducts.length > 0 && (
          <div className="bg-tg-secondary rounded-xl p-3 mb-2">
            <div className="text-xs font-semibold mb-2">{t.my_business_top_products}</div>
            <div className="space-y-1.5">
              {topProducts.map((p, i) => {
                const maxUzs = topProducts[0].total_uzs || 1;
                const barPct = Math.max(5, Math.round((p.total_uzs / maxUzs) * 100));
                return (
                  <div key={i}>
                    <div className="flex items-center justify-between text-xs">
                      <span className="truncate flex-1 mr-2">{p.name}</span>
                      <span className="text-tg-hint whitespace-nowrap">
                        {p.total_uzs > 0 ? formatUzs(p.total_uzs) : formatUsd(p.total_usd)}
                      </span>
                    </div>
                    <div className="h-1 bg-tg-hint/10 rounded-full mt-0.5">
                      <div className="h-1 bg-blue-400 rounded-full" style={{ width: `${barPct}%` }} />
                    </div>
                    <div className="text-[10px] text-tg-hint mt-0.5">
                      {p.total_qty} {t.my_business_items} · {p.order_count} {t.my_business_orders}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* Activity Summary */}
        {activitySummary && (
          <div className="bg-tg-secondary rounded-xl p-3 mb-2">
            <div className="text-xs font-semibold mb-2">{t.my_business_activity}</div>
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
                    <div className="text-sm font-bold">{formatUzs(activitySummary.lifetime.avg_order_uzs)}</div>
                    {activitySummary.lifetime.avg_order_usd > 0 && (
                      <div className="text-[10px] font-semibold text-tg-hint">{formatUsd(activitySummary.lifetime.avg_order_usd)}</div>
                    )}
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

  return (
    <div>
      <MyBusinessSection />
      <BalanceCard />

      {lastOrder && (
        <>
          <div className="text-sm text-tg-hint mb-2">
            📝 {t.wishlist_orders_title}
            <span className="text-[10px] ml-1 opacity-60">· {t.wishlist_orders_subtitle}</span>
          </div>

          <div className="bg-tg-secondary rounded-xl overflow-hidden">
            {/* Order summary row */}
            <button
              onClick={() => toggleExpand(lastOrder.id)}
              className="w-full text-left p-3 active:bg-tg-hint/10 transition-colors"
            >
              <div className="flex items-center justify-between">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-semibold">
                      #{lastOrder.id}
                    </span>
                    <span className="text-xs px-2 py-0.5 rounded-full bg-tg-bg text-tg-hint">
                      {STATUS_ICONS[lastOrder.status]} {STATUS_LABELS[lastOrder.status] || lastOrder.status}
                    </span>
                  </div>
                  <div className="text-xs text-tg-hint mt-1">
                    {formatDate(lastOrder.created_at)} · {lastOrder.item_count} {t.items_count}
                  </div>
                </div>
                <div className="text-right ml-2">
                  {lastOrder.total_usd > 0 && (
                    <div className="text-sm font-bold">{formatCartPrice(lastOrder.total_usd, 'USD')}</div>
                  )}
                  {lastOrder.total_uzs > 0 && (
                    <div className="text-sm font-bold">{formatCartPrice(lastOrder.total_uzs, 'UZS')}</div>
                  )}
                  <div className="text-xs text-tg-hint mt-0.5">
                    {isExpanded ? '▲' : '▼'}
                  </div>
                </div>
              </div>
            </button>

            {/* Expanded detail */}
            {isExpanded && (
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
                          handleReorderTap(lastOrder.id);
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
                            wishlistOrderId: lastOrder.id,
                            sourceLabel: `#${lastOrder.id}`,
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
        </>
      )}

      {/* ── Real orders (1C shipments) ── */}
      {realOrders.length > 0 && (
        <div className="mb-4">
          <div className="text-sm text-tg-hint mb-2">
            🚚 {t.real_orders_title}
            <span className="text-[10px] ml-1 opacity-60">· {t.real_orders_subtitle}</span>
          </div>
          <div className="space-y-2">
            {realOrders.slice(0, 5).map((ro) => {
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

                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              openCompare({
                                realOrderId: ro.id,
                                sourceLabel: `${t.real_order_doc} ${ro.doc_number_1c}`,
                              });
                            }}
                            className="w-full mt-3 bg-tg-button text-tg-button-text rounded-xl py-2.5 font-semibold text-sm active:scale-95 transition-transform"
                          >
                            🔀 {t.compare_button}
                          </button>
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

      {/* Compare modal */}
      {compareModal && (
        <>
          <div
            className="fixed inset-0 bg-black/50 z-[100]"
            onClick={() => setCompareModal(null)}
          />
          <div className="fixed bottom-0 left-0 right-0 z-[101] bg-tg-bg rounded-t-2xl p-5 pb-8 shadow-2xl max-h-[80vh] overflow-y-auto">
            <div className="w-10 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />
            <div className="text-center mb-4">
              <div className="text-base font-semibold">🔀 {t.compare_title}</div>
              <div className="text-xs text-tg-hint mt-1">
                {compareModal.sourceLabel}
              </div>
            </div>

            {compareModal.loading ? (
              <div className="text-center py-8 text-tg-hint text-sm">{t.loading}</div>
            ) : compareModal.orders.length === 0 ? (
              <div className="text-center py-8">
                <div className="text-3xl mb-2">🤷</div>
                <div className="text-sm font-medium">{t.compare_no_match}</div>
                <div className="text-xs text-tg-hint mt-1">{t.compare_no_match_desc}</div>
              </div>
            ) : (
              <div className="space-y-2">
                <div className="text-xs text-tg-hint mb-1">
                  {compareModal.kind === 'real'
                    ? t.compare_real_side
                    : t.compare_wishlist_side}
                  {' · '}
                  {compareModal.orders.length} {t.compare_match_found}
                </div>
                {compareModal.orders.map((o) => (
                  <div key={o.id} className="bg-tg-secondary rounded-xl p-3">
                    {compareModal.kind === 'real' ? (
                      <>
                        <div className="text-sm font-semibold">
                          {t.real_order_doc} {o.doc_number_1c}
                        </div>
                        <div className="text-xs text-tg-hint mt-1">
                          {formatDocDate(o.doc_date)} · {o.item_count} {t.real_order_items_count}
                        </div>
                        {/* Mixed-currency docs may have both sides
                            populated — show both lines, see comment
                            on the main real-orders list header. */}
                        {(() => {
                          const hasUzs = (o.total_sum || 0) > 0;
                          const hasUsd = (o.total_sum_currency || 0) > 0;
                          if (!hasUzs && !hasUsd) {
                            return <div className="text-sm font-bold mt-1">—</div>;
                          }
                          return (
                            <>
                              {hasUzs && (
                                <div className="text-sm font-bold mt-1">
                                  {formatUzs(o.total_sum)} {t.balance_currency}
                                </div>
                              )}
                              {hasUsd && (
                                <div className="text-sm font-bold mt-1">
                                  {formatUsd(o.total_sum_currency)}
                                </div>
                              )}
                            </>
                          );
                        })()}
                      </>
                    ) : (
                      <>
                        <div className="text-sm font-semibold">#{o.id}</div>
                        <div className="text-xs text-tg-hint mt-1">
                          {formatDate(o.created_at)} · {o.item_count} {t.items_count}
                        </div>
                        {o.total_uzs > 0 && (
                          <div className="text-sm font-bold mt-1">
                            {formatCartPrice(o.total_uzs, 'UZS')}
                          </div>
                        )}
                        {o.total_usd > 0 && (
                          <div className="text-sm font-bold">
                            {formatCartPrice(o.total_usd, 'USD')}
                          </div>
                        )}
                      </>
                    )}
                  </div>
                ))}
              </div>
            )}

            <button
              onClick={() => setCompareModal(null)}
              className="w-full mt-4 text-tg-hint text-sm py-2"
            >
              {t.close}
            </button>
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
