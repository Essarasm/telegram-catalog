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
  return '$' + Math.abs(Math.round(v)).toLocaleString('en-US');
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

function formatShortMonth(start) {
  if (!start) return '';
  try {
    const d = new Date(start);
    return MONTHS[d.getMonth()];
  } catch {
    return '';
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
  const [history, setHistory] = useState(null);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyCurrency, setHistoryCurrency] = useState('USD');

  const userId = getTelegramUserId();

  // Load orders and balance
  useEffect(() => {
    if (!userId) {
      setLoading(false);
      setBalanceLoading(false);
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
  }, [userId]);

  // Load history when opened
  const toggleHistory = () => {
    if (historyOpen) {
      setHistoryOpen(false);
      return;
    }
    setHistoryOpen(true);
    if (history) return; // already loaded

    setHistoryLoading(true);
    fetch(`${FINANCE_API}/balance-history?telegram_id=${userId}`)
      .then(r => r.json())
      .then(data => {
        if (data.ok && data.history) {
          setHistory(data.history);
          // Default to USD if available, else UZS
          if (data.history.USD && data.history.USD.length > 0) {
            setHistoryCurrency('USD');
          } else if (data.history.UZS && data.history.UZS.length > 0) {
            setHistoryCurrency('UZS');
          }
        }
        setHistoryLoading(false);
      })
      .catch(() => setHistoryLoading(false));
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

  // ── Balance Card ──
  const BalanceCard = () => {
    if (balanceLoading || !balance) return null;

    const currencies = balance.balances_by_currency || {};
    const uzs = currencies.UZS || {
      balance: balance.balance || 0,
      period_debit: balance.period_debit || 0,
      period_credit: balance.period_credit || 0,
      period_start: balance.period_start,
    };
    const usd = currencies.USD;

    const renderCurrencyBalance = (data, formatFn, suffix) => {
      if (!data) return null;
      const bal = data.balance || 0;
      const isDebt = bal > 0;
      return (
        <div className="mb-2">
          <div className="text-center mb-2">
            <div className={`text-xl font-bold ${isDebt ? 'text-red-500' : 'text-green-500'}`}>
              {isDebt ? '' : '−'}{formatFn(bal)} {suffix}
            </div>
          </div>
          <div className="flex justify-between text-xs">
            <div className="text-center flex-1">
              <div className="text-tg-hint">{t.balance_shipped}</div>
              <div className="font-semibold mt-0.5">{formatFn(data.period_debit || 0)}</div>
            </div>
            <div className="w-px bg-tg-hint/20" />
            <div className="text-center flex-1">
              <div className="text-tg-hint">{t.balance_paid}</div>
              <div className="font-semibold mt-0.5 text-green-600">{formatFn(data.period_credit || 0)}</div>
            </div>
          </div>
        </div>
      );
    };

    return (
      <div className="mb-4">
        <div className="text-sm text-tg-hint mb-2">{t.balance_title}</div>
        <div className="bg-tg-secondary rounded-xl p-4">
          <div className="text-xs text-tg-hint text-center mb-2">{t.balance_current}</div>

          {renderCurrencyBalance(uzs, formatUzs, t.balance_currency || "so'm")}

          {usd && (
            <>
              <div className="border-t border-tg-hint/20 my-2" />
              {renderCurrencyBalance(usd, formatUsd, '')}
            </>
          )}

          <div className="text-[10px] text-tg-hint text-center mt-2">
            {formatPeriod(uzs.period_start || balance.period_start)} · {t.balance_updated}: {formatDate(balance.imported_at)}
          </div>

          {/* History toggle button */}
          <button
            onClick={toggleHistory}
            className="w-full mt-3 pt-2 border-t border-tg-hint/20 text-xs text-tg-link text-center active:opacity-60"
          >
            {historyOpen ? '▲' : '▼'} {t.balance_history}
          </button>
        </div>

        {/* History panel (slides open below the balance card) */}
        {historyOpen && (
          <div className="bg-tg-secondary rounded-xl p-4 mt-2">
            {historyLoading ? (
              <div className="text-center py-4 text-tg-hint text-xs">{t.loading}</div>
            ) : !history || ((!history.UZS || history.UZS.length === 0) && (!history.USD || history.USD.length === 0)) ? (
              <div className="text-center py-4 text-tg-hint text-xs">{t.balance_no_history}</div>
            ) : (
              <BalanceHistory
                history={history}
                currency={historyCurrency}
                onCurrencyChange={setHistoryCurrency}
              />
            )}
          </div>
        )}
      </div>
    );
  };

  // ── Balance History Component ──
  const BalanceHistory = ({ history, currency, onCurrencyChange }) => {
    const hasUzs = history.UZS && history.UZS.length > 0;
    const hasUsd = history.USD && history.USD.length > 0;
    const periods = history[currency] || [];

    if (periods.length === 0) return null;

    const fmt = currency === 'USD' ? formatUsd : formatUzs;
    const suffix = currency === 'USD' ? '' : " so'm";

    // Find max values for scaling the bars
    const maxDebit = Math.max(...periods.map(p => Math.abs(p.period_debit || 0)), 1);
    const maxCredit = Math.max(...periods.map(p => Math.abs(p.period_credit || 0)), 1);
    const maxVal = Math.max(maxDebit, maxCredit);

    return (
      <div>
        {/* Currency tabs */}
        {hasUzs && hasUsd && (
          <div className="flex gap-2 mb-3">
            {['USD', 'UZS'].map(cur => (
              <button
                key={cur}
                onClick={() => onCurrencyChange(cur)}
                className={`flex-1 text-xs py-1.5 rounded-lg font-medium transition-colors ${
                  currency === cur
                    ? 'bg-tg-button text-tg-button-text'
                    : 'bg-tg-bg text-tg-hint'
                }`}
              >
                {cur === 'USD' ? '💵 USD' : "💴 UZS"}
              </button>
            ))}
          </div>
        )}

        {/* Legend */}
        <div className="flex gap-4 mb-3 text-[10px]">
          <div className="flex items-center gap-1">
            <div className="w-2.5 h-2.5 rounded-sm bg-red-400/80" />
            <span className="text-tg-hint">{t.balance_shipments}</span>
          </div>
          <div className="flex items-center gap-1">
            <div className="w-2.5 h-2.5 rounded-sm bg-green-400/80" />
            <span className="text-tg-hint">{t.balance_payments}</span>
          </div>
        </div>

        {/* Bar chart */}
        <div className="space-y-1.5">
          {periods.map((p, idx) => {
            const debitPct = (Math.abs(p.period_debit || 0) / maxVal) * 100;
            const creditPct = (Math.abs(p.period_credit || 0) / maxVal) * 100;
            const bal = p.balance || 0;
            const isDebt = bal > 0;

            return (
              <div key={idx} className="flex items-center gap-2">
                {/* Month label */}
                <div className="text-[10px] text-tg-hint w-7 text-right flex-shrink-0">
                  {formatShortMonth(p.period_start)}
                </div>

                {/* Bars */}
                <div className="flex-1 min-w-0">
                  {/* Debit (shipments) bar */}
                  <div className="h-2.5 bg-tg-bg rounded-sm overflow-hidden mb-0.5">
                    <div
                      className="h-full bg-red-400/80 rounded-sm transition-all duration-300"
                      style={{ width: `${Math.max(debitPct, 0.5)}%` }}
                    />
                  </div>
                  {/* Credit (payments) bar */}
                  <div className="h-2.5 bg-tg-bg rounded-sm overflow-hidden">
                    <div
                      className="h-full bg-green-400/80 rounded-sm transition-all duration-300"
                      style={{ width: `${Math.max(creditPct, 0.5)}%` }}
                    />
                  </div>
                </div>

                {/* Balance label */}
                <div className={`text-[10px] font-medium w-14 text-right flex-shrink-0 ${isDebt ? 'text-red-500' : 'text-green-500'}`}>
                  {currency === 'USD'
                    ? `${isDebt ? '' : '-'}$${Math.abs(Math.round(bal)).toLocaleString()}`
                    : `${isDebt ? '' : '-'}${formatUzs(bal)}`
                  }
                </div>
              </div>
            );
          })}
        </div>

        {/* Summary line */}
        <div className="mt-3 pt-2 border-t border-tg-hint/20 text-[10px] text-tg-hint text-center">
          {periods.length} {periods.length === 1 ? 'oy' : 'oy'} · {formatPeriod(periods[0]?.period_start)} — {formatPeriod(periods[periods.length - 1]?.period_start)}
        </div>
      </div>
    );
  };

  if (orders.length === 0 && !balance) {
    return (
      <div className="text-center py-16">
        <div className="text-5xl mb-4">🏛️</div>
        <div className="text-lg font-medium">{t.no_orders}</div>
        <div className="text-sm text-tg-hint mt-1">{t.no_orders_desc}</div>
      </div>
    );
  }

  return (
    <div>
      <BalanceCard />

      <div className="text-sm text-tg-hint mb-3">
        {t.order_history} ({orders.length})
      </div>

      <div className="space-y-2">
        {orders.map(order => {
          const isExpanded = expandedId === order.id;

          return (
            <div key={order.id} className="bg-tg-secondary rounded-xl overflow-hidden">
              {/* Order summary row */}
              <button
                onClick={() => toggleExpand(order.id)}
                className="w-full text-left p-3 active:bg-tg-hint/10 transition-colors"
              >
                <div className="flex items-center justify-between">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-semibold">
                        #{order.id}
                      </span>
                      <span className="text-xs px-2 py-0.5 rounded-full bg-tg-bg text-tg-hint">
                        {STATUS_ICONS[order.status]} {STATUS_LABELS[order.status] || order.status}
                      </span>
                    </div>
                    <div className="text-xs text-tg-hint mt-1">
                      {formatDate(order.created_at)} · {order.item_count} {t.items_count}
                    </div>
                  </div>
                  <div className="text-right ml-2">
                    {order.total_usd > 0 && (
                      <div className="text-sm font-bold">{formatCartPrice(order.total_usd, 'USD')}</div>
                    )}
                    {order.total_uzs > 0 && (
                      <div className="text-sm font-bold">{formatCartPrice(order.total_uzs, 'UZS')}</div>
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

                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          handleReorderTap(order.id);
                        }}
                        disabled={reordering}
                        className="w-full mt-3 bg-tg-button text-tg-button-text rounded-xl py-2.5 font-semibold text-sm active:scale-95 transition-transform disabled:opacity-50"
                      >
                        {reordering ? t.loading : `🔄 ${t.reorder}`}
                      </button>
                    </>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>

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
