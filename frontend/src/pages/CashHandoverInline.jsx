import { useState, useEffect } from 'react';
import t from '../i18n/uz.json';
import { submitAgentCashHandover } from '../utils/api';

// ── Input formatters (live as the user types) ───────────────────────

// UZS: integer only, space-grouped thousands → "1 000 000"
function formatUzsInput(raw) {
  const digits = (raw || '').replace(/[^\d]/g, '');
  if (!digits) return '';
  return digits.replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
}

// USD: comma-grouped thousands + period decimal, max 2 dp → "1,234.56"
function formatUsdInput(raw) {
  let cleaned = (raw || '').replace(/[^\d.]/g, '');
  const firstDot = cleaned.indexOf('.');
  if (firstDot === -1) {
    return cleaned.replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  }
  // Keep only the first period, drop trailing periods
  cleaned = cleaned.slice(0, firstDot + 1) + cleaned.slice(firstDot + 1).replace(/\./g, '');
  const intPart = cleaned.slice(0, firstDot);
  const decPart = cleaned.slice(firstDot + 1, firstDot + 3); // max 2 dp
  const intFmt = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  // Show the trailing dot while user is mid-typing (e.g. "100.")
  return decPart.length > 0 ? `${intFmt}.${decPart}` : `${intFmt}.`;
}

function parseUzsInput(s) {
  return parseFloat((s || '').replace(/\s/g, '')) || 0;
}

function parseUsdInput(s) {
  return parseFloat((s || '').replace(/,/g, '')) || 0;
}

// ── Component ───────────────────────────────────────────────────────

export default function CashHandoverInline({ telegramId, client }) {
  const [open, setOpen] = useState(false);
  const [uzs, setUzs] = useState('');
  const [usd, setUsd] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [feedback, setFeedback] = useState(null); // {type: 'ok'|'err'|'dup', text}

  // Reset form when the active client changes — prevents amount carrying
  // over to the next client by accident.
  useEffect(() => {
    setOpen(false);
    setUzs('');
    setUsd('');
    setFeedback(null);
  }, [client?.id]);

  if (!client?.id) return null;

  const closePanel = () => {
    setOpen(false);
    setUzs('');
    setUsd('');
    setFeedback(null);
  };

  const onSubmit = async (force = false) => {
    const uzsNum = parseUzsInput(uzs);
    const usdNum = parseUsdInput(usd);
    if (uzsNum <= 0 && usdNum <= 0) {
      setFeedback({ type: 'err', text: t.agent_handover_amount_required });
      return;
    }
    setSubmitting(true);
    setFeedback(null);
    const r = await submitAgentCashHandover({
      telegramId,
      clientId: client.id,
      uzsAmount: uzsNum,
      usdAmount: usdNum,
      force,
    });
    setSubmitting(false);
    if (r.ok) {
      setFeedback({ type: 'ok', text: t.agent_handover_sent });
      setTimeout(closePanel, 1500);
      return;
    }
    if (r.status === 409 && r.error === 'duplicate') {
      setFeedback({ type: 'dup', text: t.agent_handover_dup_warn });
      return;
    }
    setFeedback({ type: 'err', text: r.error || t.agent_handover_failed });
  };

  const clientName = client.client_id_1c || client.name || `#${client.id}`;

  return (
    <div className="space-y-2 mb-3">
      {/* Toggle button — high-contrast, large hit target */}
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full rounded-xl bg-emerald-500 text-white px-4 py-4 text-base font-bold shadow active:opacity-80 flex items-center justify-center gap-2"
      >
        <span className="text-xl">{open ? '✕' : '📥'}</span>
        <span>{t.agent_handover_title}</span>
      </button>
      {!open && (
        <div className="text-xs text-tg-hint px-1">
          {t.agent_handover_hint}
        </div>
      )}

      {/* Form (the agent is already acting-as the client — no picker) */}
      {open && (
        <div className="bg-tg-secondary rounded-xl p-3 space-y-3">
          <div className="text-sm font-medium truncate">👤 {clientName}</div>

          <div className="space-y-2">
            <label className="block">
              <span className="text-xs text-tg-hint">{t.agent_handover_uzs}</span>
              <input
                type="text"
                inputMode="numeric"
                value={uzs}
                onChange={(e) => setUzs(formatUzsInput(e.target.value))}
                placeholder={t.agent_handover_amount_placeholder}
                className="mt-1 w-full bg-tg-bg rounded-lg px-3 py-2 text-base outline-none border border-tg-hint/20 focus:border-tg-link"
              />
            </label>
            <label className="block">
              <span className="text-xs text-tg-hint">{t.agent_handover_usd}</span>
              <input
                type="text"
                inputMode="decimal"
                value={usd}
                onChange={(e) => setUsd(formatUsdInput(e.target.value))}
                placeholder={t.agent_handover_amount_placeholder}
                className="mt-1 w-full bg-tg-bg rounded-lg px-3 py-2 text-base outline-none border border-tg-hint/20 focus:border-tg-link"
              />
            </label>
          </div>

          {feedback?.type === 'ok' && (
            <div className="text-xs text-emerald-300 bg-emerald-500/10 rounded-lg p-2">✅ {feedback.text}</div>
          )}
          {feedback?.type === 'err' && (
            <div className="text-xs text-red-300 bg-red-500/10 rounded-lg p-2">⚠️ {feedback.text}</div>
          )}
          {feedback?.type === 'dup' && (
            <div className="space-y-2">
              <div className="text-xs text-yellow-300 bg-yellow-500/10 rounded-lg p-2">⚠️ {feedback.text}</div>
              <div className="flex gap-2">
                <button
                  onClick={() => onSubmit(true)}
                  disabled={submitting}
                  className="flex-1 rounded-lg bg-emerald-500 text-white py-3 text-sm font-semibold active:opacity-80 disabled:opacity-50"
                >
                  {t.agent_handover_dup_yes}
                </button>
                <button
                  onClick={() => setFeedback(null)}
                  className="flex-1 rounded-lg bg-tg-bg text-tg-text py-3 text-sm active:opacity-80"
                >
                  {t.agent_handover_dup_no}
                </button>
              </div>
            </div>
          )}

          {(!feedback || feedback.type === 'err') && (
            <div className="flex gap-2">
              <button
                onClick={() => onSubmit(false)}
                disabled={submitting}
                className="flex-1 rounded-lg bg-emerald-500 text-white py-3 text-base font-semibold active:opacity-80 disabled:opacity-50"
              >
                {submitting ? '…' : t.agent_handover_submit}
              </button>
              <button
                onClick={closePanel}
                disabled={submitting}
                className="flex-1 rounded-lg bg-tg-bg text-tg-text py-3 text-base active:opacity-80"
              >
                {t.agent_handover_cancel}
              </button>
            </div>
          )}
        </div>
      )}

    </div>
  );
}
