import { useState, useEffect, useRef } from 'react';
import t from '../i18n/uz.json';
import { fetchP2PCards, submitP2P } from '../utils/api';

function formatUzsInput(raw) {
  const digits = (raw || '').replace(/[^\d]/g, '');
  if (!digits) return '';
  return digits.replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
}

function parseUzsInput(s) {
  return parseFloat((s || '').replace(/\s/g, '')) || 0;
}

export default function P2PInline({ telegramId, client, defaultOpen = false, onClose }) {
  const [open, setOpen] = useState(defaultOpen);
  const [cards, setCards] = useState([]);
  const [loadingCards, setLoadingCards] = useState(false);
  const [cardId, setCardId] = useState('');
  const [uzs, setUzs] = useState('');
  const [screenshot, setScreenshot] = useState(null);
  const [submitting, setSubmitting] = useState(false);
  const [feedback, setFeedback] = useState(null);
  const fileInputRef = useRef(null);

  useEffect(() => {
    setOpen(defaultOpen);
    setCardId('');
    setUzs('');
    setScreenshot(null);
    setFeedback(null);
  }, [client?.id, defaultOpen]);

  useEffect(() => {
    if (!open || cards.length > 0 || loadingCards) return;
    setLoadingCards(true);
    fetchP2PCards(telegramId).then((r) => {
      setLoadingCards(false);
      if (r.ok && Array.isArray(r.items)) setCards(r.items);
    });
  }, [open, telegramId, cards.length, loadingCards]);

  if (!client?.id) return null;

  const closePanel = () => {
    setOpen(false);
    setCardId('');
    setUzs('');
    setScreenshot(null);
    setFeedback(null);
    if (fileInputRef.current) fileInputRef.current.value = '';
    if (onClose) onClose();
  };

  const onFilePicked = (e) => {
    const f = e.target.files?.[0];
    if (!f) {
      setScreenshot(null);
      return;
    }
    if (!/^image\/|^application\/pdf$/.test(f.type)) {
      setFeedback({ type: 'err', text: t.p2p_screenshot_bad_type });
      return;
    }
    if (f.size > 10 * 1024 * 1024) {
      setFeedback({ type: 'err', text: t.p2p_screenshot_too_big });
      return;
    }
    setFeedback(null);
    setScreenshot(f);
  };

  const onSubmit = async () => {
    const amountUzs = parseUzsInput(uzs);
    if (!cardId) {
      setFeedback({ type: 'err', text: t.p2p_card_required });
      return;
    }
    if (amountUzs <= 0) {
      setFeedback({ type: 'err', text: t.p2p_amount_required });
      return;
    }
    if (!screenshot) {
      setFeedback({ type: 'err', text: t.p2p_screenshot_required });
      return;
    }
    setSubmitting(true);
    setFeedback(null);
    const r = await submitP2P({
      telegramId,
      clientId: client.id,
      amountUzs,
      cardId: parseInt(cardId, 10),
      screenshot,
    });
    setSubmitting(false);
    if (r.ok) {
      setFeedback({ type: 'ok', text: `${t.p2p_sent} #${r.payment_id}` });
      setTimeout(closePanel, 2000);
      return;
    }
    setFeedback({ type: 'err', text: r.error || t.p2p_failed });
  };

  const selectedCard = cards.find((c) => String(c.id) === String(cardId));
  const clientName = client.client_id_1c || client.name || `#${client.id}`;

  return (
    <div className="space-y-2 mb-3">
      {!defaultOpen && (
        <>
          <button
            onClick={() => setOpen((v) => !v)}
            className="w-full rounded-xl bg-amber-500 text-white px-4 py-4 text-base font-bold shadow active:opacity-80 flex items-center justify-center gap-2"
          >
            <span className="text-xl">{open ? '✕' : '💳'}</span>
            <span>{t.p2p_title}</span>
          </button>
          {!open && <div className="text-xs text-tg-hint px-1">{t.p2p_hint}</div>}
        </>
      )}

      {open && (
        <div className="bg-tg-secondary rounded-xl p-3 space-y-3">
          <div className="text-sm font-medium truncate">👤 {clientName}</div>

          <label className="block">
            <span className="text-xs text-tg-hint">{t.p2p_card}</span>
            <select
              value={cardId}
              onChange={(e) => setCardId(e.target.value)}
              disabled={loadingCards}
              className="mt-1 w-full bg-tg-bg rounded-lg px-3 py-2 text-base outline-none border border-tg-hint/20 focus:border-tg-link"
            >
              <option value="">
                {loadingCards ? t.p2p_loading : t.p2p_card_placeholder}
              </option>
              {cards.map((c) => (
                <option key={c.id} value={c.id}>
                  {c.card_number_display} — {c.holder_first_name} {c.holder_last_name}
                </option>
              ))}
            </select>
          </label>

          {selectedCard && (
            <div className="text-xs bg-tg-bg rounded-lg p-2 space-y-1">
              <div>
                <span className="text-tg-hint">{t.p2p_card_label}:</span>{' '}
                <code className="font-mono">{selectedCard.card_number_display}</code>
              </div>
              <div>
                <span className="text-tg-hint">{t.p2p_holder_label}:</span>{' '}
                {selectedCard.holder_first_name} {selectedCard.holder_last_name}
              </div>
            </div>
          )}

          <label className="block">
            <span className="text-xs text-tg-hint">{t.p2p_amount}</span>
            <input
              type="text"
              inputMode="numeric"
              value={uzs}
              onChange={(e) => setUzs(formatUzsInput(e.target.value))}
              placeholder={t.p2p_amount_placeholder}
              className="mt-1 w-full bg-tg-bg rounded-lg px-3 py-2 text-base outline-none border border-tg-hint/20 focus:border-tg-link"
            />
          </label>

          <label className="block">
            <span className="text-xs text-tg-hint">{t.p2p_screenshot}</span>
            <input
              ref={fileInputRef}
              type="file"
              accept="image/*,application/pdf"
              onChange={onFilePicked}
              className="mt-1 w-full bg-tg-bg rounded-lg px-3 py-2 text-sm outline-none border border-tg-hint/20"
            />
            {screenshot && (
              <div className="mt-1 text-xs text-emerald-300">
                ✅ {screenshot.name} ({Math.round(screenshot.size / 1024)} KB)
              </div>
            )}
          </label>

          {feedback?.type === 'ok' && (
            <div className="text-xs text-emerald-300 bg-emerald-500/10 rounded-lg p-2">
              ✅ {feedback.text}
            </div>
          )}
          {feedback?.type === 'err' && (
            <div className="text-xs text-red-300 bg-red-500/10 rounded-lg p-2">
              ⚠️ {feedback.text}
            </div>
          )}

          <div className="flex gap-2">
            <button
              onClick={onSubmit}
              disabled={submitting}
              className="flex-1 rounded-lg bg-amber-500 text-white py-3 text-base font-semibold active:opacity-80 disabled:opacity-50"
            >
              {submitting ? '…' : t.p2p_submit}
            </button>
            <button
              onClick={closePanel}
              disabled={submitting}
              className="flex-1 rounded-lg bg-tg-bg text-tg-text py-3 text-base active:opacity-80"
            >
              {t.p2p_cancel}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
