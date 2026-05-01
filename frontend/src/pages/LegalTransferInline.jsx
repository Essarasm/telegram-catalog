import { useState, useEffect } from 'react';
import t from '../i18n/uz.json';
import { fetchPaymentCategories, submitLegalTransfer } from '../utils/api';

// UZS: integer only, space-grouped thousands → "50 000 000"
function formatUzsInput(raw) {
  const digits = (raw || '').replace(/[^\d]/g, '');
  if (!digits) return '';
  return digits.replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
}

function parseUzsInput(s) {
  return parseFloat((s || '').replace(/\s/g, '')) || 0;
}

// INN: digits only, capped at 9 chars
function formatInnInput(raw) {
  return (raw || '').replace(/[^\d]/g, '').slice(0, 9);
}

export default function LegalTransferInline({ telegramId, client }) {
  const [open, setOpen] = useState(false);
  const [categories, setCategories] = useState([]);
  const [loadingCats, setLoadingCats] = useState(false);
  const [categoryId, setCategoryId] = useState('');
  const [categoryFreetext, setCategoryFreetext] = useState('');
  const [uzs, setUzs] = useState('');
  const [entityName, setEntityName] = useState('');
  const [inn, setInn] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [feedback, setFeedback] = useState(null); // {type:'ok'|'err', text}

  // Reset when client switches
  useEffect(() => {
    setOpen(false);
    setCategoryId('');
    setCategoryFreetext('');
    setUzs('');
    setEntityName('');
    setInn('');
    setFeedback(null);
  }, [client?.id]);

  // Lazy-load categories on first open
  useEffect(() => {
    if (!open || categories.length > 0 || loadingCats) return;
    setLoadingCats(true);
    fetchPaymentCategories(telegramId).then((r) => {
      setLoadingCats(false);
      if (r.ok && Array.isArray(r.items)) {
        setCategories(r.items);
      }
    });
  }, [open, telegramId, categories.length, loadingCats]);

  if (!client?.id) return null;

  const selectedCat = categories.find((c) => String(c.id) === String(categoryId));
  const isFreetextCat = !!selectedCat?.is_freetext;

  const closePanel = () => {
    setOpen(false);
    setCategoryId('');
    setCategoryFreetext('');
    setUzs('');
    setEntityName('');
    setInn('');
    setFeedback(null);
  };

  const onSubmit = async () => {
    const amountUzs = parseUzsInput(uzs);
    if (amountUzs <= 0) {
      setFeedback({ type: 'err', text: t.legaltx_amount_required });
      return;
    }
    if (!categoryId) {
      setFeedback({ type: 'err', text: t.legaltx_category_required });
      return;
    }
    if (isFreetextCat && !categoryFreetext.trim()) {
      setFeedback({ type: 'err', text: t.legaltx_freetext_required });
      return;
    }
    if (!entityName.trim()) {
      setFeedback({ type: 'err', text: t.legaltx_entity_required });
      return;
    }
    if (inn.length !== 9) {
      setFeedback({ type: 'err', text: t.legaltx_inn_required });
      return;
    }
    setSubmitting(true);
    setFeedback(null);
    const r = await submitLegalTransfer({
      telegramId,
      clientId: client.id,
      amountUzs,
      categoryId: parseInt(categoryId, 10),
      categoryFreetext: isFreetextCat ? categoryFreetext.trim() : '',
      legalEntityName: entityName.trim(),
      legalEntityInn: inn,
    });
    setSubmitting(false);
    if (r.ok) {
      setFeedback({
        type: 'ok',
        text: `${t.legaltx_sent} #${r.transfer_id}`,
      });
      setTimeout(closePanel, 2000);
      return;
    }
    setFeedback({ type: 'err', text: r.error || t.legaltx_failed });
  };

  const clientName = client.client_id_1c || client.name || `#${client.id}`;

  return (
    <div className="space-y-2 mb-3">
      {/* Toggle button — indigo to distinguish from cash flow's emerald */}
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full rounded-xl bg-indigo-500 text-white px-4 py-4 text-base font-bold shadow active:opacity-80 flex items-center justify-center gap-2"
      >
        <span className="text-xl">{open ? '✕' : '🏛'}</span>
        <span>{t.legaltx_title}</span>
      </button>
      {!open && (
        <div className="text-xs text-tg-hint px-1">{t.legaltx_hint}</div>
      )}

      {open && (
        <div className="bg-tg-secondary rounded-xl p-3 space-y-3">
          <div className="text-sm font-medium truncate">👤 {clientName}</div>

          {/* Category dropdown */}
          <label className="block">
            <span className="text-xs text-tg-hint">{t.legaltx_category}</span>
            <select
              value={categoryId}
              onChange={(e) => {
                setCategoryId(e.target.value);
                setCategoryFreetext('');
              }}
              disabled={loadingCats}
              className="mt-1 w-full bg-tg-bg rounded-lg px-3 py-2 text-base outline-none border border-tg-hint/20 focus:border-tg-link"
            >
              <option value="">
                {loadingCats ? t.legaltx_loading : t.legaltx_category_placeholder}
              </option>
              {categories.map((c) => (
                <option key={c.id} value={c.id}>
                  {c.label_uz}
                </option>
              ))}
            </select>
          </label>

          {isFreetextCat && (
            <label className="block">
              <span className="text-xs text-tg-hint">{t.legaltx_freetext_label}</span>
              <input
                type="text"
                value={categoryFreetext}
                onChange={(e) => setCategoryFreetext(e.target.value)}
                placeholder={t.legaltx_freetext_placeholder}
                className="mt-1 w-full bg-tg-bg rounded-lg px-3 py-2 text-base outline-none border border-tg-hint/20 focus:border-tg-link"
              />
            </label>
          )}

          {/* Amount */}
          <label className="block">
            <span className="text-xs text-tg-hint">{t.legaltx_amount}</span>
            <input
              type="text"
              inputMode="numeric"
              value={uzs}
              onChange={(e) => setUzs(formatUzsInput(e.target.value))}
              placeholder={t.legaltx_amount_placeholder}
              className="mt-1 w-full bg-tg-bg rounded-lg px-3 py-2 text-base outline-none border border-tg-hint/20 focus:border-tg-link"
            />
          </label>

          {/* Legal entity name */}
          <label className="block">
            <span className="text-xs text-tg-hint">{t.legaltx_entity_name}</span>
            <input
              type="text"
              value={entityName}
              onChange={(e) => setEntityName(e.target.value)}
              placeholder={t.legaltx_entity_placeholder}
              className="mt-1 w-full bg-tg-bg rounded-lg px-3 py-2 text-base outline-none border border-tg-hint/20 focus:border-tg-link"
            />
          </label>

          {/* INN */}
          <label className="block">
            <span className="text-xs text-tg-hint">{t.legaltx_inn}</span>
            <input
              type="text"
              inputMode="numeric"
              value={inn}
              onChange={(e) => setInn(formatInnInput(e.target.value))}
              placeholder={t.legaltx_inn_placeholder}
              className="mt-1 w-full bg-tg-bg rounded-lg px-3 py-2 text-base outline-none border border-tg-hint/20 focus:border-tg-link"
            />
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
              className="flex-1 rounded-lg bg-indigo-500 text-white py-3 text-base font-semibold active:opacity-80 disabled:opacity-50"
            >
              {submitting ? '…' : t.legaltx_submit}
            </button>
            <button
              onClick={closePanel}
              disabled={submitting}
              className="flex-1 rounded-lg bg-tg-bg text-tg-text py-3 text-base active:opacity-80"
            >
              {t.legaltx_cancel}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
