import { useState, useEffect } from 'react';
import t from '../i18n/uz.json';
import CashHandoverInline from './CashHandoverInline';
import P2PInline from './P2PInline';
import LegalTransferInline from './LegalTransferInline';

/**
 * One "💳 Pay" button on the client's cabinet that expands to a method
 * picker, then renders the chosen form. Methods available depend on role:
 *   - agents (acting-as a client): cash + p2p + bank
 *   - plain clients (own cabinet): p2p + bank only (cash is staff-only)
 *
 * Each method's existing form is reused with `defaultOpen` so we don't
 * stack two toggle buttons. Cancel inside any form returns to the picker.
 */
export default function PayChooser({ telegramId, client, methods }) {
  const [expanded, setExpanded] = useState(false);
  const [method, setMethod] = useState(null);

  useEffect(() => {
    setExpanded(false);
    setMethod(null);
  }, [client?.id]);

  if (!client?.id) return null;
  if (!methods || methods.length === 0) return null;

  const closeAll = () => {
    setMethod(null);
    setExpanded(false);
  };

  // Active form view — full screen of the panel until cancel
  if (method === 'cash') {
    return (
      <CashHandoverInline
        telegramId={telegramId}
        client={client}
        defaultOpen={true}
        onClose={closeAll}
      />
    );
  }
  if (method === 'p2p') {
    return (
      <P2PInline
        telegramId={telegramId}
        client={client}
        defaultOpen={true}
        onClose={closeAll}
      />
    );
  }
  if (method === 'bank') {
    return (
      <LegalTransferInline
        telegramId={telegramId}
        client={client}
        defaultOpen={true}
        onClose={closeAll}
      />
    );
  }

  // Method picker
  const buttons = methods.map((m) => {
    if (m === 'cash') {
      return {
        key: 'cash',
        emoji: '💵',
        label: t.pay_method_cash,
        bg: 'bg-emerald-500',
      };
    }
    if (m === 'p2p') {
      return {
        key: 'p2p',
        emoji: '💳',
        label: t.pay_method_p2p,
        bg: 'bg-amber-500',
      };
    }
    if (m === 'bank') {
      return {
        key: 'bank',
        emoji: '🏛',
        label: t.pay_method_bank,
        bg: 'bg-indigo-500',
      };
    }
    return null;
  }).filter(Boolean);

  return (
    <div className="space-y-2 mb-3">
      <button
        onClick={() => setExpanded((v) => !v)}
        className="w-full rounded-xl bg-tg-link text-white px-4 py-4 text-base font-bold shadow active:opacity-80 flex items-center justify-center gap-2"
      >
        <span className="text-xl">{expanded ? '✕' : '💳'}</span>
        <span>{t.pay_button_title}</span>
      </button>
      {!expanded && (
        <div className="text-xs text-tg-hint px-1">{t.pay_button_hint}</div>
      )}
      {expanded && (
        <div className="space-y-2">
          {buttons.map((b) => (
            <button
              key={b.key}
              onClick={() => setMethod(b.key)}
              className={`w-full rounded-xl ${b.bg} text-white px-4 py-3 text-base font-semibold shadow active:opacity-80 flex items-center justify-center gap-2`}
            >
              <span className="text-lg">{b.emoji}</span>
              <span>{b.label}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
