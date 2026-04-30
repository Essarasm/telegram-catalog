// Minimal "acting-as a client" view for the worker role. Workers
// accompany drivers and confirm shop visits; they do not need to see any
// money flow. The only data shown is the client's name + phones + raw
// debt (in case the driver is unsure whether the collection makes sense)
// plus a "send location" button that opens the bot deep link to share
// GPS for this client.
import { useEffect, useState } from 'react';
import t from '../i18n/uz.json';

const FINANCE_API = '/api/finance';

function getTelegramUserId() {
  return window.Telegram?.WebApp?.initDataUnsafe?.user?.id || 0;
}

function fmtUzs(v) {
  if (!v) return '0';
  return Math.round(Math.abs(v)).toLocaleString('ru-RU').replace(/,/g, ' ');
}

function fmtUsd(v) {
  if (!v) return '$0.00';
  return '$' + Math.abs(v).toLocaleString('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

export default function WorkerClientView({ actingAsClient }) {
  const [debt, setDebt] = useState(null);
  const [loading, setLoading] = useState(true);
  const uid = getTelegramUserId();

  useEffect(() => {
    if (!uid) { setLoading(false); return; }
    fetch(`${FINANCE_API}/balance?telegram_id=${uid}`)
      .then(r => r.json())
      .then(d => {
        setDebt({
          uzs: d?.debt_uzs ?? 0,
          usd: d?.debt_usd ?? 0,
        });
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [uid, actingAsClient?.id]);

  const handleSendLocation = () => {
    window.Telegram?.WebApp?.openTelegramLink(
      'https://t.me/samrassvetbot?start=share_location'
    );
    setTimeout(() => window.Telegram?.WebApp?.close(), 300);
  };

  const phones = actingAsClient?.phones || (
    actingAsClient?.phone ? [actingAsClient.phone] : []
  );
  const clientLabel = actingAsClient?.client_id_1c
    || actingAsClient?.name
    || (actingAsClient ? `#${actingAsClient.id}` : '');

  const hasUzsDebt = debt && debt.uzs > 0;
  const hasUsdDebt = debt && debt.usd > 0;
  const hasAnyDebt = hasUzsDebt || hasUsdDebt;

  return (
    <div className="space-y-4">
      <div className="text-center">
        <div className="text-[10px] text-tg-hint uppercase tracking-wide">
          {t.akt_client_header || 'Mijoz'}
        </div>
        <div className="text-base font-semibold mt-0.5">{clientLabel}</div>
      </div>

      {phones.length > 0 && (
        <div>
          <div className="text-xs text-tg-hint mb-1.5 px-1">
            {t.worker_section_phones}
          </div>
          <div className="bg-tg-secondary rounded-xl p-3 flex flex-col gap-1.5">
            {phones.map((p) => (
              <a
                key={p}
                href={`tel:${p}`}
                className="text-sm text-tg-link"
              >
                📞 {p}
              </a>
            ))}
          </div>
        </div>
      )}

      <div>
        <div className="text-xs text-tg-hint mb-1.5 px-1">
          {t.worker_section_debt}
        </div>
        <div className="bg-tg-secondary rounded-xl p-4 text-center">
          {loading ? (
            <div className="text-sm text-tg-hint">…</div>
          ) : !hasAnyDebt ? (
            <div className="text-base font-semibold text-tg-hint">
              {t.worker_debt_none}
            </div>
          ) : (
            <div className="flex items-baseline justify-center gap-4">
              {hasUzsDebt && (
                <div className="text-xl font-bold text-red-500">
                  {fmtUzs(debt.uzs)} {t.balance_currency || "so'm"}
                </div>
              )}
              {hasUzsDebt && hasUsdDebt && (
                <div className="text-tg-hint/30 text-lg">│</div>
              )}
              {hasUsdDebt && (
                <div className="text-xl font-bold text-red-500">
                  {fmtUsd(debt.usd)}
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      <button
        onClick={handleSendLocation}
        className="w-full rounded-xl bg-gradient-to-br from-amber-500 to-orange-600 text-white font-semibold py-3 active:opacity-90"
      >
        {t.worker_send_location}
      </button>
    </div>
  );
}
