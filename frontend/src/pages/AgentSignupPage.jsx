import { useEffect, useState } from 'react';
import t from '../i18n/uz.json';
import { fetchAgentApplicationStatus, registerAgent } from '../utils/api';


function getTelegramUserId() {
  return window.Telegram?.WebApp?.initDataUnsafe?.user?.id || 0;
}


// Telegram's WebApp overlay (Close / chevron / ⋯) covers the top of the
// content; mirror App.jsx's safeAreaInset-aware spacer so the page title
// isn't buried under the overlay.
function getTopPad() {
  const tg = window.Telegram?.WebApp;
  if (!tg) return 56;
  const safe = tg.safeAreaInset?.top || 0;
  const content = tg.contentSafeAreaInset?.top || 0;
  // Always reserve at least 56px for the overlay buttons in non-fullscreen
  // modes where the insets report 0.
  return Math.max(safe + content, 56);
}


export default function AgentSignupPage({ onApproved }) {
  const uid = getTelegramUserId();
  const [stage, setStage] = useState('loading'); // loading | form | pending | rejected | approved
  const [appStatus, setAppStatus] = useState(null);
  const [firstName, setFirstName] = useState('');
  const [lastName, setLastName] = useState('');
  const [phone, setPhone] = useState('');
  const [vehicle, setVehicle] = useState('');
  const [capacity, setCapacity] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!uid) {
      setStage('form');
      return;
    }
    fetchAgentApplicationStatus(uid).then((r) => {
      if (r && r.exists) {
        setAppStatus(r);
        if (r.status === 'pending') setStage('pending');
        else if (r.status === 'approved') {
          setStage('approved');
          onApproved && onApproved();
        } else if (r.status === 'rejected') setStage('rejected');
        else setStage('form');
      } else {
        // Pre-fill from Telegram user
        const u = window.Telegram?.WebApp?.initDataUnsafe?.user;
        if (u) {
          setFirstName(u.first_name || '');
          setLastName(u.last_name || '');
        }
        setStage('form');
      }
    });
  }, [uid, onApproved]);

  const requestPhoneAndSubmit = () => {
    setError(null);
    if (firstName.trim().length < 2) {
      setError(t.agent_signup_validation_first); return;
    }
    if (lastName.trim().length < 2) {
      setError(t.agent_signup_validation_last); return;
    }
    const tg = window.Telegram?.WebApp;
    if (!tg || !tg.requestContact) {
      setError(t.agent_signup_no_telegram); return;
    }
    tg.requestContact(async (ok, event) => {
      if (!ok) {
        setError(t.agent_signup_phone_required);
        return;
      }
      const contact = event?.responseUnsafe?.contact;
      const tgPhone = contact?.phone_number;
      if (!tgPhone) {
        setError(t.agent_signup_phone_required);
        return;
      }
      setPhone(tgPhone);
      await doSubmit(tgPhone);
    });
  };

  const doSubmit = async (resolvedPhone) => {
    setSubmitting(true);
    setError(null);
    const capNum = parseFloat(capacity);
    const result = await registerAgent({
      telegram_id: uid,
      first_name: firstName.trim(),
      last_name: lastName.trim(),
      phone: resolvedPhone,
      vehicle: vehicle.trim() || null,
      vehicle_capacity_tons: (Number.isFinite(capNum) && capNum > 0) ? capNum : null,
    });
    setSubmitting(false);
    if (!result.ok) {
      const code = result.error || 'unknown';
      const map = {
        name_required: t.agent_signup_validation_first,
        phone_invalid: t.agent_signup_phone_required,
        already_agent: t.agent_signup_already_agent,
      };
      setError(map[code] || `Xatolik: ${code}`);
      return;
    }
    setAppStatus({ status: 'pending', application_id: result.application_id });
    setStage('pending');
  };

  const topPad = getTopPad();

  if (stage === 'loading') {
    return (
      <div className="text-center text-tg-hint" style={{ paddingTop: topPad + 16 }}>…</div>
    );
  }

  if (stage === 'approved') {
    return (
      <div className="px-6 pb-6 text-center space-y-4" style={{ paddingTop: topPad + 24 }}>
        <div className="text-3xl">✅</div>
        <div className="text-lg font-semibold">{t.agent_signup_approved_title}</div>
        <div className="text-sm text-tg-hint">{t.agent_signup_approved_body}</div>
      </div>
    );
  }

  if (stage === 'pending') {
    return (
      <div className="px-6 pb-6 space-y-4" style={{ paddingTop: topPad + 16 }}>
        <div className="rounded-xl bg-yellow-500/10 border border-yellow-500/40 p-4 space-y-2">
          <div className="text-2xl text-center">⏳</div>
          <div className="text-base font-semibold text-center">
            {t.agent_signup_pending_title}
          </div>
          <div className="text-sm text-tg-hint text-center">
            {t.agent_signup_pending_body}
          </div>
        </div>
        {appStatus?.requested_at && (
          <div className="text-xs text-tg-hint text-center font-mono">
            #{appStatus.application_id} · {appStatus.requested_at}
          </div>
        )}
      </div>
    );
  }

  if (stage === 'rejected') {
    return (
      <div className="px-6 pb-6 space-y-4" style={{ paddingTop: topPad + 16 }}>
        <div className="rounded-xl bg-red-500/10 border border-red-500/40 p-4 space-y-2">
          <div className="text-2xl text-center">❌</div>
          <div className="text-base font-semibold text-center">
            {t.agent_signup_rejected_title}
          </div>
          {appStatus?.reject_reason && (
            <div className="text-sm text-tg-hint text-center">
              {appStatus.reject_reason}
            </div>
          )}
          <button
            onClick={() => setStage('form')}
            className="w-full mt-2 rounded-lg bg-tg-button text-tg-button-text text-sm py-2 font-semibold"
          >
            {t.agent_signup_reapply}
          </button>
        </div>
      </div>
    );
  }

  // form
  return (
    <div className="px-4 pb-6 space-y-4" style={{ paddingTop: topPad + 8 }}>
      <div className="space-y-1">
        <h1 className="text-xl font-bold">{t.agent_signup_title}</h1>
        <div className="text-sm text-tg-hint">{t.agent_signup_subtitle}</div>
      </div>

      <div className="rounded-xl bg-tg-secondary p-3 space-y-3 border border-tg-hint/20">
        <div>
          <label className="block text-xs text-tg-hint mb-1">
            {t.agent_signup_first_label}
          </label>
          <input
            value={firstName}
            onChange={(e) => setFirstName(e.target.value)}
            className="w-full bg-tg-bg rounded px-2 py-2 text-sm"
            placeholder={t.agent_signup_first_placeholder}
          />
        </div>
        <div>
          <label className="block text-xs text-tg-hint mb-1">
            {t.agent_signup_last_label}
          </label>
          <input
            value={lastName}
            onChange={(e) => setLastName(e.target.value)}
            className="w-full bg-tg-bg rounded px-2 py-2 text-sm"
            placeholder={t.agent_signup_last_placeholder}
          />
        </div>
        <div>
          <label className="block text-xs text-tg-hint mb-1">
            {t.agent_signup_vehicle_label}
          </label>
          <input
            value={vehicle}
            onChange={(e) => setVehicle(e.target.value)}
            maxLength={60}
            className="w-full bg-tg-bg rounded px-2 py-2 text-sm"
            placeholder={t.agent_signup_vehicle_placeholder}
          />
          <div className="text-[11px] text-tg-hint mt-1">
            {t.agent_signup_vehicle_hint}
          </div>
        </div>
        <div>
          <label className="block text-xs text-tg-hint mb-1">
            {t.agent_signup_capacity_label}
          </label>
          <input
            value={capacity}
            onChange={(e) => setCapacity(e.target.value)}
            type="number"
            step="0.1"
            min="0"
            max="50"
            inputMode="decimal"
            className="w-full bg-tg-bg rounded px-2 py-2 text-sm"
            placeholder={t.agent_signup_capacity_placeholder}
          />
          <div className="text-[11px] text-tg-hint mt-1">
            {t.agent_signup_capacity_hint}
          </div>
        </div>
      </div>

      {error && (
        <div className="rounded-lg bg-red-500/10 border border-red-500/40 p-2 text-xs text-red-400">
          {error}
        </div>
      )}

      <button
        onClick={requestPhoneAndSubmit}
        disabled={submitting}
        className="w-full rounded-xl bg-tg-button text-tg-button-text font-semibold py-3 disabled:opacity-50"
      >
        {submitting ? t.agent_signup_submitting : t.agent_signup_submit}
      </button>

      <div className="text-[11px] text-tg-hint text-center">
        {t.agent_signup_phone_note}
      </div>
    </div>
  );
}
