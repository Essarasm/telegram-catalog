import { useState, useEffect, useRef } from 'react';
import t from '../i18n/uz.json';
import {
  fetchAgentCommission,
  fetchAgentMyDeliveries,
  fetchAgentVehicle,
  fetchFxRateToday,
  fetchRecentAgentClients,
  registerNewShop,
  searchAgentClients,
  setAgentVehicle,
  switchAgentClient,
} from '../utils/api';
import { roleTheme } from '../utils/roleTheme';

function getShopLocation() {
  return new Promise((resolve, reject) => {
    const tg = window.Telegram?.WebApp;
    const browserGeo = () => {
      if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition(
          (pos) => resolve({ lat: pos.coords.latitude, lng: pos.coords.longitude }),
          () => reject(),
          { timeout: 15000, enableHighAccuracy: true }
        );
      } else {
        reject();
      }
    };
    if (tg?.LocationManager) {
      tg.LocationManager.init(() => {
        if (tg.LocationManager.isInited && tg.LocationManager.isLocationAvailable) {
          tg.LocationManager.getLocation((loc) => {
            if (loc && loc.latitude) resolve({ lat: loc.latitude, lng: loc.longitude });
            else browserGeo();
          });
        } else {
          browserGeo();
        }
      });
    } else {
      browserGeo();
    }
  });
}

function RegisterShopForm({ uid, onRegistered, onCancel }) {
  const [firstName, setFirstName] = useState('');
  const [lastName, setLastName] = useState('');
  const [venue, setVenue] = useState('');
  const [phone, setPhone] = useState('');
  const [coords, setCoords] = useState(null);
  const [locating, setLocating] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState(null);
  const [collision, setCollision] = useState(null);

  const captureLocation = async () => {
    setLocating(true);
    setError(null);
    try {
      const c = await getShopLocation();
      setCoords(c);
    } catch {
      setError(t.register_shop_location_failed);
    } finally {
      setLocating(false);
    }
  };

  const submit = async () => {
    if (firstName.trim().length < 2) {
      setError(t.register_shop_validation_first_name);
      return;
    }
    if (lastName.trim().length < 2) {
      setError(t.register_shop_validation_last_name);
      return;
    }
    if (venue.trim().length < 2) {
      setError(t.register_shop_validation_venue);
      return;
    }
    if ((phone.match(/\d/g) || []).length < 9) {
      setError(t.register_shop_validation_phone);
      return;
    }
    if (!coords) {
      setError(t.register_shop_validation_location);
      return;
    }
    setSubmitting(true);
    setError(null);
    const r = await registerNewShop({
      telegram_id: uid,
      first_name: firstName.trim(),
      last_name: lastName.trim(),
      venue: venue.trim(),
      phone,
      lat: coords.lat,
      lng: coords.lng,
    });
    setSubmitting(false);
    if (!r.ok) {
      setError(r.error || 'Xatolik');
      return;
    }
    if (r.registration_status === 'linked_existing') {
      setCollision(r.client);
      return;
    }
    onRegistered(r.client);
  };

  if (collision) {
    return (
      <div className="rounded-xl border border-yellow-500/40 bg-yellow-500/10 p-3 space-y-2">
        <div className="text-sm font-semibold text-yellow-300">
          {t.register_shop_collision_title}
        </div>
        <div className="text-sm text-tg-text">
          {collision.name}{collision.client_id_1c ? ` · ${collision.client_id_1c}` : ''}
        </div>
        <div className="flex gap-2">
          <button
            onClick={() => onRegistered(collision)}
            className="flex-1 bg-tg-button text-tg-button-text font-semibold rounded-lg px-3 py-2 text-sm"
          >
            {t.register_shop_collision_action}
          </button>
          <button
            onClick={onCancel}
            className="px-3 py-2 text-sm text-tg-hint"
          >
            {t.register_shop_cancel}
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="rounded-xl bg-tg-secondary p-3 space-y-3 border border-tg-hint/20">
      <div className="text-sm font-semibold">{t.register_shop_title}</div>
      <div>
        <label className="block text-xs text-tg-hint mb-1">{t.register_shop_first_name_label}</label>
        <input
          type="text"
          value={firstName}
          onChange={(e) => setFirstName(e.target.value)}
          placeholder={t.register_shop_first_name_placeholder}
          className="w-full bg-tg-bg rounded-lg px-3 py-2 text-sm outline-none border border-tg-hint/20 focus:border-tg-link"
        />
      </div>
      <div>
        <label className="block text-xs text-tg-hint mb-1">{t.register_shop_last_name_label}</label>
        <input
          type="text"
          value={lastName}
          onChange={(e) => setLastName(e.target.value)}
          placeholder={t.register_shop_last_name_placeholder}
          className="w-full bg-tg-bg rounded-lg px-3 py-2 text-sm outline-none border border-tg-hint/20 focus:border-tg-link"
        />
      </div>
      <div>
        <label className="block text-xs text-tg-hint mb-1">{t.register_shop_venue_label}</label>
        <input
          type="text"
          value={venue}
          onChange={(e) => setVenue(e.target.value)}
          placeholder={t.register_shop_venue_placeholder}
          className="w-full bg-tg-bg rounded-lg px-3 py-2 text-sm outline-none border border-tg-hint/20 focus:border-tg-link"
        />
      </div>
      <div>
        <label className="block text-xs text-tg-hint mb-1">{t.register_shop_phone_label}</label>
        <input
          type="tel"
          value={phone}
          onChange={(e) => setPhone(e.target.value)}
          placeholder={t.register_shop_phone_placeholder}
          className="w-full bg-tg-bg rounded-lg px-3 py-2 text-sm outline-none border border-tg-hint/20 focus:border-tg-link"
        />
      </div>
      <div>
        <label className="block text-xs text-tg-hint mb-1">{t.register_shop_location_label}</label>
        {coords ? (
          <div className="text-sm text-green-400">
            {t.register_shop_location_ok}
            <span className="text-xs text-tg-hint ml-2">
              {coords.lat.toFixed(5)}, {coords.lng.toFixed(5)}
            </span>
          </div>
        ) : (
          <button
            onClick={captureLocation}
            disabled={locating}
            className="w-full bg-tg-bg border border-tg-hint/20 rounded-lg px-3 py-2 text-sm active:bg-tg-bg/70 disabled:opacity-50"
          >
            {locating ? t.register_shop_location_pending : t.register_shop_location_action}
          </button>
        )}
      </div>
      {error && <div className="text-xs text-red-400">{error}</div>}
      <div className="flex gap-2 pt-1">
        <button
          onClick={submit}
          disabled={submitting}
          className="flex-1 bg-tg-button text-tg-button-text font-semibold rounded-lg px-3 py-2 text-sm active:scale-[0.98] disabled:opacity-50"
        >
          {submitting ? t.register_shop_submitting : t.register_shop_submit}
        </button>
        <button
          onClick={onCancel}
          disabled={submitting}
          className="px-3 py-2 text-sm text-tg-hint disabled:opacity-50"
        >
          {t.register_shop_cancel}
        </button>
      </div>
    </div>
  );
}

function getTelegramUserId() {
  return window.Telegram?.WebApp?.initDataUnsafe?.user?.id || 0;
}

function formatRate(r) {
  if (!r && r !== 0) return '—';
  return Number(r).toLocaleString('ru-RU', {
    maximumFractionDigits: 0,
  });
}

function formatTimeHM(iso) {
  if (!iso) return '';
  try {
    const d = new Date(iso.replace(' ', 'T'));
    return d.toLocaleTimeString('uz-UZ', { hour: '2-digit', minute: '2-digit' });
  } catch {
    return iso;
  }
}

function formatDateDM(iso) {
  if (!iso) return '';
  try {
    const d = new Date(iso);
    return d.toLocaleDateString('uz-UZ', { day: 'numeric', month: 'short' });
  } catch {
    return iso;
  }
}

function FxRateBanner({ data }) {
  if (!data) {
    return (
      <div className="bg-tg-secondary rounded-xl p-3 animate-pulse">
        <div className="text-sm text-tg-hint">$ USD / UZS</div>
      </div>
    );
  }

  const events = data.today_events || [];

  // State A: no rate today
  if (events.length === 0) {
    return (
      <div className="rounded-xl p-3 border-2 border-yellow-500/50 bg-yellow-500/10">
        <div className="flex items-center gap-2">
          <span className="text-lg">⚠️</span>
          <div className="text-xs font-semibold text-yellow-400">
            {t.agent_fx_not_set}
          </div>
        </div>
        {data.yesterday && (
          <div className="mt-2 flex items-baseline gap-2">
            <span className="text-xs text-tg-hint">
              {t.agent_fx_yesterday} ({formatDateDM(data.yesterday.rate_date)})
            </span>
            <span className="text-xl font-bold text-tg-hint">
              {formatRate(data.yesterday.rate)}
            </span>
            <span className="text-xs text-tg-hint">so'm</span>
          </div>
        )}
      </div>
    );
  }

  // State B / C: 1 or 2 events
  const [current, previous] = events;
  return (
    <div className="rounded-xl p-3 border-2 border-green-500/40 bg-green-500/10">
      <div className="flex items-center justify-between">
        <div className="text-xs text-tg-hint">$ USD / UZS</div>
        <div className="text-xs text-green-400 font-semibold">
          {t.agent_fx_set_today}
          {events.length === 2 && ` • ${t.agent_fx_updated}`}
        </div>
      </div>
      <div className="mt-2 flex items-baseline gap-2">
        <span className="text-2xl font-bold">{formatRate(current.rate)}</span>
        <span className="text-sm text-tg-hint">so'm</span>
        <span className="text-xs text-tg-hint ml-auto">
          {formatTimeHM(current.set_at)}
          {current.set_by_name ? ` • ${current.set_by_name}` : ''}
        </span>
      </div>
      {previous && (
        <div className="mt-1 flex items-baseline gap-2 text-tg-hint/70">
          <span className="text-sm line-through">{formatRate(previous.rate)}</span>
          <span className="text-xs">so'm</span>
          <span className="text-xs ml-auto">{formatTimeHM(previous.set_at)}</span>
        </div>
      )}
    </div>
  );
}

function AgentPanelCard({ data, userRole }) {
  // Combined role-banner + commission card. Lives at the bottom of
  // AgentHomePage (per Ulugbek's 2026-05-11 UX call): the picker is the
  // top action surface; this card is ambient summary + identity. Reuses
  // the AgentStatsCard purple-gradient styling from CabinetPage.jsx:759
  // so the visual identity is consistent with the prior Cabinet design.
  //
  // Workers don't see money-flow surfaces (Agent charter Active risk #3).
  if (userRole === 'worker') return null;

  const theme = roleTheme(userRole);

  if (!data) {
    return (
      <div className={`rounded-xl p-4 shadow-lg animate-pulse ${theme.bgClass}`} style={theme.style}>
        <div className="text-[11px] uppercase tracking-wider opacity-90">{theme.label}</div>
      </div>
    );
  }
  if (!data.ok) return null;

  const fmtUzs = (v) => new Intl.NumberFormat('ru-RU').format(Math.round(v || 0));
  const fmtUsd = (v) => new Intl.NumberFormat('ru-RU', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(v || 0);
  const channelLabel = (ch) => t[`agent_commission_channel_${ch}`] || ch;
  const hasCollections = (data.collected_uzs || 0) > 0 || (data.collected_usd || 0) > 0;

  return (
    <div
      className={`rounded-xl p-4 shadow-lg space-y-2.5 ${theme.bgClass}`}
      style={theme.style}
    >
      {/* Top row — role label + Beta badge (replaces the standalone AGENT
          PANELI banner that lived at the top of the page) */}
      <div className="flex items-center justify-between">
        <div className="text-[11px] uppercase tracking-wider opacity-90">
          {theme.label}
        </div>
        <span className={`text-[10px] px-2 py-0.5 rounded-full ${theme.badgeClass}`}>
          {t.agent_dashboard_beta || 'Beta'}
        </span>
      </div>

      {/* Commission header */}
      <div className="flex items-baseline justify-between">
        <div className="text-sm font-semibold opacity-95">{t.agent_commission_title}</div>
        <div className="text-[11px] opacity-80 font-mono">{data.period}</div>
      </div>

      {data.client_count === 0 ? (
        <div className="text-xs opacity-80 py-1">{t.agent_commission_no_clients}</div>
      ) : !hasCollections ? (
        <div className="text-xs opacity-80 py-1">{t.agent_commission_no_payments}</div>
      ) : (
        <>
          <div className="flex items-baseline gap-2 flex-wrap">
            <span className="text-2xl font-bold">{fmtUzs(data.commission_uzs)}</span>
            <span className="text-xs opacity-80">so'm</span>
            {data.commission_usd > 0 && (
              <span className="text-xl font-bold ml-1">+ ${fmtUsd(data.commission_usd)}</span>
            )}
            <span className="text-[11px] opacity-95 ml-auto bg-white/20 px-1.5 py-0.5 rounded">
              {t.agent_commission_rate_badge}
            </span>
          </div>
          <div className="text-xs opacity-85">
            {t.agent_commission_collected_label}: {fmtUzs(data.collected_uzs)} so'm
            {data.collected_usd > 0 && ` · $${fmtUsd(data.collected_usd)}`}
          </div>
          {data.by_channel && data.by_channel.length > 0 && (
            <div className="flex flex-wrap gap-x-3 gap-y-1 text-[11px] opacity-80 pt-1">
              {data.by_channel.map((c) => (
                <span key={c.channel}>
                  {channelLabel(c.channel)}: {fmtUzs(c.uzs)}
                  {c.usd > 0 && ` · $${fmtUsd(c.usd)}`}
                </span>
              ))}
            </div>
          )}
        </>
      )}

      <div className="text-[10px] opacity-70 italic pt-1 border-t border-white/15">
        {t.agent_commission_phase1_note}
      </div>
    </div>
  );
}

function VehicleProfile({ uid, userRole, value, onChange }) {
  // Workers don't see agent profile surfaces.
  if (userRole === 'worker') return null;

  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(value || '');
  const [saving, setSaving] = useState(false);

  const startEdit = () => { setDraft(value || ''); setEditing(true); };
  const save = async () => {
    setSaving(true);
    const r = await setAgentVehicle(uid, draft);
    setSaving(false);
    if (r.ok) { onChange(r.vehicle || ''); setEditing(false); }
  };

  if (editing) {
    return (
      <div className="rounded-xl bg-tg-secondary p-3 border border-tg-hint/20 space-y-2">
        <div className="text-xs text-tg-hint">{t.agent_vehicle_label}</div>
        <input
          className="w-full bg-tg-bg rounded px-2 py-1.5 text-sm"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          maxLength={60}
          placeholder={t.agent_vehicle_placeholder}
          autoFocus
        />
        <div className="flex gap-2">
          <button
            onClick={save}
            disabled={saving}
            className="flex-1 rounded bg-tg-button text-tg-button-text text-sm py-1.5 font-semibold disabled:opacity-50"
          >
            {t.agent_vehicle_save}
          </button>
          <button
            onClick={() => setEditing(false)}
            className="rounded bg-tg-bg text-sm px-3 py-1.5"
          >
            {t.agent_vehicle_cancel}
          </button>
        </div>
      </div>
    );
  }

  return (
    <button
      onClick={startEdit}
      className="w-full rounded-xl bg-tg-secondary px-3 py-2 border border-tg-hint/20 flex items-center gap-2 active:bg-tg-secondary/70"
    >
      <span className="text-xs text-tg-hint">{t.agent_vehicle_label}:</span>
      <span className="text-sm font-medium flex-1 text-left truncate">
        {value || t.agent_vehicle_none}
      </span>
      <span className="text-xs text-tg-link">{t.agent_vehicle_edit}</span>
    </button>
  );
}

function MyDeliveriesSection({ data }) {
  if (!data || !data.ok) return null;
  const { active = [], history = [] } = data;
  const total = active.length + history.length;
  if (total === 0) return null;

  const fmtUzs = (v) => new Intl.NumberFormat('ru-RU').format(Math.round(v || 0));
  const statusLabel = (s) => t[`agent_delivery_status_${s}`] || s;
  // High-contrast pairs that read well in BOTH light + dark Telegram themes —
  // the prior `text-yellow-300 / blue-300 / green-300` invisibly blended with
  // their 20%-opacity fills in light mode (user screenshot 2026-05-11).
  const statusBadgeClass = (s) => {
    if (s === 'in_transit') return 'bg-blue-100 text-blue-800';
    if (s === 'assigned') return 'bg-amber-100 text-amber-800';
    if (s === 'delivered') return 'bg-green-100 text-green-800';
    if (s === 'cancelled') return 'bg-red-100 text-red-800';
    return 'bg-tg-hint/20 text-tg-text';
  };

  const Row = ({ d }) => (
    <div className="bg-tg-bg/50 rounded-lg p-2.5 space-y-1">
      <div className="flex items-baseline gap-2">
        <span className="text-sm font-medium flex-1 truncate">
          {d.client_1c || d.client_name}
        </span>
        <span className={`text-[10px] px-1.5 py-0.5 rounded ${statusBadgeClass(d.delivery_status)}`}>
          {statusLabel(d.delivery_status)}
        </span>
      </div>
      <div className="text-xs text-tg-hint flex gap-3 flex-wrap">
        <span>#{d.order_id}</span>
        <span>{d.item_count} mahsulot</span>
        <span>{fmtUzs(d.total_uzs)} so'm</span>
        {d.total_usd > 0 && <span>${d.total_usd}</span>}
      </div>
    </div>
  );

  return (
    <div className="rounded-xl bg-tg-secondary p-3 border border-tg-hint/20 space-y-2">
      <div className="text-sm font-semibold">{t.agent_my_deliveries_title}</div>
      {active.length === 0 ? (
        <div className="text-xs text-tg-hint">{t.agent_my_deliveries_empty}</div>
      ) : (
        <div className="space-y-2">
          {active.map((d) => <Row key={d.order_id} d={d} />)}
        </div>
      )}
      {history.length > 0 && (
        <>
          <div className="text-xs text-tg-hint pt-1">{t.agent_my_deliveries_history_title}</div>
          <div className="space-y-2">
            {history.map((d) => <Row key={d.order_id} d={d} />)}
          </div>
        </>
      )}
    </div>
  );
}

function ClientRow({ label, sub, onClick, isNew }) {
  return (
    <button
      onClick={onClick}
      className="w-full text-left bg-tg-bg rounded-lg p-3 flex items-center gap-3 active:bg-tg-bg/70 border border-tg-hint/10"
    >
      <span className="text-xl shrink-0">{isNew ? '🟡' : '👤'}</span>
      <div className="flex-1 min-w-0">
        <div className="text-sm font-medium truncate">{label}</div>
        {sub && <div className="text-xs text-tg-hint truncate">{sub}</div>}
      </div>
      <span className="text-tg-hint text-lg">›</span>
    </button>
  );
}

export default function AgentHomePage({ onClientSwitched, previousClient, onResumePrevious, userRole }) {
  const uid = getTelegramUserId();
  // Role label / theme now consumed by <AgentPanelCard> at the bottom of the
  // page; no longer needed at the AgentHomePage top level.
  const [fx, setFx] = useState(null);
  const [commission, setCommission] = useState(null);
  const [vehicle, setVehicle] = useState('');
  const [deliveries, setDeliveries] = useState(null);
  const [recent, setRecent] = useState([]);
  const [query, setQuery] = useState('');
  const [results, setResults] = useState(null);
  const [searching, setSearching] = useState(false);
  const [switching, setSwitching] = useState(false);
  const [registerOpen, setRegisterOpen] = useState(false);
  const debounceRef = useRef(null);

  const canRegister = userRole !== 'worker';

  // Initial load: FX + recent + commission + vehicle + deliveries
  useEffect(() => {
    fetchFxRateToday().then(setFx);
    if (uid) {
      fetchRecentAgentClients(uid).then(r => {
        if (r.ok) setRecent(r.recent || []);
      });
      fetchAgentCommission(uid).then(setCommission);
      fetchAgentVehicle(uid).then(r => {
        if (r.ok) setVehicle(r.vehicle || '');
      });
      fetchAgentMyDeliveries(uid).then(setDeliveries);
    }
  }, [uid]);

  // Debounced search
  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    const q = query.trim();
    if (!q) {
      setResults(null);
      return;
    }
    setSearching(true);
    debounceRef.current = setTimeout(async () => {
      const r = await searchAgentClients(uid, q);
      setSearching(false);
      if (r.ok) {
        setResults({
          whitelisted: r.whitelisted || [],
          new_1c: r.new_1c || [],
        });
      } else {
        setResults({ whitelisted: [], new_1c: [] });
      }
    }, 250);
    return () => clearTimeout(debounceRef.current);
  }, [query, uid]);

  const pickClient = async (payload) => {
    if (switching) return;
    setSwitching(true);
    const r = await switchAgentClient({ telegram_id: uid, ...payload });
    setSwitching(false);
    if (r.ok && r.client) {
      onClientSwitched(r.client);
    } else {
      alert(r.error || 'Xatolik');
    }
  };

  const previousLabel = previousClient
    ? (previousClient.client_id_1c || previousClient.name || `#${previousClient.id}`)
    : null;

  return (
    <div className="space-y-4">
      {/* Role banner removed 2026-05-11 — its content (label + commission
          summary) now lives in <AgentPanelCard /> at the bottom of the page,
          matching the prior CabinetPage AgentStatsCard design. */}
      {previousClient && (
        <button
          onClick={onResumePrevious}
          className="w-full rounded-xl bg-blue-500/10 border border-blue-500/40 px-3 py-2 flex items-center gap-2 active:bg-blue-500/20"
        >
          <span className="text-tg-link text-lg shrink-0">←</span>
          <div className="flex-1 min-w-0 text-left">
            <div className="text-sm font-medium truncate leading-tight">
              {previousLabel} {t.agent_back_to_catalog_suffix}
            </div>
          </div>
        </button>
      )}
      <FxRateBanner data={fx} />

      {/* Client picker — search input + results/recent + register CTA grouped
          as one visually distinct section (per user request 2026-05-11). Light
          gray tint (bg-tg-hint/10) so the grouping reads without competing
          with the FX banner / deliveries cards above. Search input is the
          primary action; recent-clients list is context; register CTA at
          the bottom is the escape valve when no existing client matches. */}
      <section className="rounded-2xl bg-tg-hint/15 border border-tg-hint/20 p-3 space-y-3">
        {/* Search bar — larger touch target, prominent icon, focus ring */}
        <div className="relative">
          <span className="absolute left-4 top-1/2 -translate-y-1/2 text-tg-hint pointer-events-none">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                 strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="11" cy="11" r="7" />
              <path d="M21 21l-4.3-4.3" />
            </svg>
          </span>
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder={t.agent_search_placeholder}
            className="w-full bg-tg-bg rounded-xl pl-12 pr-11 py-4 text-base outline-none border border-tg-hint/20 focus:border-tg-link focus:ring-2 focus:ring-tg-link/20 transition-colors placeholder:text-tg-hint/80"
          />
          {query && !searching && (
            <button
              type="button"
              onClick={() => setQuery('')}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-tg-hint hover:text-tg-text active:scale-95"
              aria-label="Tozalash"
            >
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                   strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="12" cy="12" r="10" />
                <path d="M15 9l-6 6M9 9l6 6" />
              </svg>
            </button>
          )}
          {searching && (
            <span className="absolute right-4 top-1/2 -translate-y-1/2 text-xs text-tg-hint">
              …
            </span>
          )}
        </div>

        {/* Search results */}
        {results && (
          <div className="space-y-2">
            {results.whitelisted.length === 0 && results.new_1c.length === 0 && (
              <div className="text-center text-sm text-tg-hint py-6">
                {t.agent_no_results}
              </div>
            )}
            {results.new_1c.length > 0 && (
              <div className="text-xs text-tg-hint px-1">
                {t.agent_new_1c_hint}
              </div>
            )}
            {results.new_1c.map((c) => (
              <ClientRow
                key={`new:${c.client_name_1c}`}
                label={c.client_name_1c}
                sub={`${c.balance_count} yozuv`}
                isNew
                onClick={() => pickClient({ client_name_1c: c.client_name_1c })}
              />
            ))}
            {results.whitelisted.map((c) => (
              <ClientRow
                key={`wl:${c.id}`}
                label={c.client_id_1c || c.name || `#${c.id}`}
                sub={c.phone || (c.name && c.client_id_1c !== c.name ? c.name : '')}
                onClick={() => pickClient({ client_id: c.id })}
              />
            ))}
          </div>
        )}

        {/* Recent clients (when not searching) */}
        {!results && (
          <div>
            <div className="text-[11px] uppercase tracking-wider text-tg-hint font-semibold px-1 mb-2">
              {t.agent_recent_clients}
            </div>
            {recent.length === 0 ? (
              <div className="text-center text-sm text-tg-hint py-6 rounded-xl">
                {t.agent_no_recent}
              </div>
            ) : (
              <div className="space-y-2">
                {recent.map((c) => (
                  <ClientRow
                    key={c.client_id}
                    label={c.client_id_1c || c.name || `#${c.client_id}`}
                    sub={c.phone || (c.name && c.client_id_1c !== c.name ? c.name : '')}
                    onClick={() => pickClient({ client_id: c.client_id })}
                  />
                ))}
              </div>
            )}
          </div>
        )}

        {/* Register-new-client CTA — escape valve at the bottom of the picker
            card. White pill with blue accent so it reads as a "toggle"
            against the section's tinted background. */}
        {canRegister && !registerOpen && (
          <button
            onClick={() => setRegisterOpen(true)}
            className="w-full rounded-xl bg-tg-bg border border-tg-button/40 px-3 py-2.5 text-sm font-semibold text-tg-button active:bg-tg-button/10"
          >
            {t.register_shop_button}
          </button>
        )}
        {canRegister && registerOpen && (
          <RegisterShopForm
            uid={uid}
            onRegistered={(client) => {
              setRegisterOpen(false);
              onClientSwitched(client);
            }}
            onCancel={() => setRegisterOpen(false)}
          />
        )}
      </section>

      {/* Mening yetkazmalarim — moved below the picker section per user
          UX call (2026-05-11). Reads as: pick / register a client first,
          then see your deliveries. */}
      <MyDeliveriesSection data={deliveries} />

      {/* Purple AgentPanelCard — moved here from the top per Ulugbek's
          UX call (2026-05-11). Reuses CabinetPage AgentStatsCard styling;
          merges role banner (AGENT PANELI + Beta) with commission summary
          so they're a single visual unit, not two stacked cards. */}
      <AgentPanelCard data={commission} userRole={userRole} />

      {/* Vehicle profile — small operational pill, stays at the very bottom */}
      <VehicleProfile uid={uid} userRole={userRole} value={vehicle} onChange={setVehicle} />
    </div>
  );
}
