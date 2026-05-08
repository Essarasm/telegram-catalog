import { useState, useEffect, useRef } from 'react';
import t from '../i18n/uz.json';
import {
  fetchFxRateToday,
  fetchRecentAgentClients,
  registerNewShop,
  searchAgentClients,
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

function ClientRow({ label, sub, onClick, isNew }) {
  return (
    <button
      onClick={onClick}
      className="w-full text-left bg-tg-secondary rounded-lg p-3 flex items-center gap-3 active:bg-tg-secondary/70"
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
  const theme = roleTheme(userRole);
  const roleTitle =
    userRole === 'admin' ? t.admin_panel_home_title :
    userRole === 'cashier' ? t.cashier_panel_home_title :
    userRole === 'worker' ? t.worker_panel_home_title :
    t.agent_panel_home_title;
  const [fx, setFx] = useState(null);
  const [recent, setRecent] = useState([]);
  const [query, setQuery] = useState('');
  const [results, setResults] = useState(null);
  const [searching, setSearching] = useState(false);
  const [switching, setSwitching] = useState(false);
  const [registerOpen, setRegisterOpen] = useState(false);
  const debounceRef = useRef(null);

  const canRegister = userRole !== 'worker';

  // Initial load: FX + recent
  useEffect(() => {
    fetchFxRateToday().then(setFx);
    if (uid) {
      fetchRecentAgentClients(uid).then(r => {
        if (r.ok) setRecent(r.recent || []);
      });
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
      {/* Role banner — color tells the user at a glance which panel they're in */}
      <div
        className={`rounded-xl px-3 py-2 ${theme.bgClass}`}
        style={theme.style}
      >
        <div className="text-[11px] uppercase tracking-wider opacity-80">
          {roleTitle}
        </div>
      </div>
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

      {/* Register new shop — non-workers only */}
      {canRegister && !registerOpen && (
        <button
          onClick={() => setRegisterOpen(true)}
          className="w-full rounded-xl bg-tg-button/15 border border-tg-button/40 px-3 py-2 text-sm font-semibold text-tg-button active:bg-tg-button/25"
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

      {/* Search bar */}
      <div className="relative">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder={t.agent_search_placeholder}
          className="w-full bg-tg-secondary rounded-xl px-4 py-3 text-sm outline-none border border-tg-hint/20 focus:border-tg-link"
        />
        {searching && (
          <span className="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-tg-hint">
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
          <div className="text-xs text-tg-hint font-semibold px-1 mb-2">
            {t.agent_recent_clients}
          </div>
          {recent.length === 0 ? (
            <div className="text-center text-sm text-tg-hint py-6 bg-tg-secondary/50 rounded-xl">
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
    </div>
  );
}
