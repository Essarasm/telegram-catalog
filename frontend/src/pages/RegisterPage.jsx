import { useState } from 'react';
import { cloudSave } from '../utils/cloudStorage';

/**
 * Registration gate:
 * 1. Phone is required (one button tap via Telegram requestContact)
 * 2. Location is optional — user can share or tap "Keyinroq" to skip
 */
export default function RegisterPage({ onRegistered }) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [statusText, setStatusText] = useState(null);
  const [phoneCollected, setPhoneCollected] = useState(false);
  const [userData, setUserData] = useState(null);

  const saveToServer = (data) => {
    return fetch('/api/users/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    }).then(r => r.json());
  };

  const getLocation = () => {
    return new Promise((resolve, reject) => {
      const tg = window.Telegram?.WebApp;

      if (tg?.LocationManager) {
        tg.LocationManager.init(() => {
          if (tg.LocationManager.isInited && tg.LocationManager.isLocationAvailable) {
            tg.LocationManager.getLocation((loc) => {
              if (loc && loc.latitude) resolve({ lat: loc.latitude, lng: loc.longitude });
              else reject();
            });
          } else {
            browserGeo(resolve, reject);
          }
        });
      } else {
        browserGeo(resolve, reject);
      }
    });
  };

  const browserGeo = (resolve, reject) => {
    if (navigator.geolocation) {
      navigator.geolocation.getCurrentPosition(
        (pos) => resolve({ lat: pos.coords.latitude, lng: pos.coords.longitude }),
        () => reject(),
        { timeout: 15000, enableHighAccuracy: false }
      );
    } else {
      reject();
    }
  };

  // Step 1: collect phone
  const startRegistration = () => {
    setLoading(true);
    setError(null);
    setStatusText(null);

    const tg = window.Telegram?.WebApp;
    if (!tg) {
      setError("Telegram WebApp mavjud emas");
      setLoading(false);
      return;
    }

    tg.requestContact((ok, event) => {
      if (!ok) {
        setError("Telefon raqamni yuborish bekor qilindi");
        setLoading(false);
        return;
      }

      const contact = event?.responseUnsafe?.contact;
      if (!contact || !contact.phone_number) {
        setError("Telefon raqam olinmadi");
        setLoading(false);
        return;
      }

      const user = tg.initDataUnsafe?.user || {};
      const data = {
        telegram_id: user.id || 0,
        phone: contact.phone_number,
        first_name: contact.first_name || user.first_name || '',
        last_name: contact.last_name || user.last_name || '',
        username: user.username || '',
      };

      setUserData(data);
      setPhoneCollected(true);
      setLoading(false);
    });
  };

  // Step 2a: share location
  const shareLocation = async () => {
    setLoading(true);
    setError(null);
    setStatusText("Joylashuvingiz aniqlanmoqda...");

    try {
      const { lat, lng } = await getLocation();
      setStatusText("Ma'lumotlar saqlanmoqda...");
      const result = await saveToServer({ ...userData, latitude: lat, longitude: lng });
      const regData = {
        phone: userData.phone,
        firstName: userData.first_name || '',
        lastName: userData.last_name || '',
        username: userData.username || '',
      };
      // Await CloudStorage save before declaring registration complete
      await cloudSave('reg_data', regData);
      onRegistered(result?.approved ?? false, regData);
    } catch {
      setError("Joylashuvni aniqlab bo'lmadi. Qayta urinib ko'ring yoki keyinroq yuboring.");
      setLoading(false);
      setStatusText(null);
    }
  };

  // Step 2b: skip location
  const skipLocation = async () => {
    setLoading(true);
    setStatusText("Ma'lumotlar saqlanmoqda...");

    try {
      const result = await saveToServer(userData);
      const regData = {
        phone: userData.phone,
        firstName: userData.first_name || '',
        lastName: userData.last_name || '',
        username: userData.username || '',
      };
      // Await CloudStorage save before declaring registration complete
      await cloudSave('reg_data', regData);
      onRegistered(result?.approved ?? false, regData);
    } catch {
      setError("Xatolik yuz berdi. Qayta urinib ko'ring.");
      setLoading(false);
      setStatusText(null);
    }
  };

  // --- Phone collection screen ---
  if (!phoneCollected) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[60vh] px-6 text-center">
        <div className="text-5xl mb-4">👋</div>
        <h2 className="text-xl font-bold mb-2">Xush kelibsiz!</h2>
        <p className="text-tg-hint text-sm mb-6 max-w-[280px]">
          Buyurtma berish uchun telefon raqamingizni yuboring.
          Bu faqat bir marta amalga oshiriladi.
        </p>

        <button
          onClick={startRegistration}
          disabled={loading}
          className="bg-tg-button text-tg-button-text font-semibold rounded-xl px-8 py-3 text-base active:scale-[0.98] transition-transform disabled:opacity-50"
        >
          {loading ? "Yuklanmoqda..." : "📱 Telefon raqamni yuborish"}
        </button>

        {error && <p className="text-red-500 text-sm mt-4">{error}</p>}
      </div>
    );
  }

  // --- Location sharing screen (optional) ---
  return (
    <div className="flex flex-col items-center justify-center min-h-[60vh] px-6 text-center">
      <div className="text-5xl mb-4">📍</div>
      <h2 className="text-xl font-bold mb-2">Joylashuvingizni ulashing</h2>
      <p className="text-tg-hint text-sm mb-6 max-w-[280px]">
        Yetkazib berishni tezlashtirish uchun joylashuvingizni yuborishingiz mumkin.
        Bu ixtiyoriy — keyinroq ham yuborishingiz mumkin.
      </p>

      <button
        onClick={shareLocation}
        disabled={loading}
        className="w-full max-w-[280px] bg-tg-button text-tg-button-text font-semibold rounded-xl px-8 py-3 text-base active:scale-[0.98] transition-transform disabled:opacity-50 mb-3"
      >
        {loading ? (statusText || "Yuklanmoqda...") : "📍 Joylashuvni yuborish"}
      </button>

      <button
        onClick={skipLocation}
        disabled={loading}
        className="w-full max-w-[280px] text-tg-hint font-medium rounded-xl px-8 py-2.5 text-sm active:scale-[0.98] transition-transform disabled:opacity-50"
      >
        Keyinroq →
      </button>

      {error && <p className="text-red-500 text-sm mt-4">{error}</p>}
    </div>
  );
}
