import { useState } from 'react';

/**
 * Registration gate — single step:
 * One button press collects phone, then immediately requests location.
 * User experiences it as one smooth action.
 */
export default function RegisterPage({ onRegistered }) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [statusText, setStatusText] = useState(null);

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
            // Fallback to browser
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

    // Step 1: request phone
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
      const userData = {
        telegram_id: user.id || 0,
        phone: contact.phone_number,
        first_name: contact.first_name || user.first_name || '',
        last_name: contact.last_name || user.last_name || '',
        username: user.username || '',
      };

      // Step 2: immediately request location
      setStatusText("Joylashuvingiz aniqlanmoqda...");

      getLocation()
        .then(({ lat, lng }) => {
          setStatusText("Ma'lumotlar saqlanmoqda...");
          return saveToServer({ ...userData, latitude: lat, longitude: lng });
        })
        .then(() => onRegistered())
        .catch(() => {
          setError("Joylashuvni yuborish majburiy. Iltimos, qayta urinib ko'ring.");
          setLoading(false);
          setStatusText(null);
        });
    });
  };

  return (
    <div className="flex flex-col items-center justify-center min-h-[60vh] px-6 text-center">
      <div className="text-5xl mb-4">👋</div>
      <h2 className="text-xl font-bold mb-2">Xush kelibsiz!</h2>
      <p className="text-tg-hint text-sm mb-6 max-w-[280px]">
        Buyurtma berish uchun telefon raqamingiz va joylashuvingizni yuboring.
        Bu faqat bir marta amalga oshiriladi.
      </p>

      <button
        onClick={startRegistration}
        disabled={loading}
        className="bg-tg-button text-tg-button-text font-semibold rounded-xl px-8 py-3 text-base active:scale-[0.98] transition-transform disabled:opacity-50"
      >
        {loading ? (statusText || "Yuklanmoqda...") : "Ro'yxatdan o'tish"}
      </button>

      {error && <p className="text-red-500 text-sm mt-4">{error}</p>}
    </div>
  );
}
