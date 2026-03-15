import { useState, useRef } from 'react';

/**
 * Registration gate — 2 steps:
 * 1. Share phone number (required)
 * 2. Share location (optional but encouraged)
 */
export default function RegisterPage({ onRegistered }) {
  const [step, setStep] = useState('phone'); // 'phone' | 'location'
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const contactData = useRef(null);

  const saveToServer = (data) => {
    return fetch('/api/users/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    }).then(r => r.json());
  };

  const requestPhone = () => {
    setLoading(true);
    setError(null);

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
      contactData.current = {
        telegram_id: user.id || 0,
        phone: contact.phone_number,
        first_name: contact.first_name || user.first_name || '',
        last_name: contact.last_name || user.last_name || '',
        username: user.username || '',
      };

      // Save phone first, then ask for location
      saveToServer(contactData.current)
        .then(() => {
          setLoading(false);
          setStep('location');
        })
        .catch(() => {
          setError("Serverga saqlashda xatolik");
          setLoading(false);
        });
    });
  };

  const requestLocation = () => {
    setLoading(true);
    setError(null);

    const tg = window.Telegram?.WebApp;
    if (!tg?.LocationManager) {
      // Fallback: use browser geolocation API
      if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition(
          (pos) => {
            saveToServer({
              ...contactData.current,
              latitude: pos.coords.latitude,
              longitude: pos.coords.longitude,
            })
              .then(() => onRegistered())
              .catch(() => onRegistered()); // still let them in
          },
          () => {
            // Denied or error — let them in anyway
            onRegistered();
          },
          { timeout: 15000, enableHighAccuracy: false }
        );
      } else {
        onRegistered();
      }
      return;
    }

    // Try Telegram's LocationManager
    tg.LocationManager.init(() => {
      if (!tg.LocationManager.isInited || !tg.LocationManager.isLocationAvailable) {
        // Fall back to browser geolocation
        if (navigator.geolocation) {
          navigator.geolocation.getCurrentPosition(
            (pos) => {
              saveToServer({
                ...contactData.current,
                latitude: pos.coords.latitude,
                longitude: pos.coords.longitude,
              })
                .then(() => onRegistered())
                .catch(() => onRegistered());
            },
            () => onRegistered(),
            { timeout: 15000, enableHighAccuracy: false }
          );
        } else {
          onRegistered();
        }
        return;
      }

      tg.LocationManager.getLocation((loc) => {
        if (loc && loc.latitude) {
          saveToServer({
            ...contactData.current,
            latitude: loc.latitude,
            longitude: loc.longitude,
          })
            .then(() => onRegistered())
            .catch(() => onRegistered());
        } else {
          onRegistered();
        }
      });
    });
  };

  const skipLocation = () => {
    onRegistered();
  };

  // Step 1: Phone
  if (step === 'phone') {
    return (
      <div className="flex flex-col items-center justify-center min-h-[60vh] px-6 text-center">
        <div className="text-5xl mb-4">📱</div>
        <h2 className="text-xl font-bold mb-2">Xush kelibsiz!</h2>
        <p className="text-tg-hint text-sm mb-6 max-w-[280px]">
          Buyurtma berish uchun telefon raqamingizni yuboring.
          Bu bir marta amalga oshiriladi.
        </p>

        <button
          onClick={requestPhone}
          disabled={loading}
          className="bg-tg-button text-tg-button-text font-semibold rounded-xl px-8 py-3 text-base active:scale-[0.98] transition-transform disabled:opacity-50"
        >
          {loading ? "Yuklanmoqda..." : "📞 Telefon raqamni yuborish"}
        </button>

        {error && <p className="text-red-500 text-sm mt-4">{error}</p>}
      </div>
    );
  }

  // Step 2: Location
  return (
    <div className="flex flex-col items-center justify-center min-h-[60vh] px-6 text-center">
      <div className="text-5xl mb-4">📍</div>
      <h2 className="text-xl font-bold mb-2">Joylashuvingiz</h2>
      <p className="text-tg-hint text-sm mb-6 max-w-[280px]">
        Yetkazib berishni tezlashtirish uchun joylashuvingizni yuboring.
      </p>

      <button
        onClick={requestLocation}
        disabled={loading}
        className="bg-tg-button text-tg-button-text font-semibold rounded-xl px-8 py-3 text-base active:scale-[0.98] transition-transform disabled:opacity-50 mb-3"
      >
        {loading ? "Yuklanmoqda..." : "📍 Joylashuvni yuborish"}
      </button>

      <button
        onClick={skipLocation}
        className="text-tg-hint text-sm underline"
      >
        Keyinroq
      </button>

      {error && <p className="text-red-500 text-sm mt-4">{error}</p>}
    </div>
  );
}
