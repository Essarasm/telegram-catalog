import { useState, useRef } from 'react';

/**
 * Registration gate — 2 required steps:
 * 1. Share phone number (required)
 * 2. Share location (required)
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

  const onLocationDenied = () => {
    setError("Joylashuvni yuborish majburiy. Iltimos, qayta urinib ko'ring.");
    setLoading(false);
  };

  const saveLocationAndFinish = (lat, lng) => {
    saveToServer({
      ...contactData.current,
      latitude: lat,
      longitude: lng,
    })
      .then(() => onRegistered())
      .catch(() => {
        setError("Serverga saqlashda xatolik");
        setLoading(false);
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
          (pos) => saveLocationAndFinish(pos.coords.latitude, pos.coords.longitude),
          () => onLocationDenied(),
          { timeout: 15000, enableHighAccuracy: false }
        );
      } else {
        onLocationDenied();
      }
      return;
    }

    // Try Telegram's LocationManager
    tg.LocationManager.init(() => {
      if (!tg.LocationManager.isInited || !tg.LocationManager.isLocationAvailable) {
        // Fall back to browser geolocation
        if (navigator.geolocation) {
          navigator.geolocation.getCurrentPosition(
            (pos) => saveLocationAndFinish(pos.coords.latitude, pos.coords.longitude),
            () => onLocationDenied(),
            { timeout: 15000, enableHighAccuracy: false }
          );
        } else {
          onLocationDenied();
        }
        return;
      }

      tg.LocationManager.getLocation((loc) => {
        if (loc && loc.latitude) {
          saveLocationAndFinish(loc.latitude, loc.longitude);
        } else {
          onLocationDenied();
        }
      });
    });
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

  // Step 2: Location (required)
  return (
    <div className="flex flex-col items-center justify-center min-h-[60vh] px-6 text-center">
      <div className="text-5xl mb-4">📍</div>
      <h2 className="text-xl font-bold mb-2">Joylashuvingiz</h2>
      <p className="text-tg-hint text-sm mb-6 max-w-[280px]">
        Buyurtma berish uchun joylashuvingizni yuboring.
        Bu yetkazib berishni tezlashtirish uchun kerak.
      </p>

      <button
        onClick={requestLocation}
        disabled={loading}
        className="bg-tg-button text-tg-button-text font-semibold rounded-xl px-8 py-3 text-base active:scale-[0.98] transition-transform disabled:opacity-50"
      >
        {loading ? "Yuklanmoqda..." : "📍 Joylashuvni yuborish"}
      </button>

      {error && <p className="text-red-500 text-sm mt-4">{error}</p>}
    </div>
  );
}
