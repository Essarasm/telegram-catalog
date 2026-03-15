import { useState } from 'react';

/**
 * Registration gate — asks user to share phone number via Telegram.
 * Uses Telegram WebApp's requestContact() method.
 */
export default function RegisterPage({ onRegistered }) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const requestPhone = async () => {
    setLoading(true);
    setError(null);

    try {
      const tg = window.Telegram?.WebApp;
      if (!tg) {
        setError("Telegram WebApp mavjud emas");
        setLoading(false);
        return;
      }

      // Use Telegram's built-in contact request
      tg.requestContact((ok, event) => {
        if (!ok) {
          setError("Telefon raqamni yuborish bekor qilindi");
          setLoading(false);
          return;
        }

        // event.responseUnsafe.contact contains the contact info
        const contact = event?.responseUnsafe?.contact;
        if (!contact || !contact.phone_number) {
          setError("Telefon raqam olinmadi");
          setLoading(false);
          return;
        }

        const user = tg.initDataUnsafe?.user || {};

        // Save to server
        fetch('/api/users/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            telegram_id: user.id || 0,
            phone: contact.phone_number,
            first_name: contact.first_name || user.first_name || '',
            last_name: contact.last_name || user.last_name || '',
            username: user.username || '',
          }),
        })
          .then(r => r.json())
          .then(() => {
            onRegistered({
              phone: contact.phone_number,
              first_name: contact.first_name || user.first_name || '',
            });
          })
          .catch(() => {
            setError("Serverga saqlashda xatolik");
            setLoading(false);
          });
      });
    } catch (err) {
      setError("Xatolik: " + (err.message || String(err)));
      setLoading(false);
    }
  };

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

      {error && (
        <p className="text-red-500 text-sm mt-4">{error}</p>
      )}
    </div>
  );
}
