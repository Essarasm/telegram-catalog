/**
 * Shown when user registered but phone is NOT in the client whitelist.
 * Directs them to contact the team for approval.
 */
export default function NotApprovedPage() {
  const openTelegram = () => {
    // Route to bot support forwarder — the admin group receives the message.
    const link = 'https://t.me/samrassvetbot?start=support';
    if (window.Telegram?.WebApp?.openTelegramLink) {
      window.Telegram.WebApp.openTelegramLink(link);
    } else {
      window.open(link, '_blank');
    }
  };

  return (
    <div className="flex flex-col items-center justify-center min-h-[70vh] px-6 text-center">
      <div className="text-5xl mb-4">🔒</div>
      <h2 className="text-xl font-bold mb-2">Ruxsat talab qilinadi</h2>
      <p className="text-tg-hint text-sm mb-6 max-w-[300px] leading-relaxed">
        Sizning raqamingiz mijozlar bazasida topilmadi.
        Ilovadan foydalanish uchun biz bilan bog'laning —
        sizni bazaga qo'shib, ruxsat beramiz.
      </p>

      <button
        onClick={openTelegram}
        className="bg-tg-button text-tg-button-text font-semibold rounded-xl px-8 py-3 text-base active:scale-[0.98] transition-transform mb-3"
      >
        Telegram orqali bog'lanish
      </button>

      <a
        href="tel:+998902277176"
        className="text-tg-link text-sm underline"
      >
        Yoki telefon orqali qo'ng'iroq qiling
      </a>

      <p className="text-tg-hint text-xs mt-8 max-w-[280px]">
        Agar sizning raqamingiz o'zgargan bo'lsa, iltimos, yangi raqamingizni
        xabar bering — biz bazani yangilaymiz.
      </p>
    </div>
  );
}
