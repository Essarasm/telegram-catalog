// Role → panel theme. The agent panel header (AgentHomePage) and the
// in-cabinet "agent stats card" both pull from this so a glance at the
// color tells the user what role they're acting under.
//
// admin   → solid sky #abdbec with dark slate text (light bg, no gradient)
// cashier → emerald → green gradient
// agent   → indigo → purple (the original color, unchanged)
// worker  → amber → orange
//
// Falls back to the agent theme if role is null/unknown so existing
// non-themed surfaces keep their look.

const THEMES = {
  admin: {
    bgClass: '',
    style: { background: '#abdbec', color: '#0f172a' },
    label: 'Admin paneli',
    badgeClass: 'bg-slate-900/10 text-slate-900',
  },
  cashier: {
    bgClass: 'bg-gradient-to-br from-emerald-500 to-green-600 text-white',
    style: undefined,
    label: 'Kassir paneli',
    badgeClass: 'bg-white/20 text-white',
  },
  agent: {
    bgClass: 'bg-gradient-to-br from-indigo-500 to-purple-600 text-white',
    style: undefined,
    label: 'Agent paneli',
    badgeClass: 'bg-white/20 text-white',
  },
  worker: {
    bgClass: 'bg-gradient-to-br from-amber-500 to-orange-600 text-white',
    style: undefined,
    label: 'Ishchi paneli',
    badgeClass: 'bg-white/20 text-white',
  },
};

export function roleTheme(role) {
  return THEMES[role] || THEMES.agent;
}
