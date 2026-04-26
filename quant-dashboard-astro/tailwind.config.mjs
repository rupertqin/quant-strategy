/** @type {import('tailwindcss').Config} */
export default {
  content: ['./src/**/*.{astro,html,js,jsx,md,mdx,svelte,ts,tsx,vue}'],
  theme: {
    extend: {
      colors: {
        'quant-bg': '#f8fafc',
        'quant-card': '#ffffff',
        'quant-border': '#e2e8f0',
        'quant-primary': '#0f172a',
        'quant-accent': '#3b82f6',
        'quant-success': '#10b981',
        'quant-warning': '#f59e0b',
        'quant-danger': '#ef4444',
        'quant-up': '#ff4757',
        'quant-down': '#2ed573',
      }
    },
  },
  plugins: [],
};
