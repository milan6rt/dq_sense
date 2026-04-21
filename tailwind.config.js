/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,jsx,ts,tsx}",
    "./App.jsx",
    "./main.jsx",
  ],
  theme: {
    extend: {
      colors: {
        brand: {
          bg:     '#f0ebe4',
          card:   '#ffffff',
          dark:   '#1c3d34',
          dark2:  '#254d42',
          orange: '#e8622b',
          text:   '#1a2e28',
          muted:  '#8d9e98',
          border: '#e0dbd5',
          hover:  '#f5f0ea',
          slot:   '#e8e3dc',
        }
      },
      fontFamily: {
        sans: ['Inter', 'ui-sans-serif', 'system-ui', 'sans-serif'],
      },
    },
  },
  plugins: [],
}
